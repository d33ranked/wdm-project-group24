"""Payment SAGA participant — Redis storage, Kafka messaging (Phase 1).

What changed from the PostgreSQL version
-----------------------------------------
- route_kafka_message receives r (redis.Redis) instead of conn (psycopg2 connection).
- All SQL replaced with hset / hgetall / hexists / hincrby / deduct_credit Lua script.
- Consumer loops are inlined here (start_gateway_consumer / start_internal_consumer)
  instead of delegating to the generic run_consumer_loop helper, because that
  helper uses psycopg2 connection pool semantics (getconn / putconn / rollback).

The Kafka producer/consumer and topic names are UNCHANGED.
Messaging migration (Kafka → Redis Streams) happens in Phase 2.
"""

import uuid
import time
import json
import logging

import redis as redis_lib

from common.idempotency import check_idempotency_kafka, save_idempotency_kafka
from common.kafka_helpers import build_producer, publish_response

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Topics (unchanged)
# ---------------------------------------------------------------------------

PAYMENT_GATEWAY_TOPIC   = "gateway.payment"
PAYMENT_INTERNAL_TOPIC  = "internal.payment"
GATEWAY_RESPONSE_TOPIC  = "gateway.responses"
INTERNAL_RESPONSE_TOPIC = "internal.responses"

# ---------------------------------------------------------------------------
# Module State
# ---------------------------------------------------------------------------

_redis_pool        = None
_scripts           = None  # LuaScripts instance shared from app.py startup
_gateway_producer  = None
_internal_producer = None


def init(redis_pool, scripts, gateway_kafka: str, internal_kafka: str):
    global _redis_pool, _scripts, _gateway_producer, _internal_producer
    _redis_pool        = redis_pool
    _scripts           = scripts
    _gateway_producer  = build_producer(gateway_kafka)
    _internal_producer = build_producer(internal_kafka)


def _get_r():
    """Get a Redis client from the pool — one per Kafka message."""
    return redis_lib.Redis(connection_pool=_redis_pool)


# ---------------------------------------------------------------------------
# Kafka Message Routing
# ---------------------------------------------------------------------------

def route_kafka_message(payload, r):
    method   = payload.get("method", "GET").upper()
    path     = payload.get("path", "/")
    headers  = payload.get("headers") or {}
    segments = [s for s in path.strip("/").split("/") if s]
    idem_key = headers.get("Idempotency-Key") or headers.get("idempotency-key")

    # POST /create_user
    if method == "POST" and segments and segments[0] == "create_user":
        user_id = str(uuid.uuid4())
        r.hset(f"user:{user_id}", mapping={"credit": "0"})
        return 201, {"user_id": user_id}

    # POST /batch_init/<n>/<starting_money>
    if method == "POST" and len(segments) >= 3 and segments[0] == "batch_init":
        n, starting_money = int(segments[1]), int(segments[2])
        pipe = r.pipeline(transaction=False)
        for i in range(n):
            pipe.hset(f"user:{i}", mapping={"credit": str(starting_money)})
        pipe.execute()
        return 200, {"msg": "Batch init for users successful"}

    # GET /find_user/<user_id>
    if method == "GET" and len(segments) >= 2 and segments[0] == "find_user":
        user_id = segments[1]
        data = r.hgetall(f"user:{user_id}")
        if not data:
            return 400, {"error": f"User {user_id} not found"}
        return 200, {"user_id": user_id, "credit": int(data["credit"])}

    # POST /add_funds/<user_id>/<amount>
    if method == "POST" and len(segments) >= 3 and segments[0] == "add_funds":
        user_id, amount = segments[1], int(segments[2])
        if amount <= 0:
            return 400, {"error": "Amount must be positive!"}
        cached = check_idempotency_kafka(r, idem_key)
        if cached:
            return cached
        if not r.hexists(f"user:{user_id}", "credit"):
            return 400, {"error": f"User {user_id} not found"}
        new_credit = r.hincrby(f"user:{user_id}", "credit", amount)
        resp = f"User: {user_id} credit updated to: {new_credit}"
        save_idempotency_kafka(r, idem_key, 200, resp)
        return 200, resp

    # POST /pay/<user_id>/<amount>
    if method == "POST" and len(segments) >= 3 and segments[0] == "pay":
        user_id, amount = segments[1], int(segments[2])
        if amount <= 0:
            return 400, {"error": "Amount must be positive!"}
        cached = check_idempotency_kafka(r, idem_key)
        if cached:
            return cached
        # deduct_credit lua: atomically checks credit >= amount then deducts
        try:
            new_credit = _scripts.deduct_credit(
                keys=[f"user:{user_id}"],
                args=[amount],
                client=r,
            )
        except redis_lib.exceptions.ResponseError as exc:
            err = str(exc)
            if "NOT_FOUND" in err:
                return 400, {"error": f"User {user_id} not found"}
            if "INSUFFICIENT_CREDIT" in err:
                return 400, {"error": f"User {user_id} has insufficient funds"}
            raise
        resp = f"User: {user_id} credit updated to: {new_credit}"
        save_idempotency_kafka(r, idem_key, 200, resp)
        return 200, resp

    return 404, {"error": f"No handler for {method} {path}"}


# ---------------------------------------------------------------------------
# Kafka Consumer Loops
# ---------------------------------------------------------------------------

def start_gateway_consumer(gateway_kafka: str):
    import kafka
    while True:
        try:
            consumer = kafka.KafkaConsumer(
                PAYMENT_GATEWAY_TOPIC,
                bootstrap_servers=gateway_kafka,
                group_id="payment-service-gateway",
                auto_offset_reset="earliest",
                enable_auto_commit=False,
                value_deserializer=lambda m: json.loads(m.decode("utf-8")),
            )
            logger.info("Payment gateway consumer started on '%s'", PAYMENT_GATEWAY_TOPIC)

            for message in consumer:
                payload        = message.value
                correlation_id = payload.get("correlation_id")
                if not correlation_id:
                    try: consumer.commit()
                    except Exception: pass
                    continue

                r = _get_r()
                try:
                    status_code, body = route_kafka_message(payload, r)
                except Exception as exc:
                    logger.error("Error processing %s: %s", correlation_id, exc, exc_info=True)
                    status_code, body = 500, {"error": "Internal server error"}

                publish_response(_gateway_producer, GATEWAY_RESPONSE_TOPIC,
                                 correlation_id, status_code, body)
                try: consumer.commit()
                except Exception: pass

        except Exception as exc:
            logger.error("Payment gateway consumer crashed, reconnecting in 3s: %s", exc)
            time.sleep(3)


def start_internal_consumer(internal_kafka: str):
    import kafka
    while True:
        try:
            consumer = kafka.KafkaConsumer(
                PAYMENT_INTERNAL_TOPIC,
                bootstrap_servers=internal_kafka,
                group_id="payment-service-internal",
                auto_offset_reset="earliest",
                enable_auto_commit=False,
                value_deserializer=lambda m: json.loads(m.decode("utf-8")),
            )
            logger.info("Payment internal consumer started on '%s'", PAYMENT_INTERNAL_TOPIC)

            for message in consumer:
                payload        = message.value
                correlation_id = payload.get("correlation_id")
                if not correlation_id:
                    try: consumer.commit()
                    except Exception: pass
                    continue

                r = _get_r()
                try:
                    status_code, body = route_kafka_message(payload, r)
                except Exception as exc:
                    logger.error("Error processing %s: %s", correlation_id, exc, exc_info=True)
                    status_code, body = 500, {"error": "Internal server error"}

                publish_response(_internal_producer, INTERNAL_RESPONSE_TOPIC,
                                 correlation_id, status_code, body)
                try: consumer.commit()
                except Exception: pass

        except Exception as exc:
            logger.error("Payment internal consumer crashed, reconnecting in 3s: %s", exc)
            time.sleep(3)


# ---------------------------------------------------------------------------
# Recovery
# ---------------------------------------------------------------------------

def recovery_saga(redis_pool):
    # payment is a SAGA participant, not an orchestrator — it holds no saga state
    # the order service (orchestrator) drives all recovery on restart
    print("SAGA RECOVERY PAYMENT: participant only — no saga state to recover", flush=True)
