"""Payment SAGA participant — Redis storage, Redis Streams messaging (Phase 2).

What changed from Phase 1
--------------------------
- Kafka producer/consumers replaced with Redis Streams (common.streams).
- init() now accepts bus_pool (shared redis-bus ConnectionPool) instead of
  kafka address strings; calls ensure_groups so the consumer groups exist
  before the loops start.
- start_gateway_consumer / start_internal_consumer use read_pending_then_new
  + ack for at-least-once delivery (pending messages are re-delivered on
  restart automatically).
- Response publishing uses publish() to gateway.responses / internal.responses
  instead of Kafka's publish_response().
- route_stream_message (formerly route_kafka_message) is UNCHANGED — it is
  pure Redis storage logic with no messaging dependency.
"""

import uuid
import time
import json
import logging

import redis as redis_lib

from common.idempotency import check_idempotency_kafka, save_idempotency_kafka
from common.streams import (
    get_bus, ensure_groups, publish, read_pending_then_new, ack,
)

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Stream / group names
# ---------------------------------------------------------------------------

GATEWAY_STREAM           = "gateway.payment"
GATEWAY_RESPONSE_STREAM  = "gateway.responses"
INTERNAL_STREAM          = "internal.payment"
INTERNAL_RESPONSE_STREAM = "internal.responses"

GROUP_GATEWAY  = "payment-service"
GROUP_INTERNAL = "payment-service"

# ---------------------------------------------------------------------------
# Module state
# ---------------------------------------------------------------------------

_redis_pool = None   # payment service's own Redis (data storage)
_bus_pool   = None   # shared redis-bus (messaging)
_scripts    = None   # LuaScripts instance shared from app.py


def init(redis_pool, scripts, bus_pool):
    global _redis_pool, _scripts, _bus_pool
    _redis_pool = redis_pool
    _scripts    = scripts
    _bus_pool   = bus_pool

    bus = get_bus(bus_pool)
    ensure_groups(bus, [
        (GATEWAY_STREAM,  GROUP_GATEWAY),
        (INTERNAL_STREAM, GROUP_INTERNAL),
    ])


def _get_r():
    """Redis client for payment data (storage Redis)."""
    return redis_lib.Redis(connection_pool=_redis_pool)


def _get_bus():
    """Redis client for the message bus."""
    return get_bus(_bus_pool)


# ---------------------------------------------------------------------------
# Stream Message Routing
# ---------------------------------------------------------------------------

def route_stream_message(payload, r):
    """Dispatch an inbound stream message to the appropriate handler.

    Identical logic to the old route_kafka_message — the name reflects
    that messages now arrive via Redis Streams rather than Kafka.
    Returns (status_code, body).
    """
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
# Redis Streams Consumer Loops
# ---------------------------------------------------------------------------

def _publish_response(response_stream: str, correlation_id: str,
                      status_code: int, body):
    """Publish a response back to the caller via the appropriate stream."""
    publish(_get_bus(), response_stream, {
        "correlation_id": correlation_id,
        "status_code":    status_code,
        "body":           body,
    })


def start_gateway_consumer():
    """Consume messages from gateway.payment and reply on gateway.responses.

    Called from app.py in a daemon thread when TRANSACTION_MODE=SAGA.

    Delivery guarantee: at-least-once.
    - read_pending_then_new re-delivers any messages that were processed
      but not yet ACKed before a crash.
    - ack() is called only after the response has been published, so a crash
      between processing and ack causes the message to be re-processed on
      restart (handlers are idempotent via the idempotency key mechanism).
    """
    logger.info("Payment gateway consumer started on stream '%s'", GATEWAY_STREAM)
    while True:
        try:
            bus = _get_bus()
            for msg_id, payload in read_pending_then_new(bus, GATEWAY_STREAM, GROUP_GATEWAY):
                correlation_id = payload.get("correlation_id")
                if not correlation_id:
                    ack(bus, GATEWAY_STREAM, GROUP_GATEWAY, msg_id)
                    continue

                r = _get_r()
                try:
                    status_code, body = route_stream_message(payload, r)
                except Exception as exc:
                    logger.error("Error processing %s: %s", correlation_id, exc, exc_info=True)
                    status_code, body = 400, {"error": "Internal server error"}

                _publish_response(GATEWAY_RESPONSE_STREAM, correlation_id, status_code, body)
                ack(bus, GATEWAY_STREAM, GROUP_GATEWAY, msg_id)

        except Exception as exc:
            logger.error("Payment gateway consumer error, retrying in 1s: %s", exc)
            time.sleep(1)


def start_internal_consumer():
    """Consume messages from internal.payment and reply on internal.responses.

    Called from app.py in a daemon thread when TRANSACTION_MODE=SAGA.
    Identical delivery contract to start_gateway_consumer above.
    """
    logger.info("Payment internal consumer started on stream '%s'", INTERNAL_STREAM)
    while True:
        try:
            bus = _get_bus()
            for msg_id, payload in read_pending_then_new(bus, INTERNAL_STREAM, GROUP_INTERNAL):
                correlation_id = payload.get("correlation_id")
                if not correlation_id:
                    ack(bus, INTERNAL_STREAM, GROUP_INTERNAL, msg_id)
                    continue

                r = _get_r()
                try:
                    status_code, body = route_stream_message(payload, r)
                except Exception as exc:
                    logger.error("Error processing %s: %s", correlation_id, exc, exc_info=True)
                    status_code, body = 400, {"error": "Internal server error"}

                _publish_response(INTERNAL_RESPONSE_STREAM, correlation_id, status_code, body)
                ack(bus, INTERNAL_STREAM, GROUP_INTERNAL, msg_id)

        except Exception as exc:
            logger.error("Payment internal consumer error, retrying in 1s: %s", exc)
            time.sleep(1)


# ---------------------------------------------------------------------------
# Recovery
# ---------------------------------------------------------------------------

def recovery_saga(redis_pool):
    # payment is a SAGA participant, not an orchestrator — it holds no saga state
    # the order service (orchestrator) drives all recovery on restart
    print("SAGA RECOVERY PAYMENT: participant only — no saga state to recover", flush=True)
