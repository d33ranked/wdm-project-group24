import uuid
import logging

import redis as redis_lib

from common import idempotency
from common.idempotency import check_idempotency, save_idempotency
from common.streams import (
    get_bus,
    ensure_groups,
    make_message_handler,
    run_gevent_consumer,
)

logger = logging.getLogger(__name__)


class SagaService:
    """Handles SAGA-pattern message routing for the payment service.

    Listens on two Redis Streams (gateway and internal) and routes each
    incoming request to the appropriate local handler.
    """

    GATEWAY_STREAM = "gateway.payment"
    GATEWAY_RESPONSE_STREAM = "gateway.responses"
    GATEWAY_GROUP = "payment-service"

    INTERNAL_STREAM = "internal.payment"
    INTERNAL_RESPONSE_STREAM = "internal.responses"
    INTERNAL_GROUP = "payment-service"

    def __init__(self, redis_pool, scripts, bus_pool):
        self._redis_pool = redis_pool
        self._scripts = scripts
        self._bus_pool = bus_pool

        bus = get_bus(bus_pool)
        ensure_groups(
            bus,
            [
                (self.GATEWAY_STREAM, self.GATEWAY_GROUP),
                (self.INTERNAL_STREAM, self.INTERNAL_GROUP),
            ],
        )

        self._handle_gateway_message = make_message_handler(
            self._get_bus,
            self._get_redis,
            self.GATEWAY_STREAM,
            self.GATEWAY_GROUP,
            self.GATEWAY_RESPONSE_STREAM,
            self._route,
        )
        self._handle_internal_message = make_message_handler(
            self._get_bus,
            self._get_redis,
            self.INTERNAL_STREAM,
            self.INTERNAL_GROUP,
            self.INTERNAL_RESPONSE_STREAM,
            self._route,
        )

    # ------------------------------------------------------------------
    # Consumer threads
    # ------------------------------------------------------------------

    def start_gateway_consumer(self) -> None:
        """Blocking call — run in a dedicated daemon thread."""
        run_gevent_consumer(
            self._bus_pool,
            self.GATEWAY_STREAM,
            self.GATEWAY_GROUP,
            self._handle_gateway_message,
            "Payment gateway",
        )

    def start_internal_consumer(self) -> None:
        """Blocking call — run in a dedicated daemon thread."""
        run_gevent_consumer(
            self._bus_pool,
            self.INTERNAL_STREAM,
            self.INTERNAL_GROUP,
            self._handle_internal_message,
            "Payment internal",
        )

    # ------------------------------------------------------------------
    # Message routing
    # ------------------------------------------------------------------

    def _route(self, payload: dict, r) -> tuple:
        method = payload.get("method", "GET").upper()
        path = payload.get("path", "/")
        headers = payload.get("headers") or {}
        segments = [s for s in path.strip("/").split("/") if s]
        idem_key = headers.get("Idempotency-Key") or headers.get("idempotency-key")

        logger.info(f"Handeling msg, with path: {path}, idem_key: {idem_key}")
        if method == "POST" and segments and segments[0] == "create_user":
            return self._create_user(r)

        if method == "POST" and len(segments) >= 3 and segments[0] == "batch_init":
            return self._batch_init(segments, r)

        if method == "GET" and len(segments) >= 2 and segments[0] == "find_user":
            return self._find_user(segments[1], r)

        if method == "POST" and len(segments) >= 3 and segments[0] == "add_funds":
            return self._add_funds(segments[1], int(segments[2]), idem_key, r)

        if method == "POST" and len(segments) >= 3 and segments[0] == "pay":
            return self._pay(segments[1], int(segments[2]), idem_key, r)

        logger.warning(f"Message with unknown route, method was: {method}, path was: {path}")
        return 404, {"error": f"No handler for {method} {path}"}

    # ------------------------------------------------------------------
    # Individual handlers
    # ------------------------------------------------------------------

    def _create_user(self, r) -> tuple:
        user_id = str(uuid.uuid4())
        r.hset(f"user:{user_id}", mapping={"credit": "0"})
        return 201, {"user_id": user_id}

    def _batch_init(self, segments: list, r) -> tuple:
        n, starting_money = int(segments[1]), int(segments[2])

        if starting_money < 0:
            return 400, {"error": f"Starting money should be positive, was: {starting_money}"}
        elif n < 0:
            return 400, {"error": f"Can't make less than 0 instances, n was: {n}"}

        pipe = r.pipeline(transaction=False)
        for i in range(n):
            pipe.hset(f"user:{i}", mapping={"credit": str(starting_money)})
        pipe.execute()
        return 200, {"msg": "Batch init for users successful"}

    def _find_user(self, user_id: str, r) -> tuple:
        data = r.hgetall(f"user:{user_id}")
        if not data:
            return 400, {"error": f"User {user_id} not found"}
        return 200, {"user_id": user_id, "credit": int(data["credit"])}

    def _add_funds(self, user_id: str, amount: int, idem_key: str | None, r) -> tuple:
        if amount <= 0:
            return 400, {"error": f"Amount must be positive! Was: {amount}"}

        cached = check_idempotency(r, idem_key)
        if cached:
            return cached

        if not r.hexists(f"user:{user_id}", "credit"):
            return 400, {"error": f"User {user_id} not found"}

        new_credit = r.hincrby(f"user:{user_id}", "credit", amount)
        resp = f"User: {user_id} credit updated to: {new_credit}"
        save_idempotency(r, idem_key, 200, resp)
        return 200, resp

    def _pay(self, user_id: str, amount: int, idem_key: str | None, r) -> tuple:
        if amount <= 0:
            return 400, {"error": f"Amount must be positive! Was: {amount}"}

        cached = check_idempotency(r, idem_key)
        if cached:
            return cached

        try:
            new_credit = self._scripts.deduct_credit(
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
        save_idempotency(r, idem_key, 200, resp)
        return 200, resp

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _get_redis(self):
        return redis_lib.Redis(connection_pool=self._redis_pool)

    def _get_bus(self):
        return get_bus(self._bus_pool)

    @staticmethod
    def recovery() -> None:
        """SAGA payment service is a participant only — no saga state to recover."""
        print(
            "SAGA RECOVERY PAYMENT: participant only — no saga state to recover",
            flush=True,
        )