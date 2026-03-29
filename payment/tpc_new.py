import logging

import redis as redis_lib
from flask import Flask, g, abort, Response

from common.streams import (
    get_bus,
    ensure_groups,
    publish_response,
    run_gevent_consumer,
    ack,
)

logger = logging.getLogger(__name__)


class TpcService:
    """Handles Two-Phase Commit (TPC) protocol for the payment service.

    Registers Flask routes for synchronous prepare/commit/abort calls, and
    runs a Redis Streams consumer that handles the same commands asynchronously.
    """

    STREAM = "tpc.payment"
    RESPONSE_STREAM = "tpc.responses"
    GROUP = "payment-tpc"

    def __init__(self, redis_pool, scripts, bus_pool):
        self._redis_pool = redis_pool
        self._scripts = scripts
        self._bus_pool = bus_pool

    # ------------------------------------------------------------------
    # Flask route registration
    # ------------------------------------------------------------------

    def register_routes(self, app: Flask) -> None:
        app.post("/prepare/<txn_id>/<user_id>/<amount>")(self._prepare_transaction)
        app.post("/commit/<txn_id>")(self._commit_transaction)
        app.post("/abort/<txn_id>")(self._abort_transaction)

    def _prepare_transaction(self, txn_id: str, user_id: str, amount: int) -> Response:
        amount = int(amount)
        try:
            self._scripts.prepare_payment(
                keys=[f"prepared:payment:{txn_id}", f"user:{user_id}"],
                args=[amount, user_id],
                client=g.redis,
            )
        except redis_lib.exceptions.ResponseError as exc:
            self._raise_http_error(str(exc), user_id)
            raise
        return Response("Transaction prepared", status=200)

    def _commit_transaction(self, txn_id: str) -> Response:
        self._scripts.commit_payment(
            keys=[f"prepared:payment:{txn_id}"], client=g.redis
        )
        return Response("Transaction committed", status=200)

    def _abort_transaction(self, txn_id: str) -> Response:
        self._scripts.abort_payment(
            keys=[f"prepared:payment:{txn_id}"], client=g.redis
        )
        return Response("Transaction aborted", status=200)

    # ------------------------------------------------------------------
    # Stream consumer
    # ------------------------------------------------------------------

    def init_stream(self) -> None:
        bus = get_bus(self._bus_pool)
        ensure_groups(bus, [(self.STREAM, self.GROUP)])

    def start_consumer(self) -> None:
        """Blocking call — run in a dedicated daemon thread."""
        run_gevent_consumer(
            self._bus_pool,
            self.STREAM,
            self.GROUP,
            self._handle_message,
            "Payment TPC",
        )

    def _handle_message(self, msg_id: str, payload: dict) -> None:
        correlation_id = payload.get("correlation_id")
        command = payload.get("command")
        r = redis_lib.Redis(connection_pool=self._redis_pool)

        try:
            status_code, body = self._dispatch(command, payload, r)
        except Exception as exc:
            logger.error(
                "TPC command error %s/%s: %s", command, correlation_id, exc, exc_info=True
            )
            status_code, body = 400, {"error": "Internal TPC error"}

        bus = get_bus(self._bus_pool)
        publish_response(bus, self.RESPONSE_STREAM, correlation_id, status_code, body)
        ack(bus, self.STREAM, self.GROUP, msg_id)

    def _dispatch(self, command: str, payload: dict, r) -> tuple:
        txn_id = payload.get("txn_id", "")

        if command == "prepare":
            return self._dispatch_prepare(txn_id, payload, r)
        if command == "commit":
            self._scripts.commit_payment(keys=[f"prepared:payment:{txn_id}"], client=r)
            return 200, "Transaction committed"
        if command == "abort":
            self._scripts.abort_payment(keys=[f"prepared:payment:{txn_id}"], client=r)
            return 200, "Transaction aborted"

        return 400, {"error": f"Unknown TPC command: {command}"}

    def _dispatch_prepare(self, txn_id: str, payload: dict, r) -> tuple:
        user_id = payload.get("user_id")
        amount = int(payload.get("amount", 0))
        try:
            self._scripts.prepare_payment(
                keys=[f"prepared:payment:{txn_id}", f"user:{user_id}"],
                args=[amount, user_id],
                client=r,
            )
        except redis_lib.exceptions.ResponseError as exc:
            err = str(exc)
            if "NOT_FOUND" in err:
                return 400, {"error": f"User: {user_id} not found!"}
            if "INSUFFICIENT_CREDIT" in err:
                return 400, {"error": f"User: {user_id} has insufficient credit!"}
            raise
        return 200, "Transaction prepared"

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _raise_http_error(err: str, user_id: str) -> None:
        if "NOT_FOUND" in err:
            abort(400, f"User: {user_id} not found!")
        if "INSUFFICIENT_CREDIT" in err:
            abort(400, f"User: {user_id} has insufficient credit!")

    @staticmethod
    def recovery() -> None:
        """TPC recovery is coordinator-driven; no participant-side action needed."""
        print(
            "RECOVERY PAYMENT: coordinator-driven — no participant-side action needed",
            flush=True,
        )