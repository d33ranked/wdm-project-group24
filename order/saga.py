# order saga coordinator — drives stock→payment saga via redis streams
#
# the orchestrator owns all position-writing and recovery;
# this file only knows what the steps actually do

import json
import uuid
import time
import random
import logging
import threading
from collections import defaultdict

import redis as redis_lib

from common.idempotency import check_idempotency, save_idempotency
from common.orchestrator import Orchestrator, Workflow, StepFailed, suspend
from common.streams import (
    create_bus_pool,
    get_bus,
    ensure_groups,
    publish,
    read_pending_then_new,
    ack,
)

from db import get_order, get_order_for_update, mark_paid

logger = logging.getLogger(__name__)

GATEWAY_STREAM = "gateway.orders"
GATEWAY_RESPONSE_STREAM = "gateway.responses"
INTERNAL_STOCK_STREAM = "internal.stock"
INTERNAL_PAYMENT_STREAM = "internal.payment"
INTERNAL_RESPONSE_STREAM = "internal.responses"

GROUP_GATEWAY = "order-service"
GROUP_INTERNAL = "order-response-service"

_redis_pool = None
_bus_pool = None
_orchestrator: "Orchestrator | None" = None

# price-lookup correlation: corr_id → (event, response | None)
_pending_price_lookups: dict = {}
_pending_price_lookups_lock = threading.Lock()


def init(redis_pool, bus_pool):
    global _redis_pool, _bus_pool, _orchestrator
    _redis_pool = redis_pool
    _bus_pool = bus_pool
    _orchestrator = Orchestrator(redis_pool)
    bus = get_bus(bus_pool)
    ensure_groups(
        bus,
        [
            (GATEWAY_STREAM, GROUP_GATEWAY),
            (INTERNAL_RESPONSE_STREAM, GROUP_INTERNAL),
        ],
    )


def _get_r():
    return redis_lib.Redis(connection_pool=_redis_pool)


def _get_bus():
    return get_bus(_bus_pool)


def _publish_gateway_response(correlation_id: str, status_code: int, body):
    publish(
        _get_bus(),
        GATEWAY_RESPONSE_STREAM,
        {
            "correlation_id": correlation_id,
            "status_code": status_code,
            "body": body,
        },
    )


def _publish_internal_request(
    stream, correlation_id, method, path, body=None, headers=None
):
    publish(
        _get_bus(),
        stream,
        {
            "correlation_id": correlation_id,
            "method": method.upper(),
            "path": path,
            "headers": headers or {},
            "body": body or {},
        },
    )


# ── Workflow step functions ───────────────────────────────────────────────────
#
# Each function receives ctx (the serialisable context dict) and does exactly
# one piece of real work.  Async steps publish a message and call suspend();
# the consumer thread will call orchestrator.resume() or .fail() when the
# response arrives.


def _step_subtract_stock(ctx):
    # publish subtract_batch to stock service and wait for its response
    wf_id = ctx["wf_id"]
    batch_items = [
        {"item_id": iid, "amount": qty}
        for iid, qty in ctx["items_quantities"].items()
    ]
    corr_id = f"{wf_id}:stock:subtract_batch"
    _publish_internal_request(
        INTERNAL_STOCK_STREAM,
        corr_id,
        "POST",
        "/subtract_batch",
        body={"items": batch_items},
        headers={"Idempotency-Key": corr_id},
    )
    suspend()


def _step_charge_payment(ctx):
    # publish pay to payment service and wait for its response
    wf_id = ctx["wf_id"]
    corr_id = f"{wf_id}:payment:pay"
    _publish_internal_request(
        INTERNAL_PAYMENT_STREAM,
        corr_id,
        "POST",
        f"/pay/{ctx['user_id']}/{ctx['total_cost']}",
        headers={"Idempotency-Key": corr_id},
    )
    suspend()


def _step_mark_paid(ctx):
    # sync: mark order as paid and record idempotency key for the gateway
    r = _get_r()
    mark_paid(r, ctx["order_id"])
    save_idempotency(r, ctx["idempotency_key"], 200, "Checkout successful")


def _comp_add_stock(ctx):
    # compensation for step 0: add stock back and wait for confirmation
    wf_id = ctx["wf_id"]
    batch_items = [
        {"item_id": iid, "amount": qty}
        for iid, qty in ctx["items_quantities"].items()
    ]
    corr_id = f"{wf_id}:stock:rollback"
    _publish_internal_request(
        INTERNAL_STOCK_STREAM,
        corr_id,
        "POST",
        "/add_batch",
        body={"items": batch_items},
        headers={"Idempotency-Key": corr_id},
    )
    suspend()


def _on_complete(ctx):
    # called by the engine once all forward steps succeed
    _publish_gateway_response(ctx["original_correlation_id"], 200, "Checkout successful")


def _on_failed(ctx):
    # called by the engine once all compensation steps finish
    _publish_gateway_response(
        ctx["original_correlation_id"],
        400,
        {"error": ctx.get("_error", "Checkout failed")},
    )


CHECKOUT_WORKFLOW = Workflow(
    name="checkout_saga",
    steps=[_step_subtract_stock, _step_charge_payment, _step_mark_paid],
    # compensation[0] undoes step[0]: restore stock
    compensation=[_comp_add_stock],
    on_complete=_on_complete,
    on_failed=_on_failed,
)

# correlation_id suffixes that map to forward step responses
_RESUME_SUFFIXES = [":stock:subtract_batch", ":payment:pay"]
# correlation_id suffix that maps to a compensation step response
_COMP_SUFFIX = ":stock:rollback"


# ── Public API ────────────────────────────────────────────────────────────────


def handle_checkout_saga(r, order_id: str, headers: dict):
    idem_key = headers.get("Idempotency-Key") or headers.get("idempotency-key")
    correlation_id = headers.get("X-Correlation-Id") or headers.get("x-correlation-id")

    cached = check_idempotency(r, idem_key)
    if cached:
        return cached

    try:
        order = get_order_for_update(r, order_id)
    except ValueError as exc:
        return 400, {"error": str(exc)}

    if order["paid"]:
        return 200, {"message": f"Order {order_id} is already paid"}

    items_quantities: dict = defaultdict(int)
    for item_id, quantity in order["items"]:
        items_quantities[item_id] += quantity
    if not items_quantities:
        return 200, "Order has no items."

    _orchestrator.start(
        CHECKOUT_WORKFLOW,
        {
            "order_id": order_id,
            "user_id": order["user_id"],
            "total_cost": order["total_cost"],
            "items_quantities": dict(items_quantities),
            "original_correlation_id": correlation_id,
            "idempotency_key": idem_key,
        },
    )

    return None  # response delivered async via _publish_gateway_response


def handle_internal_response(payload):
    """Dispatch one message from the internal.responses stream."""
    correlation_id = payload.get("correlation_id", "")

    # price-lookup fast path (used by addItem, not the saga)
    with _pending_price_lookups_lock:
        if correlation_id in _pending_price_lookups:
            event, _ = _pending_price_lookups[correlation_id]
            _pending_price_lookups[correlation_id] = (event, payload)
            event.set()
            return

    # forward step responses
    for suffix in _RESUME_SUFFIXES:
        if correlation_id.endswith(suffix):
            wf_id = correlation_id[: -len(suffix)]
            if payload.get("status_code") == 200:
                _orchestrator.resume(CHECKOUT_WORKFLOW, wf_id)
            else:
                body = payload.get("body") or {}
                error = (
                    body.get("error")
                    if isinstance(body, dict)
                    else str(body)
                ) or "Step failed"
                _orchestrator.fail(CHECKOUT_WORKFLOW, wf_id, error)
            return

    # compensation step response
    if correlation_id.endswith(_COMP_SUFFIX):
        wf_id = correlation_id[: -len(_COMP_SUFFIX)]
        _orchestrator.resume_comp(CHECKOUT_WORKFLOW, wf_id)
        return

    logger.warning("No handler for correlation_id: %s", correlation_id)


def route_gateway_message(payload, r):
    # dispatch one message from the gateway.orders stream; returns (status_code, body) or None for checkout
    method = payload.get("method", "GET").upper()
    path = payload.get("path", "/")
    body = payload.get("body") or {}
    headers = payload.get("headers") or {}
    headers["X-Correlation-Id"] = payload.get("correlation_id", "")
    segments = [s for s in path.strip("/").split("/") if s]
    idem_key = headers.get("Idempotency-Key") or headers.get("idempotency-key")

    # POST /create/<user_id>
    if method == "POST" and len(segments) >= 2 and segments[0] == "create":
        order_id = str(uuid.uuid4())
        r.hset(
            f"order:{order_id}",
            mapping={
                "paid": "false",
                "items": json.dumps([]),
                "user_id": segments[1],
                "total_cost": "0",
            },
        )
        return 201, {"order_id": order_id}

    # POST /batch_init/<n>/<n_items>/<n_users>/<item_price>
    if method == "POST" and len(segments) >= 5 and segments[0] == "batch_init":
        n, n_items, n_users, item_price = (
            int(segments[1]),
            int(segments[2]),
            int(segments[3]),
            int(segments[4]),
        )
        pipe = r.pipeline(transaction=False)
        for i in range(n):
            uid = str(random.randint(0, n_users - 1))
            i1 = str(random.randint(0, n_items - 1))
            i2 = str(random.randint(0, n_items - 1))
            pipe.hset(
                f"order:{i}",
                mapping={
                    "paid": "false",
                    "items": json.dumps([[i1, 1], [i2, 1]]),
                    "user_id": uid,
                    "total_cost": str(2 * item_price),
                },
            )
        pipe.execute()
        return 200, {"msg": "Batch init for orders successful"}

    # GET /find/<order_id>
    if method == "GET" and len(segments) >= 2 and segments[0] == "find":
        try:
            order = get_order(r, segments[1])
        except ValueError as exc:
            return 400, {"error": str(exc)}
        return 200, {
            "order_id": segments[1],
            "paid": order["paid"],
            "items": order["items"],
            "user_id": order["user_id"],
            "total_cost": order["total_cost"],
        }

    # POST /addItem/<order_id>/<item_id>/<quantity>
    if method == "POST" and len(segments) >= 4 and segments[0] == "addItem":
        order_id, item_id, quantity = segments[1], segments[2], int(segments[3])
        if quantity <= 0:
            return 400, {"error": "Quantity must be positive!"}
        cached = check_idempotency(r, idem_key)
        if cached:
            return cached

        stock_response = _fetch_item_price(item_id)
        if stock_response is None:
            return 400, {"error": "Stock service timeout fetching item price"}
        if stock_response.get("status_code") != 200:
            return 400, {"error": f"Item {item_id} not found in stock service"}
        item_price = stock_response["body"]["price"]

        order_data = r.hgetall(f"order:{order_id}")
        if not order_data:
            return 400, {"error": f"Order {order_id} not found"}

        items_list = json.loads(order_data.get("items", "[]"))
        total_cost = int(order_data.get("total_cost", 0))

        merged = False
        for entry in items_list:
            if entry[0] == item_id:
                entry[1] += quantity
                merged = True
                break
        if not merged:
            items_list.append([item_id, quantity])
        total_cost += quantity * item_price

        r.hset(
            f"order:{order_id}",
            mapping={
                "items": json.dumps(items_list),
                "total_cost": str(total_cost),
            },
        )
        resp = f"Item: {item_id} added to: {order_id} price updated to: {total_cost}"
        save_idempotency(r, idem_key, 200, resp)
        return 200, resp

    # POST /checkout/<order_id>
    if method == "POST" and len(segments) >= 2 and segments[0] == "checkout":
        return handle_checkout_saga(r, segments[1], headers)

    return 404, {"error": f"No handler for {method} {path}"}


def _fetch_item_price(item_id: str):
    # block until stock service responds with item price; returns response dict or None on timeout
    corr_id = str(uuid.uuid4())
    event = threading.Event()

    with _pending_price_lookups_lock:
        _pending_price_lookups[corr_id] = (event, None)

    _publish_internal_request(INTERNAL_STOCK_STREAM, corr_id, "GET", f"/find/{item_id}")

    if not event.wait(timeout=10):
        with _pending_price_lookups_lock:
            _pending_price_lookups.pop(corr_id, None)
        return None

    with _pending_price_lookups_lock:
        _, response = _pending_price_lookups.pop(corr_id)
    return response


def start_gateway_consumer():
    # read gateway.orders, dispatch each message, publish response
    while True:
        try:
            bus = _get_bus()
            for msg_id, payload in read_pending_then_new(
                bus, GATEWAY_STREAM, GROUP_GATEWAY
            ):
                correlation_id = payload.get("correlation_id")
                r = _get_r()
                result = None
                try:
                    result = route_gateway_message(payload, r)
                except Exception as exc:
                    logger.error(
                        "Error processing %s: %s", correlation_id, exc, exc_info=True
                    )
                    result = (400, {"error": "Internal server error"})

                # checkout returns None — response sent async by saga handlers
                if result is not None:
                    _publish_gateway_response(correlation_id, result[0], result[1])

                ack(bus, GATEWAY_STREAM, GROUP_GATEWAY, msg_id)
        except Exception as exc:
            logger.error("Gateway consumer error, retrying in 1s: %s", exc)
            time.sleep(1)


def start_internal_consumer():
    # read internal.responses and advance saga state machine
    while True:
        try:
            bus = _get_bus()
            for msg_id, payload in read_pending_then_new(
                bus, INTERNAL_RESPONSE_STREAM, GROUP_INTERNAL
            ):
                try:
                    handle_internal_response(payload)
                except Exception as exc:
                    logger.error("Error in internal consumer: %s", exc, exc_info=True)
                ack(bus, INTERNAL_RESPONSE_STREAM, GROUP_INTERNAL, msg_id)
        except Exception as exc:
            logger.error("Internal consumer error, retrying in 1s: %s", exc)
            time.sleep(1)


def recovery_saga():
    # scan wf:* keys for incomplete checkout_saga workflows and resume them
    _orchestrator.recover(CHECKOUT_WORKFLOW)
