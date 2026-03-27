# API Gateway — HTTP-to-Stream bridge.  Only active in SAGA mode.
#
# Request-response pattern
# ------------------------
# Every HTTP request that arrives at the gateway needs a response, but the
# services process messages asynchronously.  We bridge them like this:
#
#   1. Assign a unique correlation_id to the HTTP request.
#   2. Register the correlation_id in _pending (a dict of Events).
#   3. XADD the payload to the right service stream.
#   4. Block on the Event (up to REQUEST_TIMEOUT_S seconds).
#   5. A background thread reads from the "gateway.responses" stream
#      and calls event.set() when it sees the matching correlation_id.
#   6. The main thread wakes up, reads the response, and returns HTTP.
#
# The gateway's response consumer uses plain XREAD (not XREADGROUP) with
# start_id="$".  That means it only sees responses produced after this
# gateway instance started — responses to requests that timed out are
# ignored, which is exactly what we want.  No ACK needed.

import gevent.monkey

gevent.monkey.patch_all()

import json
import logging
import os
import threading
import time
import uuid

import redis as redis_lib
from flask import Flask, Response, abort, jsonify, request

from common.streams import create_bus_pool, get_bus, ensure_groups, publish

GATEWAY_STREAMS = ["gateway.orders", "gateway.stock", "gateway.payment"]
RESPONSE_STREAM = "gateway.responses"
REQUEST_TIMEOUT_S = int(os.environ.get("REQUEST_TIMEOUT_MS", "30000")) / 1000

_STRIP_HEADERS = {"host", "connection", "transfer-encoding", "content-length"}

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = Flask("gateway-service")

# ---------------------------------------------------------------------------
# StreamClient — publish a request and block for a correlated response
# ---------------------------------------------------------------------------

class StreamClient:

    def __init__(self, bus_pool):
        self._pool = bus_pool
        # correlation_id → (threading.Event, response_dict | None)
        self._pending: dict = {}
        self._pending_lock = threading.Lock()
        self._start_response_consumer()

    def send_request(self, stream: str, payload: dict, correlation_id: str) -> dict:
        """Publish payload to stream, block until matching response arrives."""
        event = threading.Event()

        # register BEFORE publishing — avoids a race where the response
        # arrives before we have registered the event
        with self._pending_lock:
            self._pending[correlation_id] = (event, None)

        bus = get_bus(self._pool)
        try:
            publish(bus, stream, payload)
        except Exception as exc:
            self._remove_pending(correlation_id)
            raise RuntimeError(f"Failed to publish to '{stream}': {exc}") from exc

        if not event.wait(timeout=REQUEST_TIMEOUT_S):
            self._remove_pending(correlation_id)
            abort(504, description="Gateway timeout waiting for service response")

        with self._pending_lock:
            _, response = self._pending.pop(correlation_id)
        return response

    def _remove_pending(self, correlation_id: str):
        with self._pending_lock:
            self._pending.pop(correlation_id, None)

    def _start_response_consumer(self):
        """Background thread that reads gateway.responses and wakes waiting requests."""
        def consume():
            bus = get_bus(self._pool)
            # "$" means only responses produced after this gateway instance started
            last_id = "$"
            while True:
                try:
                    result = bus.xread(
                        {RESPONSE_STREAM: last_id},
                        count=100,
                        block=2000,  # wait up to 2 s, then loop
                    )
                    if not result:
                        continue
                    for _stream, entries in result:
                        for msg_id, fields in entries:
                            last_id = msg_id
                            try:
                                self._handle_response(json.loads(fields["data"]))
                            except (KeyError, json.JSONDecodeError) as exc:
                                logger.error("Malformed response entry %s: %s", msg_id, exc)
                except Exception as exc:
                    logger.error("Response consumer error, retrying in 1s: %s", exc)
                    time.sleep(1)

        threading.Thread(target=consume, daemon=True, name="stream-response-consumer").start()

    def _handle_response(self, payload: dict):
        correlation_id = payload.get("correlation_id")
        if not correlation_id:
            return
        with self._pending_lock:
            entry = self._pending.get(correlation_id)
            if entry is None:
                return  # response arrived after timeout — discard
            event, _ = entry
            self._pending[correlation_id] = (event, payload)
        event.set()


# ---------------------------------------------------------------------------
# Flask proxy routes
# ---------------------------------------------------------------------------

def _proxy(service_stream: str, subpath: str, client: StreamClient):
    full_path = f"/{subpath}" if subpath else "/"
    correlation_id = request.correlation_id

    forwarded_headers = {k: v for k, v in request.headers if k.lower() not in _STRIP_HEADERS}

    payload = {
        "method":        request.method,
        "path":          full_path,
        "correlation_id": correlation_id,
        "query_params":  dict(request.args),
        "headers":       forwarded_headers,
        "body":          request.get_json(silent=True) or dict(request.form) or None,
    }

    try:
        response = client.send_request(
            stream=service_stream,
            payload=payload,
            correlation_id=correlation_id,
        )
    except RuntimeError as exc:
        logger.error("Stream publish error for '%s': %s", service_stream, exc)
        abort(502, description="Service temporarily unavailable")

    return _build_response(response)


def _build_response(response: dict):
    status_code = response.get("status_code", 200)
    body = response.get("body", "")
    headers = response.get("headers") or {}
    if isinstance(body, (dict, list)):
        return jsonify(body), status_code, headers
    return Response(str(body), status=status_code, headers=headers)


@app.before_request
def _attach_correlation_id():
    request.correlation_id = str(uuid.uuid4())


METHODS = ["GET", "POST", "PUT", "PATCH", "DELETE"]


@app.route("/orders/", defaults={"subpath": ""}, methods=METHODS)
@app.route("/orders/<path:subpath>", methods=METHODS)
def orders_proxy(subpath):
    return _proxy("gateway.orders", subpath, stream_client)


@app.route("/stock/", defaults={"subpath": ""}, methods=METHODS)
@app.route("/stock/<path:subpath>", methods=METHODS)
def stock_proxy(subpath):
    return _proxy("gateway.stock", subpath, stream_client)


@app.route("/payment/", defaults={"subpath": ""}, methods=METHODS)
@app.route("/payment/<path:subpath>", methods=METHODS)
def payment_proxy(subpath):
    return _proxy("gateway.payment", subpath, stream_client)


@app.route("/health")
def health():
    return jsonify({"status": "healthy"})


# ---------------------------------------------------------------------------
# Startup
# ---------------------------------------------------------------------------

bus_pool = create_bus_pool()

# pre-create the response stream so the consumer has something to XREAD
# even before any service has published a response
_startup_bus = get_bus(bus_pool)
ensure_groups(_startup_bus, [(RESPONSE_STREAM, "gateway-init")])

stream_client = StreamClient(bus_pool)

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8000, debug=False)
