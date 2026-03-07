import json
import logging
import os
import threading
import uuid
from concurrent.futures import ThreadPoolExecutor
from time import time
from typing import Any

import kafka
import requests
from flask import Flask, Response, abort, jsonify, request
from kafka.admin import NewTopic
from kafka.errors import KafkaError

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

KAFKA_BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
REQUEST_TIMEOUT_S = int(os.environ.get("REQUEST_TIMEOUT_MS", "30000")) / 1000
BRIDGE_WORKER_THREADS = int(os.environ.get("BRIDGE_WORKER_THREADS", "16"))

SERVICE_URLS: dict[str, str] = {
    "orders": os.environ.get("ORDER_SERVICE_URL", "http://order-service:5000"),
    "stock": os.environ.get("STOCK_SERVICE_URL", "http://stock-service:5000"),
    "payment": os.environ.get("PAYMENT_SERVICE_URL", "http://payment-service:5000"),
}

GATEWAY_TOPICS = ["gateway.orders", "gateway.stock", "gateway.payment"]
RESPONSE_TOPIC = "gateway.responses"

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = Flask("gateway-service")


# ---------------------------------------------------------------------------
# Kafka helpers
# ---------------------------------------------------------------------------

def _build_producer(bootstrap_servers: str) -> kafka.KafkaProducer:
    return kafka.KafkaProducer(
        bootstrap_servers=bootstrap_servers,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8") if k else None,
        acks="all",
        retries=3,
        linger_ms=5,          # small batching window improves throughput
        batch_size=32_768,
    )


def ensure_topics(bootstrap_servers: str, topics: list[str]) -> None:
    try:
        admin = kafka.KafkaAdminClient(
            bootstrap_servers=bootstrap_servers,
            request_timeout_ms=10_000,
        )
        existing = set(admin.list_topics())
        missing = [t for t in topics if t not in existing]
        if missing:
            admin.create_topics(
                [NewTopic(name=t, num_partitions=3, replication_factor=1) for t in missing],
                validate_only=False,
            )
            logger.info("Created Kafka topics: %s", missing)
        admin.close()
    except KafkaError as exc:
        logger.warning("Could not ensure topics exist: %s", exc)


# ---------------------------------------------------------------------------
# KafkaClient — one shared response consumer, event-based unblocking
# ---------------------------------------------------------------------------

class KafkaClient:
    """
    Sends a request message to a service topic and blocks the calling thread
    until the correlated response arrives on ``gateway.responses``.

    A *single* background consumer handles all in-flight responses, avoiding
    the O(N) consumer-per-request anti-pattern.
    """

    def __init__(self, bootstrap_servers: str) -> None:
        self.bootstrap_servers = bootstrap_servers
        self._producer = _build_producer(bootstrap_servers)

        # correlation_id → (threading.Event, response_dict)
        self._pending: dict[str, tuple[threading.Event, dict | None]] = {}
        self._pending_lock = threading.Lock()

        self._start_response_consumer()

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def send_request(
        self,
        topic: str,
        payload: dict[str, Any],
        correlation_id: str,
    ) -> dict[str, Any]:
        event = threading.Event()

        with self._pending_lock:
            self._pending[correlation_id] = (event, None)

        try:
            self._producer.send(topic, key=correlation_id, value=payload).get(timeout=10)
        except KafkaError as exc:
            self._remove_pending(correlation_id)
            raise RuntimeError(f"Failed to enqueue message: {exc}") from exc

        if not event.wait(timeout=REQUEST_TIMEOUT_S):
            self._remove_pending(correlation_id)
            abort(504, description="Gateway timeout waiting for service response")

        with self._pending_lock:
            _, response = self._pending.pop(correlation_id)

        return response  # type: ignore[return-value]

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _remove_pending(self, correlation_id: str) -> None:
        with self._pending_lock:
            self._pending.pop(correlation_id, None)

    def _start_response_consumer(self) -> None:
        """Runs a single long-lived consumer on a daemon thread."""

        def consume() -> None:
            consumer = kafka.KafkaConsumer(
                RESPONSE_TOPIC,
                bootstrap_servers=self.bootstrap_servers,
                group_id="gateway-response-listener",
                auto_offset_reset="latest",   # only messages produced after startup
                enable_auto_commit=True,
                value_deserializer=lambda m: json.loads(m.decode("utf-8")),
            )
            logger.info("Response consumer started on topic '%s'", RESPONSE_TOPIC)
            try:
                for message in consumer:
                    self._handle_response(message.value)
            except Exception as exc:
                logger.error("Response consumer crashed: %s", exc, exc_info=True)
            finally:
                consumer.close()

        thread = threading.Thread(target=consume, daemon=True, name="kafka-response-consumer")
        thread.start()

    def _handle_response(self, payload: dict[str, Any]) -> None:
        correlation_id = payload.get("correlation_id")
        if not correlation_id:
            return

        with self._pending_lock:
            entry = self._pending.get(correlation_id)
            if entry is None:
                # Response arrived after timeout — discard
                return
            event, _ = entry
            self._pending[correlation_id] = (event, payload)

        event.set()


# ---------------------------------------------------------------------------
# KafkaBridge — consumes service topics, forwards to microservices over HTTP
# ---------------------------------------------------------------------------

class KafkaBridge:
    """
    Reads messages from the service topics, forwards each one as an HTTP
    request to the appropriate microservice, and publishes the response back
    to ``gateway.responses``.

    HTTP calls are dispatched to a thread pool so that a single slow service
    cannot stall processing of unrelated topics.
    """

    def __init__(self, bootstrap_servers: str, service_urls: dict[str, str]) -> None:
        self.bootstrap_servers = bootstrap_servers
        self.service_urls = service_urls
        self._producer = _build_producer(bootstrap_servers)
        self._executor = ThreadPoolExecutor(
            max_workers=BRIDGE_WORKER_THREADS,
            thread_name_prefix="bridge-worker",
        )
        self._http_session = requests.Session()
        self.running = False

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    def start(self) -> None:
        self.running = True
        consumer = kafka.KafkaConsumer(
            *GATEWAY_TOPICS,
            bootstrap_servers=self.bootstrap_servers,
            group_id="gateway-bridge",
            auto_offset_reset="earliest",
            enable_auto_commit=True,
            value_deserializer=lambda m: json.loads(m.decode("utf-8")),
        )
        logger.info("Kafka bridge consuming from: %s", GATEWAY_TOPICS)
        try:
            while self.running:
                for message in consumer:
                    if not self.running:
                        break
                    # Fire-and-forget into the thread pool; consumer loop stays unblocked
                    self._executor.submit(self._process_message, message.topic, message.value)
        finally:
            consumer.close()
            self._executor.shutdown(wait=False)
            logger.info("Kafka bridge stopped")

    def stop(self) -> None:
        self.running = False

    # ------------------------------------------------------------------
    # Message processing
    # ------------------------------------------------------------------

    def _process_message(self, topic: str, payload: dict[str, Any]) -> None:
        correlation_id = payload.get("correlation_id")
        if not correlation_id:
            logger.warning("Dropped message on '%s': missing correlation_id", topic)
            return

        service_name = topic.split(".")[-1]
        service_url = self.service_urls.get(service_name)
        if not service_url:
            logger.error("No service URL configured for topic '%s'", topic)
            self._publish_response(correlation_id, 502, "Service not configured")
            return

        response = self._forward_to_service(payload, service_url)
        self._publish_response(
            correlation_id,
            response["status_code"],
            response["body"],
        )

    def _forward_to_service(self, payload: dict[str, Any], service_url: str) -> dict[str, Any]:
        method = payload.get("method", "GET").upper()
        path = payload.get("path", "/")
        body = payload.get("body") or {}
        raw_headers: dict[str, str] = payload.get("headers") or {}

        # Strip hop-by-hop headers that should not be forwarded
        skip = {"host", "connection", "transfer-encoding", "content-length"}
        headers = {k: v for k, v in raw_headers.items() if k.lower() not in skip}

        url = f"{service_url}{path}"
        dispatch = {
            "GET": lambda: self._http_session.get(url, headers=headers, timeout=30),
            "POST": lambda: self._http_session.post(url, headers=headers, json=body, timeout=30),
            "PUT": lambda: self._http_session.put(url, headers=headers, json=body, timeout=30),
            "PATCH": lambda: self._http_session.patch(url, headers=headers, json=body, timeout=30),
            "DELETE": lambda: self._http_session.delete(url, headers=headers, timeout=30),
        }

        handler = dispatch.get(method)
        if handler is None:
            return {"status_code": 405, "body": f"Method '{method}' not supported"}

        try:
            resp = handler()
        except requests.exceptions.RequestException as exc:
            logger.error("HTTP error forwarding to %s: %s", url, exc)
            return {"status_code": 502, "body": f"Service unavailable: {exc}"}

        try:
            resp_body = resp.json()
        except ValueError:
            resp_body = resp.text

        return {"status_code": resp.status_code, "body": resp_body}

    def _publish_response(
        self,
        correlation_id: str,
        status_code: int,
        body: Any,
    ) -> None:
        payload = {
            "correlation_id": correlation_id,
            "status_code": status_code,
            "body": body,
        }
        try:
            self._producer.send(RESPONSE_TOPIC, key=correlation_id, value=payload)
            self._producer.flush(timeout=5)
        except KafkaError as exc:
            logger.error("Failed to publish response for %s: %s", correlation_id, exc)


# ---------------------------------------------------------------------------
# Flask proxy routes
# ---------------------------------------------------------------------------

def _proxy(service_topic: str, subpath: str) -> Response:
    """Generic proxy handler shared by all service routes."""
    full_path = f"/{subpath}" if subpath else "/"
    correlation_id: str = request.correlation_id  # type: ignore[attr-defined]

    payload: dict[str, Any] = {
        "method": request.method,
        "path": full_path,
        "correlation_id": correlation_id,
        "query_params": dict(request.args),
        "headers": dict(request.headers),
        "body": request.get_json(silent=True) or dict(request.form) or None,
    }

    try:
        response = kafka_client.send_request(
            topic=service_topic,
            payload=payload,
            correlation_id=correlation_id,
        )
    except RuntimeError as exc:
        logger.error("Kafka send error for topic '%s': %s", service_topic, exc)
        abort(502, description="Service temporarily unavailable")

    return _build_response(response)


def _build_response(response: dict[str, Any]) -> Response:
    status_code: int = response.get("status_code", 200)
    body: Any = response.get("body", "")
    headers: dict = response.get("headers") or {}

    if isinstance(body, (dict, list)):
        return jsonify(body), status_code, headers
    return Response(str(body), status=status_code, headers=headers)


@app.before_request
def _attach_correlation_id() -> None:
    request.correlation_id = str(uuid.uuid4())


METHODS = ["GET", "POST", "PUT", "PATCH", "DELETE"]

@app.route("/orders/", defaults={"subpath": ""}, methods=METHODS)
@app.route("/orders/<path:subpath>", methods=METHODS)
def orders_proxy(subpath: str) -> Response:
    return _proxy("gateway.orders", subpath)


@app.route("/stock/", defaults={"subpath": ""}, methods=METHODS)
@app.route("/stock/<path:subpath>", methods=METHODS)
def stock_proxy(subpath: str) -> Response:
    return _proxy("gateway.stock", subpath)


@app.route("/payment/", defaults={"subpath": ""}, methods=METHODS)
@app.route("/payment/<path:subpath>", methods=METHODS)
def payment_proxy(subpath: str) -> Response:
    return _proxy("gateway.payment", subpath)


@app.route("/health")
def health() -> Response:
    return jsonify({"status": "healthy"})


# ---------------------------------------------------------------------------
# Entrypoint
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    ensure_topics(
        KAFKA_BOOTSTRAP_SERVERS,
        GATEWAY_TOPICS + [RESPONSE_TOPIC],
    )

    kafka_client = KafkaClient(KAFKA_BOOTSTRAP_SERVERS)
    kafka_bridge = KafkaBridge(KAFKA_BOOTSTRAP_SERVERS, SERVICE_URLS)

    bridge_thread = threading.Thread(
        target=kafka_bridge.start,
        daemon=True,
        name="kafka-bridge",
    )
    bridge_thread.start()

    app.run(host="0.0.0.0", port=8000, debug=False)