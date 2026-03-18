"""
test_kafka.py
=============
Kafka helpers for the test suite.

Provides kafka_api() — a synchronous-feeling wrapper around the internal
Kafka broker that mirrors the api() HTTP helper used in all tests.

Design
------
- One ephemeral reply topic is created per test-run (e.g. "test.replies.20250318-142305").
  All kafka_api() calls in the run share this topic.  It is deleted on exit.
- Each kafka_api() call gets its own consumer group (f"cg-{correlation_id}"),
  so consumers never compete and there is no cross-contamination between calls
  or between concurrent runs.
- correlation_id is still included in every message so services can echo it
  back; the consumer reads only from its private group so no filtering is
  required, but having the id in the payload is useful for debugging.
- On timeout a FakeResponse(504) is returned — the check() pattern stays intact
  and a missing reply shows as [FAIL] rather than a traceback.

Service-side requirement
------------------------
Each service must echo `correlation_id` back into its reply payload and
publish the reply to the topic named in the `reply_to` field of the request.
If your kafka_helpers.py already forwards these fields transparently, no
service changes are needed.

Usage
-----
    from test_kafka import kafka_api, kafka_tpc, kafka_saga

    r = kafka_tpc("stock", {
        "type":     "stock.prepare",
        "txn_id":   "txn-abc",
        "item_id":  item_id,
        "quantity": 3,
    })
    check("Stock PREPARE votes YES", r.status_code == 200)
"""

import atexit
import json
import os
import uuid
import time
from datetime import datetime
from typing import Any

from kafka import KafkaConsumer, KafkaProducer
from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
INTERNAL_KAFKA = os.environ.get(
    "INTERNAL_KAFKA_BOOTSTRAP_SERVERS", "localhost:9093"
)
DEFAULT_TIMEOUT = 15   # seconds to wait for a reply

# ---------------------------------------------------------------------------
# Ephemeral reply topic — one per test-run, deleted on exit
# ---------------------------------------------------------------------------
_RUN_ID         = datetime.now().strftime("%y%m%d-%H%M%S")
REPLY_TOPIC     = f"test.replies.{_RUN_ID}"
_topic_ready    = False   # created lazily on first kafka_api() call


def _ensure_reply_topic():
    """Create the ephemeral reply topic if it does not yet exist."""
    global _topic_ready
    if _topic_ready:
        return
    admin = KafkaAdminClient(bootstrap_servers=INTERNAL_KAFKA)
    try:
        admin.create_topics([NewTopic(
            name=REPLY_TOPIC,
            num_partitions=1,
            replication_factor=1,
        )])
        print(f"  [kafka] created ephemeral reply topic: {REPLY_TOPIC}")
    except TopicAlreadyExistsError:
        pass
    finally:
        admin.close()
    _topic_ready = True


def _delete_reply_topic():
    """Delete the ephemeral reply topic on process exit."""
    if not _topic_ready:
        return
    try:
        admin = KafkaAdminClient(bootstrap_servers=INTERNAL_KAFKA)
        admin.delete_topics([REPLY_TOPIC])
        admin.close()
        print(f"  [kafka] deleted ephemeral reply topic: {REPLY_TOPIC}")
    except Exception:
        pass   # best-effort cleanup


atexit.register(_delete_reply_topic)


# ---------------------------------------------------------------------------
# FakeResponse — drop-in replacement for requests.Response
# ---------------------------------------------------------------------------
class FakeResponse:
    """
    Wraps a Kafka reply so existing check() calls work unchanged.

        r.status_code   — taken from payload["status_code"] (default 200)
        r.json()        — returns the full payload dict
        r.text          — JSON-serialised payload string
    """

    def __init__(self, payload: dict):
        self._payload    = payload
        self.status_code = int(payload.get("status_code", 200))
        self.text        = json.dumps(payload)

    def json(self) -> dict:
        return self._payload

    def __repr__(self) -> str:
        return f"<FakeResponse status={self.status_code} payload={self._payload}>"


class TimeoutResponse(FakeResponse):
    """Returned when no reply arrives within the timeout window."""

    def __init__(self, correlation_id: str, topic: str, timeout: int):
        super().__init__({
            "status_code": 504,
            "type":        "test.timeout",
            "detail": (
                f"No reply received within {timeout}s "
                f"(sent to={topic}, reply_topic={REPLY_TOPIC}, "
                f"correlation_id={correlation_id})"
            ),
        })


# ---------------------------------------------------------------------------
# Module-level producer — created once, reused across calls
# ---------------------------------------------------------------------------
_producer: KafkaProducer | None = None


def _get_producer() -> KafkaProducer:
    global _producer
    if _producer is None:
        _producer = KafkaProducer(
            bootstrap_servers=INTERNAL_KAFKA,
            value_serializer=lambda v: json.dumps(v).encode(),
        )
    return _producer


# ---------------------------------------------------------------------------
# Core helper
# ---------------------------------------------------------------------------
def kafka_api(
    topic: str,
    payload: dict[str, Any],
    *,
    timeout: int = DEFAULT_TIMEOUT,
) -> FakeResponse:
    """
    Send `payload` to `topic` on the internal Kafka broker and block until
    the reply arrives on this run's ephemeral reply topic (or timeout).

    `correlation_id` and `reply_to` are injected automatically.
    Each call uses its own consumer group so messages are never shared or
    missed between concurrent or sequential calls.
    """
    _ensure_reply_topic()

    correlation_id = str(uuid.uuid4())
    message = {
        **payload,
        "correlation_id": correlation_id,
        "reply_to":        REPLY_TOPIC,
    }

    # ---- Produce -------------------------------------------------------
    _get_producer().send(topic, value=message)
    _get_producer().flush()

    # ---- Consume — private group, reads from latest offset -------------
    # Using auto_offset_reset="earliest" on an empty, brand-new group means
    # we catch the reply even if it arrives before the consumer fully joins.
    group_id = f"cg-{correlation_id}"
    consumer = KafkaConsumer(
        REPLY_TOPIC,
        bootstrap_servers=INTERNAL_KAFKA,
        group_id=group_id,
        auto_offset_reset="earliest",
        enable_auto_commit=False,
        value_deserializer=lambda b: json.loads(b.decode()),
    )

    deadline = time.time() + timeout
    try:
        while time.time() < deadline:
            remaining_ms = int((deadline - time.time()) * 1000)
            if remaining_ms <= 0:
                break
            records = consumer.poll(timeout_ms=min(remaining_ms, 500))
            for _, messages in records.items():
                for msg in messages:
                    if msg.value.get("correlation_id") == correlation_id:
                        return FakeResponse(msg.value)
    finally:
        consumer.close()

    return TimeoutResponse(correlation_id, topic, timeout)


# ---------------------------------------------------------------------------
# Convenience wrappers
# ---------------------------------------------------------------------------
def kafka_tpc(service: str, payload: dict[str, Any], **kwargs) -> FakeResponse:
    """Send a 2PC command to internal.{service}.tpc"""
    return kafka_api(f"internal.{service}.tpc", payload, **kwargs)


def kafka_saga(service: str, payload: dict[str, Any], **kwargs) -> FakeResponse:
    """Send a SAGA command to internal.{service}.saga"""
    return kafka_api(f"internal.{service}.saga", payload, **kwargs)