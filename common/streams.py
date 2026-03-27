# Redis Streams messaging — replaces Kafka for SAGA mode.
#
# Conceptual mapping
# ------------------
# Kafka topic          → Redis Stream key  (e.g. "gateway.orders")
# Kafka producer.send  → bus.xadd(stream, {"data": json_payload})
# Kafka consumer group → bus.xgroup_create(stream, group, ...)
# consumer.poll        → bus.xreadgroup(group, consumer, {stream: ">"}, block=5000)
# consumer.commit      → bus.xack(stream, group, message_id)
#
# The critical extra feature over Kafka: pending messages.
# When a service crashes mid-processing, its unACKed messages stay in a
# "pending" list.  On restart the service reads with start_id="0" first,
# which re-delivers every pending message before fetching new ones.
# This gives at-least-once delivery with zero extra code on each service.
#
# All message payloads are serialised as a single JSON string stored in
# a "data" field.  Flat key-value serialisation would require escaping
# nested dicts/lists; one JSON blob keeps things simple and uniform.

import json
import logging
import os

import redis as redis_lib

logger = logging.getLogger(__name__)

# how long each blocking XREADGROUP call waits before returning empty
# keeping this short (2 s) makes consumer loops feel responsive on startup
BLOCK_MS = 2_000

# fixed consumer name — we run one worker per service, so all pending
# messages from before a crash are always owned by the same name
CONSUMER_NAME = "worker"


# ---------------------------------------------------------------------------
# Pool / client
# ---------------------------------------------------------------------------

def create_bus_pool() -> redis_lib.ConnectionPool:
    """Create a connection pool to the shared redis-bus instance.

    Every service imports this once at startup.  The bus is separate from
    each service's storage Redis so that killing redis-stock does not drop
    in-flight checkout messages.
    """
    host = os.environ.get("REDIS_BUS_HOST", "redis-bus")
    port = int(os.environ.get("REDIS_BUS_PORT", 6379))
    pool = redis_lib.ConnectionPool(
        host=host,
        port=port,
        max_connections=20,
        decode_responses=True,
        socket_keepalive=True,
        socket_connect_timeout=2,
        socket_timeout=15,
    )
    logger.info("Redis bus pool → %s:%s", host, port)
    return pool


def get_bus(pool: redis_lib.ConnectionPool) -> redis_lib.Redis:
    """Borrow a connection from the bus pool (cheap — just socket checkout)."""
    return redis_lib.Redis(connection_pool=pool)


# ---------------------------------------------------------------------------
# Group setup
# ---------------------------------------------------------------------------

def ensure_groups(bus: redis_lib.Redis, stream_groups: list):
    """Create consumer groups (and streams) if they do not already exist.

    stream_groups is a list of (stream_name, group_name) pairs.

    id="0" means the group's cursor starts at the very beginning of the
    stream — so messages written before the service started are not silently
    dropped.  mkstream=True creates the stream key if it does not exist.

    On service restart this is called again; the "BUSYGROUP" error means the
    group already exists, which is normal and harmless.
    """
    for stream, group in stream_groups:
        try:
            bus.xgroup_create(stream, group, id="0", mkstream=True)
        except redis_lib.exceptions.ResponseError as exc:
            if "BUSYGROUP" not in str(exc):
                raise


# ---------------------------------------------------------------------------
# Publishing
# ---------------------------------------------------------------------------

def publish(bus: redis_lib.Redis, stream: str, payload: dict):
    """Append one message to a stream.

    MAXLEN ~ 50000 keeps the stream bounded.  The tilde (~) means Redis
    trims approximately rather than exactly — much faster because it only
    trims at radix-tree node boundaries.
    """
    bus.xadd(stream, {"data": json.dumps(payload)}, maxlen=50_000, approximate=True)


# ---------------------------------------------------------------------------
# Consuming
# ---------------------------------------------------------------------------

def read_pending_then_new(bus: redis_lib.Redis, stream: str, group: str) -> list:
    """Read unACKed (pending) messages first, then block for new ones.

    Returns a list of (message_id, payload_dict) tuples.

    Two-phase read pattern:
    1. "0"  → re-deliver messages that were delivered before but never ACKed
              (i.e. the service crashed before calling ack()).  This drains
              the pending list left over from before the restart.
    2. ">"  → deliver fresh messages not yet seen by this consumer group.
              Blocks up to BLOCK_MS milliseconds if the stream is empty.

    Callers should call ack() for each message after successfully processing
    it.  If they crash before ack(), the message will be re-delivered on the
    next startup — hence at-least-once semantics.
    """
    # phase 1 — pending from before crash
    pending = _xreadgroup(bus, stream, group, start_id="0", block=False)
    if pending:
        return pending
    # phase 2 — new messages (blocking)
    return _xreadgroup(bus, stream, group, start_id=">", block=True)


def ack(bus: redis_lib.Redis, stream: str, group: str, message_id: str):
    """Acknowledge a message — remove it from the pending list.

    Call this only after the message has been fully processed and any
    response has been published.  If the service crashes between processing
    and ack(), the message is re-delivered — make sure handlers are
    idempotent (they already are, via the idempotency key mechanism).
    """
    bus.xack(stream, group, message_id)


# ---------------------------------------------------------------------------
# Internal helper
# ---------------------------------------------------------------------------

def _xreadgroup(bus, stream, group, start_id, block):
    try:
        result = bus.xreadgroup(
            group,
            CONSUMER_NAME,
            {stream: start_id},
            count=10,
            block=BLOCK_MS if block else None,
        )
    except redis_lib.exceptions.ResponseError as exc:
        # group does not exist yet (race on very first startup) — loop retries
        logger.warning("xreadgroup error on '%s': %s", stream, exc)
        return []

    if not result:
        return []

    messages = []
    for _stream_name, entries in result:
        for msg_id, fields in entries:
            try:
                payload = json.loads(fields["data"])
                messages.append((msg_id, payload))
            except (KeyError, json.JSONDecodeError) as exc:
                logger.error("Malformed stream entry %s on '%s': %s", msg_id, stream, exc)
                # ACK malformed entries so they are not re-delivered forever
                bus.xack(stream, group, msg_id)
    return messages
