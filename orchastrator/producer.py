"""
PRODUCER RELIABILITY SETTINGS
------------------------------
We configure the underlying producer for maximum durability:

    acks=all          — broker writes to all in-sync replicas before
                        acknowledging; no data loss on leader failover.

    enable.idempotence=true
                      — prevents duplicate messages on retry; requires
                        acks=all and retries > 0 (set automatically).

    retries           — librdkafka retries transient send failures
                        internally; combined with idempotence this is safe.

These settings trade some throughput for correctness — the right
trade-off for a distributed data system where loss is unacceptable.
"""

from __future__ import annotations

import asyncio
import logging
from typing import Any, Optional

from confluent_kafka import Producer, KafkaException
from confluent_kafka.serialization import StringSerializer

logger = logging.getLogger(__name__)

# Shared serialiser for string keys
_str_ser = StringSerializer("utf_8")


class KafkaProducerWrapper:
    """
    Async wrapper around a confluent-kafka Producer with delivery callbacks.

    Every produce() call:
        1. Serializes the message
        2. Registers a delivery callback (fires on broker ack or error)
        3. Returns an asyncio.Future that resolves when the broker confirms

    This means an awaited produce() will not return until the message is
    durable in Kafka — preventing silent produce failures.
    """

    def __init__(self, config: dict) -> None:
        """
        Args:
            config: confluent-kafka Producer configuration dict.
                    MUST include 'bootstrap.servers'.
                    acks, idempotence, and retries are merged in below.
        """
        # Override safety-critical settings — callers should not be able to
        # accidentally weaken delivery guarantees.
        safe_config = {
            **config,
            "acks":               "all",
            "enable.idempotence": True,
            "retries":            10,
        }
        self._producer = Producer(safe_config)
        self._loop     = asyncio.get_event_loop()

    async def produce(
        self,
        topic:     str,
        value:     Any,
        key:       Optional[str] = None,
        serializer: Any          = None,
    ) -> None:
        """
        Publish *value* to *topic* and await broker confirmation.

        Args:
            topic:      Destination Kafka topic.
            value:      Message payload — serialized by *serializer* if
                        provided, otherwise encoded as UTF-8 bytes.
            key:        Optional message key (string).
            serializer: Callable(value) -> bytes.  Defaults to str→bytes.

        Raises:
            KafkaException: if the broker rejects the message after all
                            internal retries.  The caller should handle
                            this and move the task to the DLQ.
        """
        future: asyncio.Future = self._loop.create_future()

        def _on_delivery(err, msg):
            """
            Delivery callback executed by librdkafka's poll thread.
            We schedule the future resolution on the event loop from here.
            """
            if err is not None:
                self._loop.call_soon_threadsafe(
                    future.set_exception,
                    KafkaException(err),
                )
                logger.error(
                    "Kafka produce failed",
                    extra={
                        "topic":  topic,
                        "key":    key,
                        "error":  str(err),
                    },
                )
            else:
                self._loop.call_soon_threadsafe(future.set_result, None)
                logger.debug(
                    "Message delivered",
                    extra={
                        "topic":     msg.topic(),
                        "partition": msg.partition(),
                        "offset":    msg.offset(),
                    },
                )

        # Serialize the value
        if serializer is not None:
            encoded = serializer(value)
        elif isinstance(value, bytes):
            encoded = value
        else:
            encoded = str(value).encode("utf-8")

        try:
            self._producer.produce(
                topic    = topic,
                value    = encoded,
                key      = _str_ser(key) if key else None,
                callback = _on_delivery,
            )
            # Trigger delivery callbacks for any pending messages
            self._producer.poll(0)
        except BufferError:
            # Producer queue full — let the event loop breathe and retry
            logger.warning(
                "Producer queue full, flushing before retry",
                extra={"topic": topic},
            )
            await self.flush()
            # Re-raise so the orchestrator can decide whether to retry the task
            raise

        # Wait for the broker to confirm (or for the delivery callback to
        # report an error).  This is what makes produce() safe to await.
        await future

    async def flush(self, timeout: float = 10.0) -> None:
        """
        Block until all buffered messages have been delivered or timeout.

        Call on graceful shutdown to avoid losing messages that were
        produced but not yet confirmed.
        """
        loop = asyncio.get_event_loop()
        remaining = await loop.run_in_executor(
            None, lambda: self._producer.flush(timeout)
        )
        if remaining > 0:
            logger.warning(
                "Producer flush timed out with undelivered messages",
                extra={"undelivered": remaining, "timeout_s": timeout},
            )
        else:
            logger.info("Producer flushed cleanly")