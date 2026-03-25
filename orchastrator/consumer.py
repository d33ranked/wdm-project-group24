"""
OFFSET COMMIT STRATEGY
-----------------------
We use *manual* offset commits rather than auto-commit.  Auto-commit
advances the offset on a timer, which can mark a message as processed
before the handler actually completes — causing silent data loss on
restart.

With manual commits:
    1. Receive message
    2. Write checkpoint (task is now durable)
    3. Run handler (possibly with retries)
    4. Commit offset ONLY after the handler succeeds

This means a message may be delivered more than once after a crash
(at-least-once), but it will *never* be silently skipped.

DEPENDENCY NOTE
---------------
This module wraps confluent-kafka (librdkafka bindings).
Install: pip install confluent-kafka
"""

from __future__ import annotations

import asyncio
import logging
from typing import  AsyncIterator

from confluent_kafka import Consumer, KafkaError, KafkaException, Message

logger = logging.getLogger(__name__)


class KafkaConsumerWrapper:
    """
    Thin async wrapper around a confluent-kafka Consumer.

    Responsibilities:
        - Subscribe to one or more topics
        - Yield Message objects to the orchestrator
        - Commit offsets only when instructed (after successful processing)
        - Log every poll error — never silently swallow them

    The wrapper deliberately knows nothing about task handlers, retries,
    or checkpoints; those concerns belong to the Orchestrator.
    """

    def __init__(self, config: dict, topics: list[str]) -> None:
        """
        Args:
            config: confluent-kafka Consumer configuration dict.
                    MUST include 'bootstrap.servers' and 'group.id'.
                    'enable.auto.commit' is forced to False here.
            topics: List of Kafka topic names to subscribe to.
        """
        # Force manual commits — auto-commit is incompatible with our
        # at-least-once guarantee.
        config = {**config, "enable.auto.commit": False}

        self._consumer = Consumer(config)
        self._topics   = topics
        self._running  = False

    def start(self) -> None:
        """Subscribe to configured topics. Call before consuming."""
        self._consumer.subscribe(self._topics)
        self._running = True
        logger.info(
            "Kafka consumer subscribed",
            extra={"topics": self._topics},
        )

    def stop(self) -> None:
        """Unsubscribe and close the consumer. Safe to call multiple times."""
        self._running = False
        self._consumer.close()
        logger.info("Kafka consumer closed")

    async def messages(self, poll_timeout: float = 1.0) -> AsyncIterator[Message]:
        """
        Async generator that yields raw Kafka Message objects.

        Polls in a thread pool so the event loop is never blocked.
        Yields nothing (continues loop) on timeout or retriable errors.
        Raises KafkaException on fatal broker errors.
        """
        loop = asyncio.get_event_loop()

        while self._running:
            # Run the blocking poll() in a thread pool executor
            msg: Message | None = await loop.run_in_executor(
                None, self._consumer.poll, poll_timeout
            )

            if msg is None:
                # Timeout — no message available; normal, keep polling
                continue

            if msg.error():
                err = msg.error()
                if err.code() == KafkaError._PARTITION_EOF:
                    # End of partition — informational, not an error
                    logger.debug(
                        "Partition EOF reached",
                        extra={"topic": msg.topic(), "partition": msg.partition()},
                    )
                    continue

                # All other errors are logged loudly.
                # UNKNOWN_TOPIC, OFFSET_OUT_OF_RANGE, etc. should not be silenced.
                logger.error(
                    "Kafka consumer error",
                    extra={
                        "code":      err.code(),
                        "name":      err.name(),
                        "topic":     msg.topic(),
                        "partition": msg.partition(),
                    },
                )

                if err.fatal():
                    # Fatal errors cannot be recovered — let the orchestrator
                    # decide whether to restart the whole consumer.
                    raise KafkaException(err)

                # Retriable — log and continue
                continue

            yield msg

    async def commit(self, message: Message) -> None:
        """
        Commit the offset for *message* synchronously.

        Must be called AFTER the task has been successfully processed
        AND the checkpoint has been cleaned up.  Committing earlier
        would risk losing the task on restart.
        """
        loop = asyncio.get_event_loop()
        await loop.run_in_executor(
            None,
            lambda: self._consumer.commit(message=message, asynchronous=False),
        )
        logger.debug(
            "Offset committed",
            extra={
                "topic":     message.topic(),
                "partition": message.partition(),
                "offset":    message.offset(),
            },
        )