"""
THE ABSTRACTION CONTRACT
-------------------------
Business logic lives exclusively in the handler functions that callers
register.  The orchestrator's only job is:

    1. Consume messages from Kafka topics
    2. Build a Task from each message
    3. Checkpoint the task (durable before any processing starts)
    4. Dispatch to the registered handler via RetryHandler
    5. On success  → publish the TaskResult, commit offset, clean checkpoint
    6. On failure  → publish to dead-letter queue, commit offset, log
    7. On startup  → recover any tasks orphaned by a previous crash

The orchestrator never inspects message payloads, never makes routing
decisions based on content, and never knows what the business domain is.
All of that is in the handler.

HANDLER SIGNATURE
-----------------
    async def my_handler(task: Task) -> TaskResult: ...

Handlers receive a Task and return a TaskResult.  They should raise on
transient failures (network timeouts, service unavailable) so the retry
machinery fires.  They should return TaskResult.fail() for permanent
business-logic failures that should not be retried.

USAGE
-----
    from kafka_orchestrator import Orchestrator, Task, TaskResult

    orch = Orchestrator(
        consumer_config   = {"bootstrap.servers": "localhost:9092", "group.id": "svc"},
        producer_config   = {"bootstrap.servers": "localhost:9092"},
        checkpoint_dir    = "/var/lib/myservice/checkpoints",
        dlq_topic         = "my-service.dlq",
    )

    @orch.handler("orders.created")
    async def handle_order(task: Task) -> TaskResult:
        order = task.value                          # business object
        result = await process_order(order)        # your logic here
        return TaskResult.ok("orders.processed", result)

    asyncio.run(orch.run())
"""

from __future__ import annotations

import asyncio
import json
import logging
from typing import Callable, Awaitable

from .checkpoint import CheckpointStore
from .consumer import KafkaConsumerWrapper
from .producer import KafkaProducerWrapper
from .retry import RetryHandler, RetryPolicy
from .task import Task, TaskResult, TaskStatus

logger = logging.getLogger(__name__)


class Orchestrator:
    """
    Kafka-backed task orchestrator library.

    Wires together the consumer, producer, retry handler, and checkpoint
    store into a single cohesive loop.  Callers attach handlers for
    specific topics and then call run().

    Attributes:
        _handlers:  topic → async handler function
        _consumer:  wraps confluent-kafka Consumer
        _producer:  wraps confluent-kafka Producer
        _retry:     executes handlers with retry + backoff
        _store:     persists in-flight task state to disk
        _dlq_topic: destination for tasks that exhaust all retries
    """

    def __init__(
        self,
        consumer_config:  dict,
        producer_config:  dict,
        checkpoint_dir:   str,
        dlq_topic:        str,
        retry_policy:     RetryPolicy | None = None,
        max_concurrency:  int                = 10,
    ) -> None:
        """
        Args:
            consumer_config:  confluent-kafka Consumer config dict.
                              Must include 'bootstrap.servers' and 'group.id'.
            producer_config:  confluent-kafka Producer config dict.
                              Must include 'bootstrap.servers'.
            checkpoint_dir:   Directory path for checkpoint files.
                              Created automatically if it doesn't exist.
            dlq_topic:        Kafka topic for permanently-failed tasks.
                              Every service should define one.
            retry_policy:     Override the default backoff behaviour.
            max_concurrency:  Maximum tasks processed in parallel.
                              Prevents a single slow downstream from
                              exhausting memory with unbounded task queues.
        """
        self._handlers:   dict[str, Callable[[Task], Awaitable[TaskResult]]] = {}
        self._dlq_topic   = dlq_topic
        self._semaphore   = asyncio.Semaphore(max_concurrency)
        self._consumer    = KafkaConsumerWrapper(consumer_config, topics=[])  # topics added via handler()
        self._producer    = KafkaProducerWrapper(producer_config)
        self._retry       = RetryHandler(retry_policy)
        self._store       = CheckpointStore(checkpoint_dir)

    # ------------------------------------------------------------------
    # Public API — called by service authors
    # ------------------------------------------------------------------

    def handler(
        self,
        topic:       str,
        max_retries: int = 3,
    ) -> Callable:
        """
        Decorator that registers an async function as the handler for *topic*.

        Usage::

            @orch.handler("payments.received", max_retries=5)
            async def handle_payment(task: Task) -> TaskResult:
                ...

        The decorated function must:
            - Accept a single Task argument
            - Return a TaskResult
            - Raise on transient failures (will be retried)
            - Return TaskResult.fail() on permanent failures (goes to DLQ)
        """
        def decorator(fn: Callable[[Task], Awaitable[TaskResult]]):
            if topic in self._handlers:
                raise ValueError(
                    f"A handler for topic '{topic}' is already registered. "
                    "Each topic may have exactly one handler."
                )

            # Wrap the handler to ensure the max_retries setting is baked in
            async def _wrapped(task: Task) -> TaskResult:
                task.max_retries = max_retries
                return await fn(task)

            self._handlers[topic] = _wrapped
            logger.info(
                "Handler registered",
                extra={"topic": topic, "handler": fn.__name__, "max_retries": max_retries},
            )
            return fn  # return original so it can still be called directly in tests

        return decorator

    async def run(self) -> None:
        """
        Start the consume-process-commit loop.  Runs until cancelled.

        On startup:
            - Recovers any tasks orphaned by a previous crash
            - Subscribes to all registered topics

        The loop:
            - Polls Kafka for messages
            - Dispatches each message to its handler (concurrently, bounded)
            - Commits offsets only after successful processing + checkpoint cleanup
        """
        if not self._handlers:
            raise RuntimeError(
                "No handlers registered. Use @orch.handler('topic') before calling run()."
            )

        # Step 1: Recover orphaned in-flight tasks from a previous run
        await self._recover_from_checkpoint()

        # Step 2: Subscribe the consumer to all registered topics
        self._consumer._topics = list(self._handlers.keys())
        self._consumer.start()

        logger.info(
            "Orchestrator running",
            extra={"topics": list(self._handlers.keys()), "max_concurrency": self._semaphore._value},
        )

        try:
            async for msg in self._consumer.messages():
                # Acquire the semaphore before spawning a task so we don't
                # build an unbounded backlog of concurrent handlers.
                await self._semaphore.acquire()
                asyncio.create_task(self._process_message(msg))

        except asyncio.CancelledError:
            logger.info("Orchestrator received cancellation, shutting down")
        finally:
            await self._shutdown()

    # ------------------------------------------------------------------
    # Internals — not part of the public API
    # ------------------------------------------------------------------

    async def _process_message(self, msg) -> None:
        """
        Full lifecycle for a single Kafka message.

        Designed to run concurrently via asyncio.create_task().
        The semaphore is released in the finally block regardless of outcome.
        """
        try:
            # Deserialize — attempt JSON, fall back to raw bytes
            try:
                value = json.loads(msg.value())
            except (json.JSONDecodeError, TypeError):
                value = msg.value()  # pass raw bytes to the handler

            task = Task(
                topic     = msg.topic(),
                partition = msg.partition(),
                offset    = msg.offset(),
                key       = msg.key(),
                value     = value,
            )

            handler = self._handlers.get(task.topic)
            if handler is None:
                # This should never happen because we only subscribe to
                # topics with registered handlers, but log just in case.
                logger.error(
                    "No handler for topic — message dropped",
                    extra={"topic": task.topic, "offset": task.offset},
                )
                return

            # Checkpoint BEFORE dispatching — if we crash mid-handler,
            # the task is recoverable on next startup.
            await self._store.save(task)

            try:
                # Dispatch with retry logic
                result: TaskResult = await self._retry.run(task, handler)

                if result.success and result.output_topic:
                    # Publish the result to the downstream topic
                    await self._producer.produce(
                        topic = result.output_topic,
                        value = result.output_value,
                        key   = task.task_id,
                    )

                elif not result.success:
                    # Handler returned a permanent failure (no exception raised)
                    # — treat as DLQ without additional retries.
                    logger.warning(
                        "Handler returned permanent failure, sending to DLQ",
                        extra={
                            "task_id": task.task_id,
                            "reason":  result.error_message,
                        },
                    )
                    await self._send_to_dlq(task, result.error_message or "handler returned failure")

            except Exception as exc:
                # RetryHandler already exhausted all retries and re-raised.
                await self._send_to_dlq(task, str(exc))

            finally:
                # Clean up checkpoint BEFORE committing the offset.
                # If we crash between these two operations, the worst case
                # is a duplicate delivery (at-least-once), not data loss.
                await self._store.delete(task)
                await self._consumer.commit(msg)

        except Exception as exc:
            # Unexpected errors outside the handler lifecycle — log loudly
            # but don't crash the whole consumer loop.
            logger.critical(
                "Unexpected error processing message",
                extra={"topic": msg.topic(), "offset": msg.offset(), "error": str(exc)},
                exc_info=True,
            )
        finally:
            self._semaphore.release()

    async def _send_to_dlq(self, task: Task, reason: str) -> None:
        """
        Publish a failed task to the dead-letter queue topic.

        The DLQ message includes full task metadata so operators can
        inspect failures, replay them, or build alerting on top.
        """
        task.status = TaskStatus.DLQ
        task.touch()

        dlq_payload = {
            **task.to_checkpoint(),
            "dlq_reason": reason,
        }

        try:
            await self._producer.produce(
                topic = self._dlq_topic,
                value = json.dumps(dlq_payload),
                key   = task.task_id,
            )
            logger.error(
                "Task sent to DLQ",
                extra={
                    "task_id":  task.task_id,
                    "topic":    task.topic,
                    "offset":   task.offset,
                    "reason":   reason,
                },
            )
        except Exception as exc:
            # DLQ produce failure is a critical data-safety issue.
            # Log at CRITICAL so on-call can be alerted.
            logger.critical(
                "CRITICAL: Failed to publish task to DLQ — potential data loss",
                extra={
                    "task_id": task.task_id,
                    "topic":   task.topic,
                    "offset":  task.offset,
                    "reason":  reason,
                    "error":   str(exc),
                },
                exc_info=True,
            )

    async def _recover_from_checkpoint(self) -> None:
        """
        On startup, find tasks that were in-flight when we last crashed.

        Currently logs them and sends them to the DLQ for manual review.
        You can extend this to replay them by re-producing their original
        messages to the input topic, or to skip them if you detect duplicates
        via an idempotency key in your business logic.
        """
        orphans = await self._store.recover()

        for checkpoint in orphans:
            status = checkpoint.get("status")

            if status in ("success", "dlq"):
                # Completed tasks whose checkpoint wasn't cleaned up —
                # clean up now, no action needed.
                task_id = checkpoint["task_id"]
                await self._store._sync_delete(task_id)  # sync ok here; we're single-threaded at startup
                logger.info(
                    "Cleaned up stale completed checkpoint",
                    extra={"task_id": task_id, "status": status},
                )
                continue

            # RUNNING or RETRYING at crash time — we don't know if the
            # handler completed, so we cannot safely replay without risking
            # a duplicate.  Send to DLQ for human review.
            logger.warning(
                "Orphaned in-flight task found — sending to DLQ for review",
                extra={
                    "task_id":   checkpoint.get("task_id"),
                    "topic":     checkpoint.get("topic"),
                    "offset":    checkpoint.get("offset"),
                    "status":    status,
                    "attempt":   checkpoint.get("attempt"),
                },
            )

            # Produce the full checkpoint as the DLQ payload
            try:
                await self._producer.produce(
                    topic = self._dlq_topic,
                    value = json.dumps({**checkpoint, "dlq_reason": "orphaned_at_startup"}),
                    key   = checkpoint.get("task_id"),
                )
            except Exception as exc:
                logger.critical(
                    "CRITICAL: Failed to DLQ orphaned task — manual recovery required",
                    extra={"checkpoint": checkpoint, "error": str(exc)},
                )

            # Remove the checkpoint so we don't re-process it on every restart
            if task_id := checkpoint.get("task_id"):
                await self._store._sync_delete(task_id)

    async def _shutdown(self) -> None:
        """
        Graceful shutdown: flush the producer and close the consumer.
        Called automatically when run() is cancelled.
        """
        logger.info("Flushing producer before shutdown…")
        await self._producer.flush()
        self._consumer.stop()
        logger.info("Orchestrator shutdown complete")