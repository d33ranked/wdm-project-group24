"""
Exponential backoff with full jitter is used by default. Full jitter
("random between 0 and cap") distributes retry storms across time,
which prevents multiple failing workers from hammering a downstream
service simultaneously (the thundering-herd problem).

Reference: https://aws.amazon.com/blogs/architecture/exponential-backoff-and-jitter/
"""

from __future__ import annotations

import asyncio
import logging
import random
from dataclasses import dataclass
from typing import Callable, Awaitable

from .task import Task, TaskStatus

logger = logging.getLogger(__name__)


@dataclass
class RetryPolicy:
    """
    Configuration for how retries behave.

    All durations are in seconds.  Defaults give a safe starting point
    for most inter-service calls, but callers should tune these to the
    SLA of their downstream dependency.
    """

    base_delay:  float = 1.0    # Initial backoff before first retry
    max_delay:   float = 60.0   # Hard ceiling; prevents unbounded waits
    multiplier:  float = 2.0    # How aggressively the delay grows
    jitter:      bool  = True   # Randomise delays to prevent thundering herd

    def delay_for(self, attempt: int) -> float:
        """
        Compute the sleep duration before the given attempt number.

        attempt=1 → first retry, attempt=2 → second retry, etc.
        With full jitter the actual delay is uniform in [0, computed_cap].
        """
        # Exponential growth capped at max_delay
        cap = min(self.max_delay, self.base_delay * (self.multiplier ** (attempt - 1)))

        if self.jitter:
            # Full jitter: uniform spread prevents all workers retrying at once
            return random.uniform(0, cap)

        return cap


class RetryHandler:
    """
    Executes an async callable with automatic retries on exception.

    Responsibilities:
        - Track attempt count on the Task object
        - Sleep between retries using the configured RetryPolicy
        - Update TaskStatus (RETRYING → RUNNING → SUCCESS/FAILURE)
        - Never swallow exceptions silently; always log before retrying

    Intentionally knows nothing about Kafka, topics, or business rules.
    """

    def __init__(self, policy: RetryPolicy | None = None) -> None:
        self.policy = policy or RetryPolicy()

    async def run(
        self,
        task: Task,
        fn: Callable[[Task], Awaitable],
    ):
        """
        Run *fn* with the task, retrying on any Exception.

        Returns whatever *fn* returns on success.
        Raises the last exception if all attempts are exhausted.

        The caller (Orchestrator) is responsible for routing the task
        to the DLQ after this method raises.
        """

        while True:
            task.attempt += 1
            task.status = TaskStatus.RUNNING
            task.touch()

            try:
                logger.info(
                    "Dispatching task",
                    extra={
                        "task_id":   task.task_id,
                        "topic":     task.topic,
                        "attempt":   task.attempt,
                        "max":       task.max_retries,
                        "partition": task.partition,
                        "offset":    task.offset,
                    },
                )

                result = await fn(task)

                task.status = TaskStatus.SUCCESS
                task.touch()
                logger.info(
                    "Task succeeded",
                    extra={"task_id": task.task_id, "attempt": task.attempt},
                )
                return result

            except Exception as exc:
                task.error = str(exc)
                task.touch()

                if task.exhausted:
                    # All retries used up — propagate so the orchestrator
                    # can send the task to the dead-letter queue.
                    task.status = TaskStatus.FAILURE
                    logger.error(
                        "Task failed permanently",
                        extra={
                            "task_id": task.task_id,
                            "attempt": task.attempt,
                            "error":   str(exc),
                        },
                        exc_info=True,
                    )
                    raise

                # Still have attempts left — schedule a retry
                task.status = TaskStatus.RETRYING
                delay = self.policy.delay_for(task.attempt)
                logger.warning(
                    "Task failed, will retry",
                    extra={
                        "task_id":    task.task_id,
                        "attempt":    task.attempt,
                        "remaining":  task.max_retries - task.attempt,
                        "delay_s":    round(delay, 2),
                        "error":      str(exc),
                    },
                )
                await asyncio.sleep(delay)