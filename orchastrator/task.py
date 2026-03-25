"""
These are plain value objects: they carry state and metadata but contain
no business logic. Keeping them separate ensures the orchestrator layer
never bleeds into service code.
"""

from __future__ import annotations

import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import Any, Optional


class TaskStatus(Enum):
    """Lifecycle states of a single task execution."""
    PENDING   = "pending"    # Received, not yet dispatched to a handler
    RUNNING   = "running"    # Handler is currently executing
    SUCCESS   = "success"    # Handler returned without raising
    FAILURE   = "failure"    # Handler raised; retries exhausted
    RETRYING  = "retrying"   # Handler raised; a retry is scheduled
    DLQ       = "dlq"        # Moved to the dead-letter queue


@dataclass
class Task:
    """
    Represents a unit of work derived from a single Kafka message.

    Fields that are *identity*  (topic, partition, offset, key) come from
    Kafka and are never modified after construction.
    Fields that are *state*     (status, attempt, timestamps) are mutated
    by the orchestrator as the task progresses.
    Fields that are *payload*   (value) are passed verbatim to the handler
    and are likewise never modified.
    """

    # Kafka provenance — used to commit offsets and deduplicate on restart
    topic:      str
    partition:  int
    offset:     int
    key:        Optional[bytes]

    # The deserialized message body forwarded to the registered handler
    value:      Any

    # Unique ID for logging/tracing across services
    task_id:    str = field(default_factory=lambda: str(uuid.uuid4()))

    # Retry bookkeeping
    max_retries: int        = 3
    attempt:     int        = 0          # 0 = first attempt, 1 = first retry, …

    # Mutable lifecycle fields
    status:      TaskStatus = TaskStatus.PENDING
    error:       Optional[str] = None    # Last exception message, if any

    # Timestamps (UTC)
    created_at:  datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    updated_at:  datetime = field(default_factory=lambda: datetime.now(timezone.utc))

    def touch(self) -> None:
        """Update the modified timestamp. Call after every status change."""
        self.updated_at = datetime.now(timezone.utc)

    @property
    def exhausted(self) -> bool:
        """True when no further retry attempts are allowed."""
        return self.attempt >= self.max_retries

    def to_checkpoint(self) -> dict:
        """
        Serialize to a dict suitable for persistence in CheckpointStore.
        Only includes fields needed to reconstruct state after a crash.
        """
        return {
            "task_id":    self.task_id,
            "topic":      self.topic,
            "partition":  self.partition,
            "offset":     self.offset,
            "status":     self.status.value,
            "attempt":    self.attempt,
            "error":      self.error,
            "created_at": self.created_at.isoformat(),
            "updated_at": self.updated_at.isoformat(),
        }


@dataclass
class TaskResult:
    """
    Returned by a handler function to tell the orchestrator what to do next.

    The handler should never import Kafka or orchestrator internals —
    it just returns a TaskResult and lets the library handle routing.
    """

    success:        bool
    output_topic:   Optional[str] = None   # Publish result here (None = no publish)
    output_value:   Any            = None  # Payload to publish
    error_message:  Optional[str] = None   # Human-readable failure reason

    @classmethod
    def ok(cls, output_topic: str, output_value: Any) -> "TaskResult":
        """Convenience constructor for successful results."""
        return cls(success=True, output_topic=output_topic, output_value=output_value)

    @classmethod
    def fail(cls, error_message: str) -> "TaskResult":
        """Convenience constructor for failed results."""
        return cls(success=False, error_message=error_message)