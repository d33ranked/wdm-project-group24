"""
WHY THIS EXISTS
---------------
If the orchestrator process dies while a task is RUNNING or RETRYING,
that task would normally be silently lost.  The checkpoint store writes
task state to disk *before* the handler is called and *after* it
completes, giving us three properties:

    1. No silent data loss — every in-flight task can be recovered.
    2. Exactly-once-on-success semantics — a task that already
       reached SUCCESS won't be replayed on restart (Kafka delivers
       at-least-once; the checkpoint provides the dedup layer).
    3. Crash visibility — on startup we can list all tasks that were
       RUNNING at crash time and either replay or alert on them.

DESIGN NOTES
------------
- Uses a local JSON file per task (named by task_id) inside a
  configurable directory.  This is intentionally simple — replace
  with Redis, Postgres, or DynamoDB for production multi-node use.
- File writes are atomic: we write to a .tmp file and rename, so
  a crash mid-write never corrupts the checkpoint.
- The store is intentionally synchronous (threadpool-wrapped) so
  callers can await it without blocking the event loop.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
from pathlib import Path
from typing import Dict, Iterator, Optional

from .task import Task, TaskStatus

logger = logging.getLogger(__name__)

# Sentinel used as a checkpoint value for tasks that have been cleaned up
_COMPLETED_STATUSES = {TaskStatus.SUCCESS, TaskStatus.DLQ}


class CheckpointStore:
    """
    Persists task state to individual JSON files.

    Usage::

        store = CheckpointStore("/var/lib/orchestrator/checkpoints")
        await store.save(task)         # Before calling the handler
        await store.save(task)         # After any status change
        await store.delete(task)       # After confirmed Kafka commit
        tasks = await store.recover()  # On startup — find orphaned tasks
    """

    def __init__(self, directory: str) -> None:
        self.directory = Path(directory)
        self.directory.mkdir(parents=True, exist_ok=True)
        logger.info("CheckpointStore initialised", extra={"dir": str(self.directory)})

    # ------------------------------------------------------------------
    # Public async API
    # ------------------------------------------------------------------

    async def save(self, task: Task) -> None:
        """
        Persist the current task state atomically.

        Atomic write (write-to-temp + rename) ensures a crash mid-write
        never leaves a partial or corrupt checkpoint file.
        """
        await asyncio.get_event_loop().run_in_executor(
            None, self._sync_save, task
        )

    async def delete(self, task: Task) -> None:
        """
        Remove the checkpoint once a task is fully settled and the
        Kafka offset has been committed.  Safe to call if the file
        doesn't exist (e.g. called twice due to at-least-once delivery).
        """
        await asyncio.get_event_loop().run_in_executor(
            None, self._sync_delete, task.task_id
        )

    async def recover(self) -> list[dict]:
        """
        Return the serialized state of every task that was not cleanly
        completed.  Call this once at startup before consuming messages.

        The returned dicts are the raw to_checkpoint() payloads — callers
        decide what to do with each one (replay, alert, DLQ, etc.).
        """
        return await asyncio.get_event_loop().run_in_executor(
            None, self._sync_recover
        )

    # ------------------------------------------------------------------
    # Synchronous internals (run in a thread pool)
    # ------------------------------------------------------------------

    def _path(self, task_id: str) -> Path:
        return self.directory / f"{task_id}.json"

    def _sync_save(self, task: Task) -> None:
        data = task.to_checkpoint()
        target = self._path(task.task_id)
        tmp    = target.with_suffix(".tmp")

        try:
            # Write to a sibling .tmp file first, then atomically rename
            tmp.write_text(json.dumps(data, indent=4), encoding="utf-8")
            os.replace(tmp, target)  # atomic on POSIX; best-effort on Windows
        except OSError as exc:
            # A checkpoint failure must be loud — we'd rather crash than
            # silently proceed without durability.
            logger.critical(
                "Failed to write checkpoint — aborting",
                extra={"task_id": task.task_id, "path": str(target), "error": str(exc)},
                exc_info=True,
            )
            raise

        logger.debug(
            "Checkpoint saved",
            extra={"task_id": task.task_id, "status": task.status.value},
        )

    def _sync_delete(self, task_id: str) -> None:
        path = self._path(task_id)
        try:
            path.unlink(missing_ok=True)
            logger.debug("Checkpoint deleted", extra={"task_id": task_id})
        except OSError as exc:
            # Log but don't raise — a stale checkpoint is recoverable;
            # crashing here would interrupt the happy path unnecessarily.
            logger.warning(
                "Could not delete checkpoint",
                extra={"task_id": task_id, "error": str(exc)},
            )

    def _sync_recover(self) -> list[dict]:
        """Read all checkpoint files and return their contents."""
        recovered = []

        for path in self.directory.glob("*.json"):
            try:
                data = json.loads(path.read_text(encoding="utf-8"))

                # Tasks in a terminal state that somehow weren't cleaned up
                # are included so the orchestrator can decide — don't discard
                # silently.
                recovered.append(data)

            except (json.JSONDecodeError, ValueError, OSError) as exc:
                # A corrupt checkpoint file should alert, not silently skip.
                logger.error(
                    "Corrupt checkpoint file — manual inspection required",
                    extra={"path": str(path), "error": str(exc)},
                )

        if recovered:
            logger.warning(
                "Recovered in-flight tasks from checkpoint",
                extra={"count": len(recovered)},
            )
        else:
            logger.info("No in-flight tasks found at startup")

        return recovered