import json
import logging
import uuid

import redis as redis_lib

logger = logging.getLogger(__name__)


# ── Public signals ────────────────────────────────────────────────────────────


class StepFailed(Exception):
    """Raise from a step to abort the workflow and trigger compensation."""


def suspend():
    """Call from inside a step after publishing an async message.
    The engine pauses here; call orchestrator.resume() when the response arrives."""
    raise _AsyncSuspend()


# ── Workflow definition ───────────────────────────────────────────────────────


class Workflow:
    """Describes a durable sequence of steps.

    steps        list of callables  step(ctx) → dict | None
                 Return a dict to merge new values into ctx, or None for no update.
                 Raise StepFailed to abort.  Call suspend() to wait for an async event.

    compensation list of callables  comp(ctx) → None  (index-aligned with steps)
                 comp[i] undoes the effect of step[i].
                 Run in reverse order starting from the step before the one that failed.
                 May also call suspend() if the undo operation is itself async.

    on_complete  optional callable(ctx) invoked once all steps succeed.
    on_failed    optional callable(ctx) invoked once all compensations complete (or are absent).
    """

    def __init__(
        self, name, steps, compensation=None, on_complete=None, on_failed=None
    ):
        self.name = name
        self.steps = steps
        self.compensation = compensation or []
        self.on_complete = on_complete
        self.on_failed = on_failed


# ── Orchestrator engine ───────────────────────────────────────────────────────


class Orchestrator:
    _PREFIX = "wf:"
    _TERMINAL = {"completed", "failed"}

    # status name constants
    RUNNING = "running"
    WAITING = "waiting"  # async forward step in flight
    COMPENSATING = "compensating"  # running compensation steps synchronously
    WAITING_COMP = "waiting_comp"  # async compensation step in flight
    COMPLETED = "completed"
    FAILED = "failed"

    def __init__(self, redis_pool):
        self._pool = redis_pool

    def _r(self):
        return redis_lib.Redis(connection_pool=self._pool)

    # ── public API ────────────────────────────────────────────────────────────

    def start(self, workflow: Workflow, context: dict) -> str:
        # create a new workflow instance
        wf_id = str(uuid.uuid4())

        # inject the workflow id into the context
        context = dict(context)
        context["wf_id"] = wf_id

        # write the initial state to redis for recovery
        r = self._r()
        r.hset(
            f"{self._PREFIX}{wf_id}",
            mapping={
                "name": workflow.name,
                "step": "0",
                "status": self.RUNNING,
                "context": json.dumps(context),
            },
        )

        # start workflow execution
        self._execute_forward(workflow, wf_id, r)
        return wf_id

    def resume(self, workflow: Workflow, wf_id: str, result: dict = None):
        """Signal that an async forward step completed successfully.

        Advances the step pointer by one, merges the result into the stored
        context, and continues executing the next forward steps.

        Silently ignored if the workflow is not in 'waiting' state (guards
        against duplicate responses arriving after recovery re-published).
        """
        r = self._r()
        raw = r.hgetall(f"{self._PREFIX}{wf_id}")
        if not raw or raw.get("status") != self.WAITING:
            return
        ctx = json.loads(raw["context"])
        if result:
            ctx.update(result)
        next_step = int(raw["step"]) + 1
        r.hset(
            f"{self._PREFIX}{wf_id}",
            mapping={
                "step": str(next_step),
                "status": self.RUNNING,
                "context": json.dumps(ctx),
            },
        )
        self._execute_forward(workflow, wf_id, r)

    def fail(self, workflow: Workflow, wf_id: str, error: str = ""):
        """Signal that an async forward step failed.

        Stores the error, sets status to 'compensating', and begins running
        compensation steps in reverse from the step before the one that failed.

        Silently ignored if the workflow is not in 'waiting' status.
        """
        r = self._r()
        raw = r.hgetall(f"{self._PREFIX}{wf_id}")
        if not raw or raw.get("status") not in (self.WAITING, self.RUNNING):
            return
        step = int(raw.get("step", "0"))
        ctx = json.loads(raw.get("context", "{}"))
        ctx["_error"] = error
        r.hset(
            f"{self._PREFIX}{wf_id}",
            mapping={
                "status": self.COMPENSATING,
                "error": error,
                "context": json.dumps(ctx),
            },
        )
        self._execute_compensation(workflow, wf_id, r, ctx, failed_at=step)

    def resume_comp(self, workflow: Workflow, wf_id: str):
        """Signal that an async compensation step completed.

        Continues the compensation chain toward index 0.  When all compensations
        are done, calls workflow.on_failed and marks the instance 'failed'.

        Silently ignored if the workflow is not in 'waiting_comp' status.
        """
        r = self._r()
        raw = r.hgetall(f"{self._PREFIX}{wf_id}")
        if not raw or raw.get("status") != self.WAITING_COMP:
            return
        comp_step = int(raw.get("comp_step", "0"))
        ctx = json.loads(raw.get("context", "{}"))
        r.hset(f"{self._PREFIX}{wf_id}", "status", self.COMPENSATING)
        # comp_step just finished; continue from comp_step-1 downward
        # passing failed_at=comp_step runs range(comp_step-1, -1, -1)
        self._execute_compensation(workflow, wf_id, r, ctx, failed_at=comp_step)

    def get_status(self, wf_id: str) -> tuple:
        """Return (status, error, context) for a workflow instance."""
        r = self._r()
        raw = r.hgetall(f"{self._PREFIX}{wf_id}")
        if not raw:
            return "unknown", "", {}
        return (
            raw.get("status", "unknown"),
            raw.get("error", ""),
            json.loads(raw.get("context", "{}")),
        )

    def recover(self, workflow: Workflow):
        """Startup recovery: find all non-terminal instances of this workflow and resume them.

        Safe to call unconditionally at every startup.  Terminal instances
        ('completed', 'failed') are skipped.  For every other instance the engine
        re-executes from the stored position:
          - sync steps are re-called (must be idempotent)
          - async steps re-publish their messages (participants use idempotency keys)
          - compensation steps re-run in reverse from where they left off
        """
        r = self._r()
        cursor = 0
        recovered = 0
        while True:
            cursor, keys = r.scan(cursor, match=f"{self._PREFIX}*", count=100)
            for key in keys:
                raw = r.hgetall(key)
                if not raw or raw.get("name") != workflow.name:
                    continue
                if raw.get("status") in self._TERMINAL:
                    continue
                wf_id = key[len(self._PREFIX) :]
                logger.info(
                    "RECOVERY: wf=%s status=%s step=%s",
                    wf_id,
                    raw.get("status"),
                    raw.get("step"),
                )
                recovered += 1
                self._recover_one(workflow, wf_id, raw, r)
            if cursor == 0:
                break
        if recovered == 0:
            logger.info("RECOVERY: no incomplete '%s' workflows found", workflow.name)
        else:
            logger.info(
                "RECOVERY: resumed %d '%s' workflow(s)", recovered, workflow.name
            )

    # ── internal ──────────────────────────────────────────────────────────────

    def _recover_one(self, workflow, wf_id, raw, r):
        status = raw.get("status", "")
        ctx = json.loads(raw.get("context", "{}"))
        comp_step = int(raw.get("comp_step", "-1"))

        if status in (self.RUNNING, self.WAITING):
            # Re-run the current forward step.  If it was a sync step, it
            # re-executes cleanly.  If it was an async step it re-publishes
            # the message and suspends again.
            r.hset(f"{self._PREFIX}{wf_id}", "status", self.RUNNING)
            self._execute_forward(workflow, wf_id, r)

        elif status in (self.COMPENSATING, self.WAITING_COMP):
            r.hset(f"{self._PREFIX}{wf_id}", "status", self.COMPENSATING)
            if comp_step >= 0:
                # comp_step was stored before running that compensation step,
                # so we re-run it by passing failed_at = comp_step + 1
                # (which makes the loop start at comp_step).
                self._execute_compensation(
                    workflow, wf_id, r, ctx, failed_at=comp_step + 1
                )
            else:
                # compensation was triggered but no comp_step stored yet;
                # start fresh from the step that failed.
                step = int(raw.get("step", "0"))
                self._execute_compensation(workflow, wf_id, r, ctx, failed_at=step)

    def _execute_forward(self, workflow, wf_id, r):
        # get the current step index and context
        raw = r.hgetall(f"{self._PREFIX}{wf_id}")
        step_idx = int(raw.get("step", "0"))
        ctx = json.loads(raw.get("context", "{}"))

        # execute the steps in order
        for idx in range(step_idx, len(workflow.steps)):
            # write the step pointer BEFORE running the step so that a crash leaves a recoverable position
            r.hset(f"{self._PREFIX}{wf_id}", "step", str(idx))
            # execute the step
            try:
                result = workflow.steps[idx](ctx)
                if result:
                    ctx.update(result)
                    r.hset(f"{self._PREFIX}{wf_id}", "context", json.dumps(ctx))

            except _AsyncSuspend:
                # step published an async message and surrendered control;
                # resume() or fail() will drive the next transition
                r.hset(f"{self._PREFIX}{wf_id}", "status", self.WAITING)
                return

            except StepFailed as exc:
                ctx["_error"] = str(exc)
                r.hset(
                    f"{self._PREFIX}{wf_id}",
                    mapping={
                        "status": self.COMPENSATING,
                        "error": str(exc),
                        "context": json.dumps(ctx),
                    },
                )
                self._execute_compensation(workflow, wf_id, r, ctx, failed_at=idx)
                return

        # all forward steps completed successfully
        r.hset(f"{self._PREFIX}{wf_id}", "status", self.COMPLETED)
        if workflow.on_complete:
            try:
                workflow.on_complete(ctx)
            except Exception as exc:
                logger.error("on_complete failed for wf %s: %s", wf_id, exc)

    def _execute_compensation(self, workflow, wf_id, r, ctx, failed_at):
        """Run comp[failed_at-1], comp[failed_at-2], ..., comp[0] in reverse order.

        failed_at is the index of the step that failed (not yet compensated).
        Only steps with indices 0 .. failed_at-1 completed and need undoing.
        """
        for idx in range(failed_at - 1, -1, -1):
            if idx >= len(workflow.compensation):
                continue
            # write comp_step BEFORE running so recovery knows where to resume
            r.hset(f"{self._PREFIX}{wf_id}", "comp_step", str(idx))
            try:
                workflow.compensation[idx](ctx)

            except _AsyncSuspend:
                # compensation published an async undo message; pause here
                r.hset(f"{self._PREFIX}{wf_id}", "status", self.WAITING_COMP)
                return

            except Exception as exc:
                logger.error(
                    "Compensation step %d failed for wf %s: %s", idx, wf_id, exc
                )
                # continue attempting remaining compensations even if one errors

        # all compensations done (or skipped)
        r.hset(f"{self._PREFIX}{wf_id}", "status", self.FAILED)
        if workflow.on_failed:
            try:
                workflow.on_failed(ctx)
            except Exception as exc:
                logger.error("on_failed callback failed for wf %s: %s", wf_id, exc)


# ── Internal signal ───────────────────────────────────────────────────────────


class _AsyncSuspend(BaseException):
    """Not an error — raised by suspend() as a control-flow signal.
    Inherits from BaseException so it is not accidentally caught by bare 'except Exception'.
    """
