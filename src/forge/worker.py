"""Worker loop: claim one job, run its stage handler with resumable steps."""
from __future__ import annotations

import logging
from typing import Any, Callable, Mapping, Optional, Sequence

from .jobs import Job, JobStore, LeaseLost
from .redact import RedactingFilter

log = logging.getLogger("forge.worker")
log.addFilter(RedactingFilter())


class PermanentError(Exception):
    """Raise from a handler when retrying cannot help (bad input, etc.)."""


class StepContext:
    """Gives a handler resumable steps. A step that already has a checkpoint is
    NOT executed again; its stored result is returned instead."""

    def __init__(self, store: JobStore, job: Job, worker_id: str, lease_seconds: float):
        self.store, self.job, self.worker_id, self.lease_seconds = store, job, worker_id, lease_seconds
        self._done = store.checkpoints(job.id)

    @property
    def payload(self) -> dict:
        return self.job.payload

    def step(self, name: str, fn: Callable[[], Any]) -> Any:
        if name in self._done:
            log.info("job %s: skipping completed step %s", self.job.id, name)
            return self._done[name]
        result = fn()  # result must be JSON-serialisable
        self.store.checkpoint(self.job, self.worker_id, name, result, self.lease_seconds)
        self._done[name] = result
        return result

    def heartbeat(self) -> None:
        self.store.heartbeat(self.job, self.worker_id, self.lease_seconds)

    def external(
        self, op_name: str, inputs: Any,
        execute: Callable[[str], Any],
        probe: Optional[Callable[[str], Any]] = None,
    ) -> Any:
        """Run an external side effect (upload, download, TTS call, export) safely.

        A checkpoint alone is NOT enough: the process can die after the effect succeeded and
        before the checkpoint is written. This method keeps a durable ledger entry keyed by a
        deterministic operation_id (documentary + stage + op_name + hash(inputs)):

          1. ledger says 'committed'            -> return the recorded output; execute NOT called.
          2. ledger says 'started' by an earlier attempt (executions > 1):
               a. `probe(operation_id)` finds the effect already in the external system and
                  returns its output (content-hash verified by the caller) -> commit it, skip execute;
               b. no probe / probe returns None -> call execute(operation_id) again. This is
                  AT-LEAST-ONCE execution: `execute` MUST be idempotent under operation_id
                  (use it as the object key / idempotency-key header, write the same bytes).
          3. first time -> execute(operation_id), then commit.

        Guarantee: exactly-once EFFECT only if the external operation is idempotent (or the probe
        is reliable). Exactly-once EXECUTION is not guaranteed and not claimed.
        `inputs` and the returned output must be JSON-serialisable (store paths/hashes, not bytes).
        """
        rec = self.store.begin_operation(self.job, self.worker_id, op_name, inputs)
        if rec.status == "committed":
            log.info("job %s: operation %s already committed; skipping", self.job.id, rec.operation_id)
            return rec.output
        output = None
        if rec.executions > 1 and probe is not None:
            output = probe(rec.operation_id)
            if output is not None:
                log.info("job %s: operation %s effect found by probe; not re-executing", self.job.id, rec.operation_id)
        if output is None:
            output = execute(rec.operation_id)
        self.store.commit_operation(self.job, self.worker_id, rec.operation_id, output)
        return output


Handler = Callable[[StepContext], Any]


def run_one(
    store: JobStore, worker_id: str, capabilities: Sequence[str],
    handlers: Mapping[str, Handler], lease_seconds: float = 300,
) -> Optional[str]:
    """Run at most one job. Returns the outcome, or None if nothing was claimable.

    Outcomes: succeeded, retryable_failed, permanently_failed, dead_letter, lease_lost.
    A BaseException that is not Exception (e.g. a killed process) deliberately
    propagates WITHOUT recording a failure - recovery then relies on lease expiry.
    """
    job = store.claim(worker_id, capabilities, lease_seconds)
    if job is None:
        return None
    handler = handlers.get(job.stage)
    try:
        if handler is None:
            raise PermanentError(f"no handler registered for stage {job.stage!r}")
        result = handler(StepContext(store, job, worker_id, lease_seconds))
        store.complete(job, worker_id, result)
        return "succeeded"
    except LeaseLost:
        log.warning("job %s: lease lost; abandoning without writing", job.id)
        return "lease_lost"
    except PermanentError as exc:
        return _safe_fail(store, job, worker_id, str(exc), retryable=False)
    except Exception as exc:  # bounded by max_attempts
        return _safe_fail(store, job, worker_id, f"{type(exc).__name__}: {exc}", retryable=True)


def _safe_fail(store: JobStore, job: Job, worker_id: str, error: str, retryable: bool) -> str:
    try:
        return store.fail(job, worker_id, error, retryable)
    except LeaseLost:
        return "lease_lost"
