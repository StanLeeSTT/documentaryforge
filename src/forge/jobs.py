"""Local reference implementation of the DocumentaryForge job engine (SQLite).

This mirrors supabase/migrations/0001_foundation.sql so recovery logic can be proven
without any external service. Differences between the two are listed in docs/PARITY.md.
The PostgreSQL version is UNTESTED until it has been executed against a real PostgreSQL.

Rules:
  * claim is atomic (BEGIN IMMEDIATE); two workers never hold one job;
  * every claim increments `attempts`; that number is the FENCING TOKEN and only ever grows;
  * heartbeat/checkpoint/complete/fail/external-operation writes require matching worker AND
    attempt, so a worker whose lease was taken over gets LeaseLost and cannot write;
  * an expired lease makes a job claimable again, unless attempts are exhausted -> dead_letter;
  * job identity is scoped: UNIQUE (documentary_id, stage, idempotency_key);
  * re-enqueue with the same identity but different inputs raises IdempotencyConflict;
  * dependencies are a DAG: cycles, self-dependencies and cross-documentary edges are rejected;
  * a job whose (transitive) dependency is permanently_failed / dead_letter / cancelled is moved
    to needs_review with blocked_by set, so nothing is silently stuck (see blocked_jobs()).
"""
from __future__ import annotations

import json
import sqlite3
import time
import uuid
from collections import defaultdict
from contextlib import contextmanager
from dataclasses import dataclass
from typing import Any, Callable, Iterator, Optional, Sequence

from .idempotency import canonical_json, hash_obj, make_operation_id, sha256_hex
from .redact import redact

SCHEMA_VERSION = 2

ACTIVE_STATES = ("claimed", "running", "checkpointed")
ALL_STATES = (
    "queued", "claimed", "running", "checkpointed", "succeeded",
    "retryable_failed", "permanently_failed", "dead_letter",
    "needs_review", "cancelled",
)
BAD_DEPENDENCY_STATES = ("permanently_failed", "dead_letter", "cancelled")
REQUEUEABLE_STATES = ("permanently_failed", "dead_letter", "cancelled", "needs_review")
DEPENDENCY_EDITABLE_STATES = ("queued", "retryable_failed", "needs_review")
BASE_BACKOFF_SECONDS = 30
MAX_BACKOFF_SECONDS = 3600
MAX_ERROR_CHARS = 2000
DEFAULT_LEASE_SECONDS = 300
DEFAULT_MAX_ATTEMPTS = 3
DEFAULT_PRIORITY = 100

Clock = Callable[[], float]


class LeaseLost(Exception):
    """This worker no longer owns the job (lease taken over, job finished or cancelled)."""


class DependencyError(ValueError):
    """Invalid dependency (unknown job, cross-documentary, job already started)."""


class DependencyCycle(DependencyError):
    """The dependency would create a cycle (including a job depending on itself)."""


class IdempotencyConflict(Exception):
    """Same (documentary, stage, idempotency_key) re-enqueued with different inputs."""


class OperationConflict(Exception):
    """An external operation was committed twice with different outputs."""


class UnknownDocumentary(ValueError):
    pass


def _in(values: Sequence[str]) -> str:
    return "(" + ",".join("'" + v + "'" for v in values) + ")"


SCHEMA = f"""
CREATE TABLE IF NOT EXISTS documentaries (
    id TEXT PRIMARY KEY,
    title TEXT NOT NULL DEFAULT '',
    brief TEXT NOT NULL DEFAULT '{{}}',
    status TEXT NOT NULL DEFAULT 'draft',
    created_at REAL NOT NULL
);
CREATE TABLE IF NOT EXISTS jobs (
    seq INTEGER PRIMARY KEY AUTOINCREMENT,
    id TEXT NOT NULL UNIQUE,
    documentary_id TEXT NOT NULL,
    stage TEXT NOT NULL,
    required_capability TEXT NOT NULL DEFAULT 'cpu',
    payload TEXT NOT NULL DEFAULT '{{}}',
    idempotency_key TEXT NOT NULL,
    input_hash TEXT NOT NULL,
    priority INTEGER NOT NULL DEFAULT {DEFAULT_PRIORITY},
    status TEXT NOT NULL DEFAULT 'queued' CHECK (status IN {_in(ALL_STATES)}),
    attempts INTEGER NOT NULL DEFAULT 0,
    max_attempts INTEGER NOT NULL DEFAULT {DEFAULT_MAX_ATTEMPTS} CHECK (max_attempts >= 1),
    run_after REAL NOT NULL,
    lease_owner TEXT,
    lease_expires_at REAL,
    last_heartbeat_at REAL,
    last_error TEXT,
    blocked_by TEXT,
    result TEXT,
    created_at REAL NOT NULL,
    updated_at REAL NOT NULL,
    UNIQUE (documentary_id, stage, idempotency_key)
);
CREATE TABLE IF NOT EXISTS job_dependencies (
    job_id TEXT NOT NULL,
    depends_on TEXT NOT NULL,
    PRIMARY KEY (job_id, depends_on),
    CHECK (job_id <> depends_on)
);
CREATE TABLE IF NOT EXISTS job_attempts (
    job_id TEXT NOT NULL,
    attempt INTEGER NOT NULL,
    worker_id TEXT NOT NULL,
    started_at REAL NOT NULL,
    ended_at REAL,
    outcome TEXT,
    error TEXT,
    PRIMARY KEY (job_id, attempt)
);
CREATE TABLE IF NOT EXISTS job_checkpoints (
    job_id TEXT NOT NULL,
    step TEXT NOT NULL,
    data TEXT NOT NULL,
    attempt INTEGER NOT NULL,
    created_at REAL NOT NULL,
    PRIMARY KEY (job_id, step)
);
CREATE TABLE IF NOT EXISTS external_operations (
    operation_id TEXT PRIMARY KEY,
    documentary_id TEXT NOT NULL,
    stage TEXT NOT NULL,
    op_name TEXT NOT NULL,
    input_hash TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'started' CHECK (status IN ('started','committed')),
    output TEXT,
    output_hash TEXT,
    executions INTEGER NOT NULL DEFAULT 1,
    job_id TEXT,
    last_worker TEXT,
    last_attempt INTEGER,
    created_at REAL NOT NULL,
    updated_at REAL NOT NULL,
    committed_at REAL
);
"""


@dataclass(frozen=True)
class Job:
    id: str
    documentary_id: str
    stage: str
    required_capability: str
    payload: dict
    idempotency_key: str
    priority: int
    status: str
    attempts: int
    max_attempts: int
    run_after: float
    lease_owner: Optional[str]
    lease_expires_at: Optional[float]
    last_error: Optional[str]
    result: Any
    input_hash: str = ""
    blocked_by: Optional[str] = None


@dataclass(frozen=True)
class OperationRecord:
    operation_id: str
    op_name: str
    input_hash: str
    status: str            # 'started' | 'committed'
    output: Any
    output_hash: Optional[str]
    executions: int        # how many times execution has been BEGUN (>1 => an earlier attempt may have had effects)


@dataclass(frozen=True)
class BlockedJob:
    job_id: str
    documentary_id: str
    stage: str
    reason: str            # 'dependency_failed' | 'review_required'
    blocked_by: Optional[str]
    detail: Optional[str]
    since: float


def _job(row: sqlite3.Row) -> Job:
    return Job(
        id=row["id"], documentary_id=row["documentary_id"], stage=row["stage"],
        required_capability=row["required_capability"], payload=json.loads(row["payload"]),
        idempotency_key=row["idempotency_key"], priority=row["priority"], status=row["status"],
        attempts=row["attempts"], max_attempts=row["max_attempts"], run_after=row["run_after"],
        lease_owner=row["lease_owner"], lease_expires_at=row["lease_expires_at"],
        last_error=row["last_error"],
        result=json.loads(row["result"]) if row["result"] is not None else None,
        input_hash=row["input_hash"], blocked_by=row["blocked_by"],
    )


def _op(row: sqlite3.Row) -> OperationRecord:
    return OperationRecord(
        operation_id=row["operation_id"], op_name=row["op_name"], input_hash=row["input_hash"],
        status=row["status"], output=json.loads(row["output"]) if row["output"] is not None else None,
        output_hash=row["output_hash"], executions=row["executions"],
    )


class JobStore:
    """One JobStore = one connection = one worker's view of the queue."""

    def __init__(self, path: str = ":memory:", clock: Clock = time.time, require_documentary: bool = True):
        """require_documentary=True mirrors PostgreSQL's foreign key to public.documentaries."""
        self.clock = clock
        self.require_documentary = require_documentary
        self.conn = sqlite3.connect(path, timeout=30, isolation_level=None)
        self.conn.row_factory = sqlite3.Row
        if path != ":memory:":
            self.conn.execute("PRAGMA journal_mode=WAL")
        self._init_schema()

    def _init_schema(self) -> None:
        version = self.conn.execute("PRAGMA user_version").fetchone()[0]
        has_jobs = self.conn.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name='jobs'").fetchone()
        if version not in (0, SCHEMA_VERSION) or (version == 0 and has_jobs):
            raise RuntimeError(
                f"incompatible job database (schema version {version}, expected {SCHEMA_VERSION}); "
                "this is a throwaway reference database - delete it and start again")
        self.conn.executescript(SCHEMA)
        self.conn.execute(f"PRAGMA user_version = {SCHEMA_VERSION}")

    def close(self) -> None:
        self.conn.close()

    @contextmanager
    def _tx(self) -> Iterator[sqlite3.Connection]:
        self.conn.execute("BEGIN IMMEDIATE")
        try:
            yield self.conn
        except BaseException:
            self.conn.execute("ROLLBACK")
            raise
        else:
            self.conn.execute("COMMIT")

    # ------------------------------------------------------------ documentaries
    def register_documentary(self, documentary_id: str, title: str = "", brief: Optional[dict] = None) -> None:
        with self._tx() as c:
            c.execute("INSERT OR IGNORE INTO documentaries (id, title, brief, created_at) VALUES (?,?,?,?)",
                      (documentary_id, title, canonical_json(brief or {}), self.clock()))

    # ------------------------------------------------------------------ enqueue
    def enqueue(
        self, *, documentary_id: str, stage: str, idempotency_key: str,
        payload: Optional[dict] = None, capability: str = "cpu", priority: int = DEFAULT_PRIORITY,
        max_attempts: int = DEFAULT_MAX_ATTEMPTS, depends_on: Sequence[str] = (),
    ) -> Job:
        """Create a job, or return the existing one with the SAME (documentary, stage, key).

        Same identity + same inputs      -> the existing job (safe to repeat).
        Same identity + different inputs -> IdempotencyConflict (use a new key, e.g. add a version).
        Different documentary or stage   -> always a different job, even with an equal key.
        `depends_on` only applies when a job is created; it is ignored for an existing job.
        """
        if max_attempts < 1:
            raise ValueError("max_attempts must be >= 1")
        payload = payload if payload is not None else {}
        input_hash = hash_obj({"payload": payload, "capability": capability})  # TypeError if not JSON
        with self._tx() as c:
            if self.require_documentary and not c.execute(
                    "SELECT 1 FROM documentaries WHERE id=?", (documentary_id,)).fetchone():
                raise UnknownDocumentary("documentary is not registered")
            existing = c.execute(
                "SELECT * FROM jobs WHERE documentary_id=? AND stage=? AND idempotency_key=?",
                (documentary_id, stage, idempotency_key)).fetchone()
            if existing:
                if existing["input_hash"] != input_hash:
                    raise IdempotencyConflict(
                        "a job with this documentary/stage/idempotency_key already exists with different inputs")
                return _job(existing)
            for dep in depends_on:
                drow = c.execute("SELECT documentary_id FROM jobs WHERE id=?", (dep,)).fetchone()
                if drow is None:
                    raise DependencyError("unknown dependency job")
                if drow["documentary_id"] != documentary_id:
                    raise DependencyError("a job cannot depend on a job from another documentary")
            now, jid = self.clock(), str(uuid.uuid4())
            c.execute(
                "INSERT INTO jobs (id, documentary_id, stage, required_capability, payload,"
                " idempotency_key, input_hash, priority, max_attempts, run_after, created_at, updated_at)"
                " VALUES (?,?,?,?,?,?,?,?,?,?,?,?)",
                (jid, documentary_id, stage, capability, canonical_json(payload),
                 idempotency_key, input_hash, priority, max_attempts, now, now, now))
            for dep in depends_on:
                c.execute("INSERT OR IGNORE INTO job_dependencies (job_id, depends_on) VALUES (?,?)", (jid, dep))
            self._reconcile(c, now)  # a dependency may already be failed
            return _job(c.execute("SELECT * FROM jobs WHERE id=?", (jid,)).fetchone())

    # ------------------------------------------------------------- dependencies
    def add_dependency(self, job_id: str, depends_on_id: str) -> None:
        """Add an edge job_id -> depends_on_id. Rejects self-edges, cycles, cross-documentary
        edges, unknown jobs and jobs that have already started."""
        if job_id == depends_on_id:
            raise DependencyCycle("a job cannot depend on itself")
        with self._tx() as c:
            jrow = c.execute("SELECT documentary_id, status FROM jobs WHERE id=?", (job_id,)).fetchone()
            drow = c.execute("SELECT documentary_id FROM jobs WHERE id=?", (depends_on_id,)).fetchone()
            if jrow is None or drow is None:
                raise DependencyError("unknown job")
            if c.execute("SELECT 1 FROM job_dependencies WHERE job_id=? AND depends_on=?",
                         (job_id, depends_on_id)).fetchone():
                return  # already present
            if jrow["documentary_id"] != drow["documentary_id"]:
                raise DependencyError("a job cannot depend on a job from another documentary")
            if jrow["status"] not in DEPENDENCY_EDITABLE_STATES:
                raise DependencyError("dependencies can only be added to jobs that have not started")
            if job_id in self._reachable(c, depends_on_id):
                raise DependencyCycle("dependency would create a cycle")
            c.execute("INSERT INTO job_dependencies (job_id, depends_on) VALUES (?,?)", (job_id, depends_on_id))
            self._reconcile(c, self.clock())

    @staticmethod
    def _reachable(c: sqlite3.Connection, start: str) -> set[str]:
        """`start` plus everything it transitively depends on."""
        seen: set[str] = set()
        stack = [start]
        while stack:
            node = stack.pop()
            if node in seen:
                continue
            seen.add(node)
            for r in c.execute("SELECT depends_on FROM job_dependencies WHERE job_id=?", (node,)):
                stack.append(r["depends_on"])
        return seen

    def _reconcile(self, c: sqlite3.Connection, now: float) -> int:
        """Move jobs with a failed/cancelled ancestor to needs_review (blocked_by = root cause),
        and release them again when the cause is gone. Mirrors reconcile_blocked_jobs() in SQL.
        Succeeded jobs are not traversed: their ancestors are irrelevant."""
        rows = c.execute("SELECT id, status, blocked_by FROM jobs").fetchall()
        status = {r["id"]: r["status"] for r in rows}
        deps: dict[str, list[str]] = defaultdict(list)
        for r in c.execute("SELECT job_id, depends_on FROM job_dependencies"):
            deps[r["job_id"]].append(r["depends_on"])

        def bad_root(jid: str) -> Optional[str]:
            seen: set[str] = set()
            stack = list(deps[jid])
            roots: list[str] = []
            while stack:
                d = stack.pop()
                if d in seen:
                    continue
                seen.add(d)
                st = status.get(d)
                if st == "succeeded":
                    continue
                if st in BAD_DEPENDENCY_STATES:
                    roots.append(d)
                stack.extend(deps[d])
            return min(roots) if roots else None  # deterministic: smallest id (C collation in SQL)

        changed = 0
        for r in rows:
            jid, st = r["id"], r["status"]
            if st in ("queued", "retryable_failed"):
                root = bad_root(jid)
                if root is not None:
                    c.execute("UPDATE jobs SET status='needs_review', blocked_by=?, last_error=?, updated_at=? WHERE id=?",
                              (root, f"blocked: dependency {root} is {status[root]}", now, jid))
                    changed += 1
            elif st == "needs_review" and r["blocked_by"] is not None:
                root = bad_root(jid)
                if root is None:
                    c.execute("UPDATE jobs SET status='queued', blocked_by=NULL, updated_at=? WHERE id=?", (now, jid))
                    changed += 1
                elif root != r["blocked_by"]:
                    c.execute("UPDATE jobs SET blocked_by=?, last_error=?, updated_at=? WHERE id=?",
                              (root, f"blocked: dependency {root} is {status[root]}", now, jid))
                    changed += 1
        return changed

    def blocked_jobs(self) -> list[BlockedJob]:
        """Everything that needs a human: jobs blocked by a failed dependency, and jobs that
        asked for review. Nothing in this list will move on its own."""
        rows = self.conn.execute(
            "SELECT * FROM jobs WHERE status='needs_review' ORDER BY updated_at, seq").fetchall()
        return [BlockedJob(
            job_id=r["id"], documentary_id=r["documentary_id"], stage=r["stage"],
            reason="dependency_failed" if r["blocked_by"] else "review_required",
            blocked_by=r["blocked_by"], detail=r["last_error"], since=r["updated_at"]) for r in rows]

    # ----------------------------------------------------------- operator actions
    def cancel_job(self, job_id: str, reason: Optional[str] = None) -> bool:
        """Cancel a job that has not succeeded. A worker still holding it gets LeaseLost on its
        next write. Dependents become needs_review (blocked_by = this job). Returns False if no-op."""
        with self._tx() as c:
            now = self.clock()
            row = c.execute("SELECT status FROM jobs WHERE id=?", (job_id,)).fetchone()
            if row is None:
                raise KeyError(job_id)
            if row["status"] in ("succeeded", "cancelled"):
                return False
            err = redact(reason)[:MAX_ERROR_CHARS] if reason else None
            c.execute("UPDATE jobs SET status='cancelled', lease_owner=NULL, lease_expires_at=NULL,"
                      " blocked_by=NULL, last_error=COALESCE(?, last_error), updated_at=? WHERE id=?",
                      (err, now, job_id))
            c.execute("UPDATE job_attempts SET ended_at=?, outcome='cancelled' WHERE job_id=? AND ended_at IS NULL",
                      (now, job_id))
            self._reconcile(c, now)
            return True

    def requeue_job(self, job_id: str, extra_attempts: int = DEFAULT_MAX_ATTEMPTS) -> Job:
        """Operator retry of a failed/cancelled/needs_review job. `attempts` is NEVER reset (it is
        the fencing token); instead max_attempts = attempts + extra_attempts."""
        if extra_attempts < 1:
            raise ValueError("extra_attempts must be >= 1")
        with self._tx() as c:
            now = self.clock()
            row = c.execute("SELECT status FROM jobs WHERE id=?", (job_id,)).fetchone()
            if row is None:
                raise KeyError(job_id)
            if row["status"] not in REQUEUEABLE_STATES:
                raise ValueError(f"cannot requeue a job in state {row['status']}")
            c.execute("UPDATE jobs SET status='queued', max_attempts=attempts+?, run_after=?, blocked_by=NULL,"
                      " lease_owner=NULL, lease_expires_at=NULL, updated_at=? WHERE id=?",
                      (extra_attempts, now, now, job_id))
            self._reconcile(c, now)
            return _job(c.execute("SELECT * FROM jobs WHERE id=?", (job_id,)).fetchone())

    # -------------------------------------------------------------------- claim
    def claim(self, worker_id: str, capabilities: Sequence[str], lease_seconds: float = DEFAULT_LEASE_SECONDS) -> Optional[Job]:
        if lease_seconds <= 0:
            raise ValueError("lease_seconds must be > 0")
        if not capabilities:
            return None
        ph = ",".join("?" for _ in capabilities)
        with self._tx() as c:
            now = self.clock()
            exhausted = c.execute(
                "SELECT id FROM jobs WHERE status IN ('claimed','running','checkpointed')"
                " AND lease_expires_at < ? AND attempts >= max_attempts", (now,)).fetchall()
            for r in exhausted:
                c.execute(
                    "UPDATE jobs SET status='dead_letter', lease_owner=NULL, lease_expires_at=NULL,"
                    " last_error=COALESCE(last_error,'') || ' [lease expired on final attempt]',"
                    " updated_at=? WHERE id=?", (now, r["id"]))
                c.execute("UPDATE job_attempts SET ended_at=?, outcome='lease_expired'"
                          " WHERE job_id=? AND ended_at IS NULL", (now, r["id"]))
            if exhausted:
                self._reconcile(c, now)
            row = c.execute(
                f"""SELECT * FROM jobs p
                    WHERE required_capability IN ({ph})
                      AND ( (status IN ('queued','retryable_failed') AND run_after <= ?)
                         OR (status IN ('claimed','running','checkpointed')
                             AND lease_expires_at < ? AND attempts < max_attempts) )
                      AND NOT EXISTS (SELECT 1 FROM job_dependencies d
                                      JOIN jobs dj ON dj.id = d.depends_on
                                      WHERE d.job_id = p.id AND dj.status <> 'succeeded')
                    ORDER BY priority, seq LIMIT 1""",
                (*capabilities, now, now)).fetchone()
            if row is None:
                return None
            c.execute("UPDATE job_attempts SET ended_at=?, outcome='lease_expired'"
                      " WHERE job_id=? AND ended_at IS NULL", (now, row["id"]))
            attempt = row["attempts"] + 1
            c.execute(
                "UPDATE jobs SET attempts=?, status='claimed', lease_owner=?, lease_expires_at=?,"
                " last_heartbeat_at=?, updated_at=? WHERE id=?",
                (attempt, worker_id, now + lease_seconds, now, now, row["id"]))
            c.execute("INSERT INTO job_attempts (job_id, attempt, worker_id, started_at) VALUES (?,?,?,?)",
                      (row["id"], attempt, worker_id, now))
            return _job(c.execute("SELECT * FROM jobs WHERE id=?", (row["id"],)).fetchone())

    # ---------------------------------------------------- fenced owner operations
    def _check(self, c: sqlite3.Connection, job_id: str, worker_id: str, attempt: int) -> None:
        row = c.execute(
            "SELECT 1 FROM jobs WHERE id=? AND lease_owner=? AND attempts=?"
            " AND status IN ('claimed','running','checkpointed')",
            (job_id, worker_id, attempt)).fetchone()
        if row is None:
            raise LeaseLost("worker no longer owns this job")

    def heartbeat(self, job: Job, worker_id: str, lease_seconds: float = DEFAULT_LEASE_SECONDS) -> None:
        with self._tx() as c:
            self._check(c, job.id, worker_id, job.attempts)
            now = self.clock()
            c.execute(
                "UPDATE jobs SET lease_expires_at=?, last_heartbeat_at=?, updated_at=?,"
                " status=CASE WHEN status='claimed' THEN 'running' ELSE status END WHERE id=?",
                (now + lease_seconds, now, now, job.id))

    def checkpoint(self, job: Job, worker_id: str, step: str, data: Any, lease_seconds: float = DEFAULT_LEASE_SECONDS) -> None:
        blob = canonical_json(data)  # raises TypeError if not JSON-serialisable
        with self._tx() as c:
            self._check(c, job.id, worker_id, job.attempts)
            now = self.clock()
            c.execute(
                "INSERT INTO job_checkpoints (job_id, step, data, attempt, created_at) VALUES (?,?,?,?,?)"
                " ON CONFLICT(job_id, step) DO UPDATE SET data=excluded.data,"
                " attempt=excluded.attempt, created_at=excluded.created_at",
                (job.id, step, blob, job.attempts, now))
            c.execute("UPDATE jobs SET status='checkpointed', lease_expires_at=?, last_heartbeat_at=?,"
                      " updated_at=? WHERE id=?", (now + lease_seconds, now, now, job.id))

    def complete(self, job: Job, worker_id: str, result: Any = None) -> None:
        blob = canonical_json(result)
        with self._tx() as c:
            self._check(c, job.id, worker_id, job.attempts)
            now = self.clock()
            c.execute("UPDATE jobs SET status='succeeded', result=?, lease_owner=NULL,"
                      " lease_expires_at=NULL, updated_at=? WHERE id=?", (blob, now, job.id))
            c.execute("UPDATE job_attempts SET ended_at=?, outcome='succeeded' WHERE job_id=? AND attempt=?",
                      (now, job.id, job.attempts))

    def fail(self, job: Job, worker_id: str, error: str, retryable: bool = True,
             base_backoff: float = BASE_BACKOFF_SECONDS) -> str:
        """Record a failure. Returns the new status."""
        err = redact(str(error))[:MAX_ERROR_CHARS]
        with self._tx() as c:
            self._check(c, job.id, worker_id, job.attempts)
            now = self.clock()
            if not retryable:
                status = "permanently_failed"
            elif job.attempts >= job.max_attempts:
                status = "dead_letter"
            else:
                status = "retryable_failed"
            delay = min(MAX_BACKOFF_SECONDS, base_backoff * (2 ** (job.attempts - 1)))
            run_after = now + delay if status == "retryable_failed" else now
            c.execute("UPDATE jobs SET status=?, last_error=?, run_after=?, lease_owner=NULL,"
                      " lease_expires_at=NULL, updated_at=? WHERE id=?", (status, err, run_after, now, job.id))
            c.execute("UPDATE job_attempts SET ended_at=?, outcome=?, error=? WHERE job_id=? AND attempt=?",
                      (now, status, err, job.id, job.attempts))
            if status in BAD_DEPENDENCY_STATES:
                self._reconcile(c, now)
            return status

    # -------------------------------------------------- external operation ledger
    def begin_operation(self, job: Job, worker_id: str, op_name: str, inputs: Any) -> OperationRecord:
        """Record that this job is about to perform an external operation. Fenced.

        Returns the ledger row. status=='committed' means the operation already finished
        (possibly in another attempt or another job) and `output` is its recorded result: do NOT
        execute again. executions>1 with status=='started' means an earlier attempt began it and
        may have produced the effect before crashing: probe the external system, or re-execute
        only if the operation is idempotent under operation_id."""
        input_hash = hash_obj(inputs)
        op_id = make_operation_id(job.documentary_id, job.stage, op_name, input_hash)
        with self._tx() as c:
            self._check(c, job.id, worker_id, job.attempts)
            now = self.clock()
            row = c.execute("SELECT * FROM external_operations WHERE operation_id=?", (op_id,)).fetchone()
            if row is None:
                c.execute(
                    "INSERT INTO external_operations (operation_id, documentary_id, stage, op_name, input_hash,"
                    " job_id, last_worker, last_attempt, created_at, updated_at) VALUES (?,?,?,?,?,?,?,?,?,?)",
                    (op_id, job.documentary_id, job.stage, op_name, input_hash, job.id, worker_id, job.attempts, now, now))
            elif row["input_hash"] != input_hash:
                raise OperationConflict("operation id collision with different input")
            elif row["status"] == "started":
                c.execute("UPDATE external_operations SET executions=executions+1, job_id=?, last_worker=?,"
                          " last_attempt=?, updated_at=? WHERE operation_id=?",
                          (job.id, worker_id, job.attempts, now, op_id))
            return _op(c.execute("SELECT * FROM external_operations WHERE operation_id=?", (op_id,)).fetchone())

    def commit_operation(self, job: Job, worker_id: str, operation_id: str, output: Any) -> OperationRecord:
        """Record the operation's result. Fenced. A second commit with an identical output is a
        no-op; a second commit with a different output raises OperationConflict."""
        blob = canonical_json(output)
        out_hash = sha256_hex(blob)
        with self._tx() as c:
            self._check(c, job.id, worker_id, job.attempts)
            now = self.clock()
            row = c.execute("SELECT * FROM external_operations WHERE operation_id=?", (operation_id,)).fetchone()
            if row is None or row["documentary_id"] != job.documentary_id or row["stage"] != job.stage:
                raise KeyError("unknown operation for this job")
            if row["status"] == "committed":
                if row["output_hash"] != out_hash:
                    raise OperationConflict("operation already committed with a different output")
            else:
                c.execute("UPDATE external_operations SET status='committed', output=?, output_hash=?,"
                          " committed_at=?, updated_at=? WHERE operation_id=?", (blob, out_hash, now, now, operation_id))
            return _op(c.execute("SELECT * FROM external_operations WHERE operation_id=?", (operation_id,)).fetchone())

    def get_operation(self, operation_id: str) -> Optional[OperationRecord]:
        row = self.conn.execute("SELECT * FROM external_operations WHERE operation_id=?", (operation_id,)).fetchone()
        return _op(row) if row else None

    # -------------------------------------------------------------------- reads
    def get(self, job_id: str) -> Job:
        row = self.conn.execute("SELECT * FROM jobs WHERE id=?", (job_id,)).fetchone()
        if row is None:
            raise KeyError(job_id)
        return _job(row)

    def checkpoints(self, job_id: str) -> dict[str, Any]:
        rows = self.conn.execute("SELECT step, data FROM job_checkpoints WHERE job_id=?", (job_id,)).fetchall()
        return {r["step"]: json.loads(r["data"]) for r in rows}

    def attempts_log(self, job_id: str) -> list[dict]:
        rows = self.conn.execute("SELECT * FROM job_attempts WHERE job_id=? ORDER BY attempt", (job_id,)).fetchall()
        return [dict(r) for r in rows]
