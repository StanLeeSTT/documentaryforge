"""STATIC parity checks between the SQLite reference engine and the PostgreSQL migration.

These read the SQL text and the Python source. They catch drift in constants, state lists,
signatures, fencing predicates and constraints. They do NOT execute any SQL, and they prove
NOTHING about PostgreSQL runtime behaviour. PostgreSQL compatibility remains UNVERIFIED until
the migration has been executed against a real PostgreSQL (Phase 2)."""
import inspect
import os
import re
import unittest

from forge import jobs as J

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
with open(os.path.join(REPO, "supabase/migrations/0001_foundation.sql")) as fh:
    SQL = fh.read()
SRC = inspect.getsource(J)


def tuple_literal(values):
    return "(" + ",".join(f"'{v}'" for v in values) + ")"


class StaticParityTests(unittest.TestCase):
    def test_job_status_set_is_identical(self):
        m = re.search(r"status\s+text not null default 'queued'\s+check \(status in \(([^)]*)\)\)", SQL)
        self.assertIsNotNone(m)
        self.assertEqual(re.findall(r"'([a-z_]+)'", m.group(1)), list(J.ALL_STATES))

    def test_state_groups_match(self):
        self.assertIn(tuple_literal(J.ACTIVE_STATES), SQL)
        self.assertIn(tuple_literal(J.BAD_DEPENDENCY_STATES), SQL)
        self.assertIn(tuple_literal(J.REQUEUEABLE_STATES), SQL)
        self.assertIn(tuple_literal(J.DEPENDENCY_EDITABLE_STATES), SQL)

    def test_numeric_defaults_match(self):
        self.assertIn(f"p_lease_seconds int default {J.DEFAULT_LEASE_SECONDS}", SQL)
        self.assertIn(f"priority             int  not null default {J.DEFAULT_PRIORITY}", SQL)
        self.assertIn(f"max_attempts         int  not null default {J.DEFAULT_MAX_ATTEMPTS} check (max_attempts >= 1)", SQL)
        self.assertIn(f"p_base_backoff_seconds int default {J.BASE_BACKOFF_SECONDS}", SQL)
        self.assertIn(f"least({J.MAX_BACKOFF_SECONDS}, p_base_backoff_seconds * power(2, j.attempts - 1))", SQL)
        self.assertIn(f"left(p_error, {J.MAX_ERROR_CHARS})", SQL)
        self.assertIn("min(MAX_BACKOFF_SECONDS, base_backoff * (2 ** (job.attempts - 1)))".replace("job.", "job."), SRC)

    def test_job_identity_is_scoped_in_both(self):
        self.assertIn("unique (documentary_id, stage, idempotency_key)", SQL)
        self.assertIn("UNIQUE (documentary_id, stage, idempotency_key)", J.SCHEMA)
        self.assertNotRegex(SQL, r"idempotency_key\s+text not null unique")
        self.assertNotRegex(J.SCHEMA, r"idempotency_key TEXT NOT NULL UNIQUE")
        self.assertIn("input_hash", SQL)
        self.assertIn("input_hash TEXT NOT NULL", J.SCHEMA)

    def test_claim_ordering_is_deterministic_and_equal(self):
        self.assertIn("order by p.priority, p.seq", SQL)
        self.assertIn("ORDER BY priority, seq", inspect.getsource(J.JobStore.claim))
        self.assertIn("seq                  bigint generated always as identity", SQL)

    def test_claim_conditions_match(self):
        claim_sql = inspect.getsource(J.JobStore.claim)
        for fragment_sql, fragment_py in (
            ("p.run_after <= now()", "run_after <= ?"),
            ("p.lease_expires_at < now()", "lease_expires_at < ?"),
            ("p.attempts < p.max_attempts", "attempts < max_attempts"),
            ("dj.status <> 'succeeded'", "dj.status <> 'succeeded'"),
            ("attempts >= max_attempts", "attempts >= max_attempts"),
        ):
            self.assertIn(fragment_sql, SQL)
            self.assertIn(fragment_py, claim_sql)

    def test_fencing_predicate_count_matches(self):
        # heartbeat, checkpoint, complete, fail, begin_operation, commit_operation
        self.assertEqual(SQL.count("attempts = p_attempt"), 6)
        self.assertEqual(SRC.count("self._check(c, job.id, worker_id, job.attempts)"), 6)

    def test_every_python_operation_has_a_sql_function(self):
        mapping = {"enqueue": "enqueue_job", "add_dependency": "add_dependency", "claim": "claim_job",
                   "heartbeat": "heartbeat_job", "checkpoint": "checkpoint_job", "complete": "complete_job",
                   "fail": "fail_job", "cancel_job": "cancel_job", "requeue_job": "requeue_job",
                   "begin_operation": "begin_operation", "commit_operation": "commit_operation",
                   "_reconcile": "reconcile_blocked_jobs"}
        for py, sql in mapping.items():
            with self.subTest(op=py):
                self.assertTrue(hasattr(J.JobStore, py))
                self.assertRegex(SQL, rf"create or replace function public\.{sql}\(")
        self.assertIn("create or replace view public.blocked_jobs", SQL)
        self.assertTrue(hasattr(J.JobStore, "blocked_jobs"))

    def test_dependency_protection_present_in_sql(self):
        for fragment in ("create trigger job_dependencies_guard_trg", "pg_advisory_xact_lock",
                         "dependency_cycle", "cross-documentary", "job already started",
                         "with recursive reach(id)"):
            self.assertIn(fragment, SQL)

    def test_error_codes_declared(self):
        for code in ("DF001", "DF002", "DF003", "DF004"):
            self.assertIn(code, SQL)
        for cls in ("IdempotencyConflict", "DependencyCycle", "DependencyError", "OperationConflict", "LeaseLost"):
            self.assertTrue(hasattr(J, cls))

    def test_every_table_has_rls_and_every_function_is_locked_down(self):
        tables = re.findall(r"create table if not exists public\.(\w+)", SQL)
        rls = re.findall(r"alter table public\.(\w+)\s+enable row level security", SQL)
        self.assertEqual(sorted(tables), sorted(rls))
        self.assertEqual(len(tables), 8)
        functions = re.findall(r"create or replace function public\.(\w+)\(", SQL)
        revoke_block = SQL[SQL.index("foreach f in array array["):]
        for fn in functions:
            with self.subTest(function=fn):
                self.assertIn(f"'{fn}(", revoke_block)

    def test_external_operation_ledger_columns_match(self):
        for col in ("operation_id", "documentary_id", "stage", "op_name", "input_hash", "status", "output",
                    "output_hash", "executions", "job_id", "last_worker", "last_attempt", "committed_at"):
            self.assertIn(col, SQL)
            self.assertIn(col, J.SCHEMA)


if __name__ == "__main__":
    unittest.main()
