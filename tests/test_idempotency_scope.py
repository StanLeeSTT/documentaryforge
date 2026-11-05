"""Phase 1.1: scoped job identity, input-hash conflicts, schema/FK strictness."""
import os
import sqlite3
import tempfile
import unittest

from forge.jobs import (IdempotencyConflict, JobStore, UnknownDocumentary, ALL_STATES)
from tests.helpers import FakeClock, SharedDB


class ScopedIdentityTests(unittest.TestCase):
    def setUp(self):
        self.clock = FakeClock()
        self.db = SharedDB(self.clock)
        self.s = self.db.store()

    def tearDown(self):
        self.db.cleanup()

    def enq(self, doc="indus_valley_001", stage="research", key="k", **kw):
        return self.s.enqueue(documentary_id=doc, stage=stage, idempotency_key=key, **kw)

    def test_same_key_different_stage_is_a_different_job(self):
        self.assertNotEqual(self.enq(stage="plan").id, self.enq(stage="research").id)

    def test_same_key_different_documentary_is_a_different_job(self):
        a, b = self.enq(doc="indus_valley_001"), self.enq(doc="other_doc_002")
        self.assertNotEqual(a.id, b.id)
        self.assertEqual((a.documentary_id, b.documentary_id), ("indus_valley_001", "other_doc_002"))

    def test_repeat_enqueue_returns_the_same_job(self):
        self.assertEqual(self.enq(payload={"q": 1}).id, self.enq(payload={"q": 1}).id)

    def test_payload_key_order_does_not_matter(self):
        a = self.enq(payload={"a": 1, "b": [1, 2]})
        b = self.enq(payload={"b": [1, 2], "a": 1})
        self.assertEqual(a.id, b.id)

    def test_same_identity_different_payload_conflicts_instead_of_returning_old_job(self):
        self.enq(payload={"plan_version": 1})
        with self.assertRaises(IdempotencyConflict):
            self.enq(payload={"plan_version": 2})

    def test_same_identity_different_capability_conflicts(self):
        self.enq(capability="cpu")
        with self.assertRaises(IdempotencyConflict):
            self.enq(capability="gpu")

    def test_priority_change_alone_is_not_a_conflict(self):
        a = self.enq(priority=100)
        self.assertEqual(self.enq(priority=5).id, a.id)

    def test_new_key_for_changed_inputs_creates_new_job(self):
        a = self.enq(key="plan:v1", payload={"v": 1})
        b = self.enq(key="plan:v2", payload={"v": 2})
        self.assertNotEqual(a.id, b.id)

    def test_conflict_does_not_create_or_modify_rows(self):
        a = self.enq(payload={"v": 1})
        with self.assertRaises(IdempotencyConflict):
            self.enq(payload={"v": 2})
        n = self.s.conn.execute("SELECT COUNT(*) FROM jobs").fetchone()[0]
        self.assertEqual(n, 1)
        self.assertEqual(self.s.get(a.id).payload, {"v": 1})

    def test_re_enqueue_ignores_new_depends_on_for_existing_job(self):
        dep = self.enq(stage="plan")
        job = self.enq(stage="research", key="r")
        again = self.enq(stage="research", key="r", depends_on=[dep.id])
        self.assertEqual(again.id, job.id)
        self.assertEqual(self.s.conn.execute("SELECT COUNT(*) FROM job_dependencies").fetchone()[0], 0)

    def test_non_json_payload_raises_and_creates_nothing(self):
        with self.assertRaises(TypeError):
            self.enq(payload={"x": object()})
        with self.assertRaises(ValueError):
            self.enq(payload={"x": float("nan")})
        self.assertEqual(self.s.conn.execute("SELECT COUNT(*) FROM jobs").fetchone()[0], 0)

    def test_max_attempts_must_be_positive(self):
        with self.assertRaises(ValueError):
            self.enq(max_attempts=0)


class StrictSchemaTests(unittest.TestCase):
    def setUp(self):
        self.clock = FakeClock()
        self.db = SharedDB(self.clock)

    def tearDown(self):
        self.db.cleanup()

    def test_unregistered_documentary_rejected_in_strict_mode(self):
        s = self.db.store(strict=True)
        with self.assertRaises(UnknownDocumentary):
            s.enqueue(documentary_id="nope", stage="x", idempotency_key="k")

    def test_registered_documentary_accepted_and_registration_is_idempotent(self):
        s = self.db.store(strict=True)
        s.register_documentary("indus_valley_001", "The Lost Civilization of the Indus Valley")
        s.register_documentary("indus_valley_001", "ignored second title")
        job = s.enqueue(documentary_id="indus_valley_001", stage="plan", idempotency_key="k")
        self.assertEqual(job.documentary_id, "indus_valley_001")
        self.assertEqual(s.conn.execute("SELECT COUNT(*) FROM documentaries").fetchone()[0], 1)
        self.assertEqual(s.conn.execute("SELECT title FROM documentaries").fetchone()[0],
                         "The Lost Civilization of the Indus Valley")

    def test_database_rejects_invalid_status_and_zero_max_attempts(self):
        s = self.db.store()
        j = s.enqueue(documentary_id="d", stage="x", idempotency_key="k")
        with self.assertRaises(sqlite3.IntegrityError):
            s.conn.execute("UPDATE jobs SET status='bogus' WHERE id=?", (j.id,))
        with self.assertRaises(sqlite3.IntegrityError):
            s.conn.execute("UPDATE jobs SET max_attempts=0 WHERE id=?", (j.id,))

    def test_status_check_accepts_every_documented_state(self):
        s = self.db.store()
        j = s.enqueue(documentary_id="d", stage="x", idempotency_key="k")
        for state in ALL_STATES:
            s.conn.execute("UPDATE jobs SET status=? WHERE id=?", (state, j.id))

    def test_lease_seconds_must_be_positive(self):
        s = self.db.store()
        with self.assertRaises(ValueError):
            s.claim("w", ["cpu"], 0)

    def test_old_phase1_database_is_rejected_not_silently_reused(self):
        with tempfile.TemporaryDirectory() as d:
            path = os.path.join(d, "old.sqlite3")
            raw = sqlite3.connect(path)
            raw.execute("CREATE TABLE jobs (id TEXT PRIMARY KEY, idempotency_key TEXT UNIQUE)")
            raw.commit()
            raw.close()
            with self.assertRaises(RuntimeError):
                JobStore(path)

    def test_reopening_a_current_database_works(self):
        s = self.db.store()
        s.enqueue(documentary_id="d", stage="x", idempotency_key="k")
        again = self.db.store()
        self.assertEqual(again.conn.execute("SELECT COUNT(*) FROM jobs").fetchone()[0], 1)

    def test_attempts_never_decrease_across_requeue(self):
        s = self.db.store()
        j = s.enqueue(documentary_id="d", stage="x", idempotency_key="k", max_attempts=1)
        c1 = s.claim("w1", ["cpu"])
        s.fail(c1, "w1", "boom", retryable=False)
        r = s.requeue_job(j.id, extra_attempts=2)
        self.assertEqual((r.attempts, r.max_attempts), (1, 3))
        c2 = s.claim("w2", ["cpu"])
        self.assertEqual(c2.attempts, 2)  # fencing token kept growing


if __name__ == "__main__":
    unittest.main()
