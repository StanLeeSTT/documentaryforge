"""Phase 1.1: DAG enforcement and failed/cancelled-dependency handling."""
import unittest

from forge.jobs import DependencyCycle, DependencyError, LeaseLost
from forge.worker import run_one
from tests.helpers import FakeClock, SharedDB

DOC = "indus_valley_001"


class DependencyTests(unittest.TestCase):
    def setUp(self):
        self.clock = FakeClock()
        self.db = SharedDB(self.clock)
        self.s = self.db.store()
        self.w = self.db.store()

    def tearDown(self):
        self.db.cleanup()

    def job(self, name, doc=DOC, deps=(), **kw):
        return self.s.enqueue(documentary_id=doc, stage=name, idempotency_key=name, depends_on=deps, **kw)

    def edges(self):
        return self.s.conn.execute("SELECT COUNT(*) FROM job_dependencies").fetchone()[0]

    def status(self, j):
        return self.s.get(j.id).status

    def fail_permanently(self, j):
        got = self.w.claim("w", ["cpu"])
        self.assertEqual(got.id, j.id)
        self.w.fail(got, "w", "bad input", retryable=False)

    # ---- cycles -------------------------------------------------------------------------
    def test_self_dependency_rejected(self):
        a = self.job("a")
        with self.assertRaises(DependencyCycle):
            self.s.add_dependency(a.id, a.id)
        self.assertEqual(self.edges(), 0)

    def test_two_job_cycle_rejected(self):
        a, b = self.job("a"), self.job("b")
        self.s.add_dependency(a.id, b.id)
        with self.assertRaises(DependencyCycle):
            self.s.add_dependency(b.id, a.id)
        self.assertEqual(self.edges(), 1)

    def test_long_cycle_rejected_and_nothing_inserted(self):
        a, b, c, d, e = (self.job(n) for n in "abcde")
        for x, y in ((a, b), (b, c), (c, d), (d, e)):
            self.s.add_dependency(x.id, y.id)
        with self.assertRaises(DependencyCycle):
            self.s.add_dependency(e.id, a.id)
        with self.assertRaises(DependencyCycle):
            self.s.add_dependency(c.id, a.id)
        self.assertEqual(self.edges(), 4)
        self.assertEqual(self.w.claim("w", ["cpu"]).id, e.id)    # the chain still runs from its end

    def test_diamond_is_not_a_false_positive(self):
        a, b, c, d = (self.job(n) for n in "abcd")
        for x, y in ((a, b), (a, c), (b, d), (c, d)):
            self.s.add_dependency(x.id, y.id)
        self.assertEqual(self.edges(), 4)

    def test_pipeline_runs_in_dependency_order(self):
        order = []
        a = self.job("plan"); b = self.job("research", deps=[a.id]); self.job("script", deps=[b.id])
        handlers = {n: (lambda ctx, n=n: order.append(n)) for n in ("plan", "research", "script")}
        while run_one(self.w, "w", ["cpu"], handlers):
            pass
        self.assertEqual(order, ["plan", "research", "script"])

    def test_add_dependency_is_idempotent(self):
        a, b = self.job("a"), self.job("b")
        self.s.add_dependency(a.id, b.id)
        self.s.add_dependency(a.id, b.id)
        self.assertEqual(self.edges(), 1)

    def test_unknown_and_cross_documentary_dependencies_rejected(self):
        a = self.job("a")
        other = self.job("x", doc="another_doc")
        with self.assertRaises(DependencyError):
            self.s.add_dependency(a.id, "nope")
        with self.assertRaises(DependencyError):
            self.s.add_dependency(a.id, other.id)
        with self.assertRaises(DependencyError):
            self.job("y", deps=[other.id])
        with self.assertRaises(ValueError):                      # legacy contract: still a ValueError
            self.job("z", deps=["nope"])
        self.assertEqual(self.edges(), 0)

    def test_cannot_add_dependency_to_a_job_that_already_started(self):
        a, b = self.job("a"), self.job("b")
        got = self.w.claim("w", ["cpu"])
        self.assertEqual(got.id, a.id)
        with self.assertRaises(DependencyError):
            self.s.add_dependency(a.id, b.id)

    # ---- failed / cancelled dependencies -------------------------------------------------------
    def test_permanently_failed_dependency_blocks_dependents_transitively(self):
        a = self.job("a"); b = self.job("b", deps=[a.id]); c = self.job("c", deps=[b.id]); free = self.job("free")
        self.fail_permanently(a)  # 'a' has lowest seq so it is claimed first
        self.assertEqual((self.status(b), self.status(c)), ("needs_review", "needs_review"))
        self.assertEqual(self.s.get(b.id).blocked_by, a.id)
        self.assertEqual(self.s.get(c.id).blocked_by, a.id)
        report = {r.job_id: r for r in self.s.blocked_jobs()}
        self.assertEqual(set(report), {b.id, c.id})
        self.assertTrue(all(r.reason == "dependency_failed" and r.blocked_by == a.id for r in report.values()))
        self.assertIn("permanently_failed", report[b.id].detail)
        self.assertEqual(self.w.claim("w", ["cpu"]).id, free.id)  # unrelated work continues
        self.assertIsNone(self.w.claim("w", ["cpu"]))             # blocked jobs are never claimed

    def test_dead_letter_dependency_blocks_dependents(self):
        a = self.job("a", max_attempts=1); b = self.job("b", deps=[a.id])
        got = self.w.claim("w", ["cpu"])
        self.assertEqual(self.w.fail(got, "w", "boom"), "dead_letter")
        self.assertEqual(self.status(b), "needs_review")
        self.assertEqual(self.s.blocked_jobs()[0].blocked_by, a.id)

    def test_lease_expiry_dead_letter_also_blocks_dependents(self):
        a = self.job("a", max_attempts=1); b = self.job("b", deps=[a.id])
        self.w.claim("w1", ["cpu"], 60)
        self.clock.advance(61)
        self.assertIsNone(self.s.claim("w2", ["cpu"]))           # sweep: a -> dead_letter, b -> needs_review
        self.assertEqual((self.status(a), self.status(b)), ("dead_letter", "needs_review"))

    def test_cancelled_dependency_blocks_dependents(self):
        a = self.job("a"); b = self.job("b", deps=[a.id]); c = self.job("c", deps=[b.id])
        self.assertTrue(self.s.cancel_job(a.id, "operator cancelled"))
        self.assertEqual((self.status(a), self.status(b), self.status(c)), ("cancelled", "needs_review", "needs_review"))
        self.assertIsNone(self.w.claim("w", ["cpu"]))
        self.assertEqual(len(self.s.blocked_jobs()), 2)
        self.assertIn("cancelled", self.s.get(b.id).last_error)

    def test_cancelling_a_running_job_fences_out_its_worker(self):
        a = self.job("a")
        got = self.w.claim("w", ["cpu"], 60)
        self.assertTrue(self.s.cancel_job(a.id))
        with self.assertRaises(LeaseLost):
            self.w.heartbeat(got, "w")
        with self.assertRaises(LeaseLost):
            self.w.complete(got, "w")
        self.assertEqual(self.status(a), "cancelled")

    def test_cancel_is_noop_for_finished_or_cancelled_jobs(self):
        a = self.job("a")
        got = self.w.claim("w", ["cpu"])
        self.w.complete(got, "w")
        self.assertFalse(self.s.cancel_job(a.id))
        b = self.job("b")
        self.assertTrue(self.s.cancel_job(b.id))
        self.assertFalse(self.s.cancel_job(b.id))

    def test_enqueue_under_an_already_failed_dependency_is_blocked_immediately(self):
        a = self.job("a"); self.fail_permanently(a)
        b = self.job("b", deps=[a.id])
        self.assertEqual((b.status, self.s.get(b.id).blocked_by), ("needs_review", a.id))

    def test_adding_a_dependency_on_a_failed_job_blocks(self):
        a = self.job("a"); b = self.job("b")
        self.fail_permanently(a)
        self.s.add_dependency(b.id, a.id)
        self.assertEqual(self.status(b), "needs_review")

    def test_requeue_root_cause_releases_dependents_and_pipeline_completes(self):
        order = []
        a = self.job("a"); b = self.job("b", deps=[a.id]); c = self.job("c", deps=[b.id])
        self.fail_permanently(a)
        self.assertEqual(len(self.s.blocked_jobs()), 2)
        r = self.s.requeue_job(a.id)
        self.assertEqual(r.status, "queued")
        self.assertEqual((self.status(b), self.status(c)), ("queued", "queued"))
        self.assertEqual(self.s.blocked_jobs(), [])
        handlers = {n: (lambda ctx, n=n: order.append(n)) for n in ("a", "b", "c")}
        while run_one(self.w, "w", ["cpu"], handlers):
            pass
        self.assertEqual(order, ["a", "b", "c"])

    def test_partial_release_when_another_dependency_is_still_failed(self):
        a = self.job("a"); b = self.job("b"); c = self.job("c", deps=[a.id, b.id])
        self.fail_permanently(a); self.fail_permanently(b)
        self.assertEqual(self.s.get(c.id).blocked_by, min(a.id, b.id))
        self.s.requeue_job(a.id)
        self.s.requeue_job(b.id)  # both back in play -> c released
        self.assertEqual(self.status(c), "queued")
        self.fail_permanently(a)  # a fails again (claimed first: lowest seq)
        self.assertEqual((self.status(c), self.s.get(c.id).blocked_by), ("needs_review", a.id))
        self.s.cancel_job(b.id)
        self.s.requeue_job(a.id)  # a healthy again, but b is cancelled -> c stays blocked, now by b
        self.assertEqual((self.status(c), self.s.get(c.id).blocked_by), ("needs_review", b.id))

    def test_requeue_rejected_for_healthy_jobs_and_bad_arguments(self):
        a = self.job("a")
        with self.assertRaises(ValueError):
            self.s.requeue_job(a.id)                      # queued jobs are not requeueable
        self.fail_permanently(a)
        with self.assertRaises(ValueError):
            self.s.requeue_job(a.id, extra_attempts=0)    # must grant at least one attempt
        with self.assertRaises(KeyError):
            self.s.requeue_job("no-such-job")

    def test_blocked_jobs_empty_when_healthy(self):
        a = self.job("a"); self.job("b", deps=[a.id])
        self.assertEqual(self.s.blocked_jobs(), [])


if __name__ == "__main__":
    unittest.main()
