import unittest

from forge.worker import PermanentError, run_one
from tests.helpers import FakeClock, SharedDB


class SimulatedCrash(BaseException):
    """Stands in for a killed process: not an Exception, so nothing records a failure."""


class RecoveryTests(unittest.TestCase):
    def setUp(self):
        self.clock = FakeClock()
        self.db = SharedDB(self.clock)
        self.s1, self.s2 = self.db.store(), self.db.store()
        self.runs: list[str] = []  # records every time a step body actually executes

    def tearDown(self):
        self.db.cleanup()

    def handler(self, crash_after=None, fail_at=None):
        def h(ctx):
            out = []
            for name in ("discover", "fetch", "parse"):
                def body(n=name):
                    self.runs.append(n)
                    if fail_at == n:
                        raise RuntimeError("provider timeout")
                    return f"{n}-done"
                out.append(ctx.step(name, body))
                if crash_after == name:
                    raise SimulatedCrash()
            return out
        return h

    def test_crashed_worker_resumes_without_repeating_steps(self):
        job = self.s1.enqueue(documentary_id="indus_valley_001", stage="research", idempotency_key="r1")
        with self.assertRaises(SimulatedCrash):
            run_one(self.s1, "w1", ["cpu"], {"research": self.handler(crash_after="fetch")}, 60)
        self.assertEqual(self.runs, ["discover", "fetch"])
        self.assertEqual(self.s2.get(job.id).status, "checkpointed")

        self.assertIsNone(run_one(self.s2, "w2", ["cpu"], {"research": self.handler()}, 60))  # lease valid
        self.clock.advance(61)
        self.assertEqual(run_one(self.s2, "w2", ["cpu"], {"research": self.handler()}, 60), "succeeded")

        self.assertEqual(self.runs, ["discover", "fetch", "parse"])  # nothing repeated
        done = self.s2.get(job.id)
        self.assertEqual((done.status, done.attempts), ("succeeded", 2))
        self.assertEqual(done.result, ["discover-done", "fetch-done", "parse-done"])

    def test_retry_after_error_skips_completed_steps(self):
        job = self.s1.enqueue(documentary_id="d", stage="research", idempotency_key="r2")
        out = run_one(self.s1, "w1", ["cpu"], {"research": self.handler(fail_at="parse")})
        self.assertEqual(out, "retryable_failed")
        self.clock.advance(31)
        self.assertEqual(run_one(self.s2, "w2", ["cpu"], {"research": self.handler()}), "succeeded")
        self.assertEqual(self.runs, ["discover", "fetch", "parse", "parse"])
        self.assertEqual(self.s1.get(job.id).attempts, 2)

    def test_succeeded_job_never_runs_again(self):
        self.s1.enqueue(documentary_id="d", stage="research", idempotency_key="r3")
        self.assertEqual(run_one(self.s1, "w1", ["cpu"], {"research": self.handler()}), "succeeded")
        self.clock.advance(1_000_000)
        self.assertIsNone(run_one(self.s2, "w2", ["cpu"], {"research": self.handler()}))
        self.assertEqual(len(self.runs), 3)

    def test_slow_worker_is_fenced_out_after_takeover(self):
        job = self.s1.enqueue(documentary_id="d", stage="research", idempotency_key="r4")

        def slow(ctx):
            ctx.step("discover", lambda: "a")
            self.clock.advance(61)  # this worker stalls past its lease...
            # ...and another worker takes over and finishes the job:
            self.assertEqual(run_one(self.s2, "w2", ["cpu"], {"research": self.handler()}, 60), "succeeded")
            ctx.step("fetch", lambda: "b")  # stale write must be rejected

        out = run_one(self.s1, "w1", ["cpu"], {"research": slow}, 60)
        self.assertEqual(out, "lease_lost")
        final = self.s1.get(job.id)
        self.assertEqual((final.status, final.lease_owner), ("succeeded", None))

    def test_permanent_error_and_missing_handler(self):
        def bad(ctx):
            raise PermanentError("brief is empty")
        self.s1.enqueue(documentary_id="d", stage="research", idempotency_key="p1")
        self.assertEqual(run_one(self.s1, "w", ["cpu"], {"research": bad}), "permanently_failed")
        self.s1.enqueue(documentary_id="d", stage="unknown", idempotency_key="p2")
        self.assertEqual(run_one(self.s1, "w", ["cpu"], {}), "permanently_failed")

    def test_pipeline_enqueue_twice_creates_no_duplicates(self):
        def build(store):
            a = store.enqueue(documentary_id="d", stage="plan", idempotency_key="d:plan")
            b = store.enqueue(documentary_id="d", stage="research", idempotency_key="d:research", depends_on=[a.id])
            return a.id, b.id
        self.assertEqual(build(self.s1), build(self.s2))

    def test_stage_dependency_order(self):
        order = []
        a = self.s1.enqueue(documentary_id="d", stage="plan", idempotency_key="d:plan")
        self.s1.enqueue(documentary_id="d", stage="research", idempotency_key="d:research", depends_on=[a.id])
        handlers = {s: (lambda ctx, s=s: order.append(s)) for s in ("plan", "research")}
        while run_one(self.s2, "w", ["cpu"], handlers):
            pass
        self.assertEqual(order, ["plan", "research"])


if __name__ == "__main__":
    unittest.main()
