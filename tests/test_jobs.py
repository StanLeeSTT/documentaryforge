import threading
import unittest

from forge.jobs import LeaseLost
from tests.helpers import FakeClock, SharedDB

DOC = "indus_valley_001"


class JobEngineTests(unittest.TestCase):
    def setUp(self):
        self.clock = FakeClock()
        self.db = SharedDB(self.clock)
        self.a = self.db.store()
        self.b = self.db.store()

    def tearDown(self):
        self.db.cleanup()

    def enqueue(self, key="k1", store=None, **kw):
        return (store or self.a).enqueue(documentary_id=DOC, stage="demo", idempotency_key=key, **kw)

    def test_enqueue_is_idempotent(self):
        j1, j2 = self.enqueue("same"), self.enqueue("same")
        self.assertEqual(j1.id, j2.id)

    def test_claim_sets_lease_and_attempt(self):
        self.enqueue()
        job = self.a.claim("w1", ["cpu"], 60)
        self.assertEqual((job.status, job.attempts, job.lease_owner), ("claimed", 1, "w1"))
        self.assertEqual(job.lease_expires_at, self.clock.t + 60)

    def test_capability_filtering(self):
        self.enqueue(capability="gpu")
        self.assertIsNone(self.a.claim("cpu-worker", ["cpu"]))
        self.assertIsNotNone(self.a.claim("gpu-worker", ["cpu", "gpu"]))

    def test_priority_then_age(self):
        self.enqueue("low", priority=200)
        hi = self.enqueue("high", priority=10)
        self.assertEqual(self.a.claim("w", ["cpu"]).id, hi.id)

    def test_duplicate_claim_single_job_many_threads(self):
        self.enqueue()
        winners, barrier = [], threading.Barrier(8)

        def attempt(i):
            s = self.db.store(track=False)  # own connection = own worker
            try:
                barrier.wait()
                job = s.claim(f"w{i}", ["cpu"])
                if job:
                    winners.append(job.lease_owner)
            finally:
                s.close()

        threads = [threading.Thread(target=attempt, args=(i,)) for i in range(8)]
        [t.start() for t in threads]
        [t.join() for t in threads]
        self.assertEqual(len(winners), 1)

    def test_many_jobs_many_workers_each_claimed_once(self):
        ids = {self.enqueue(f"j{i}").id for i in range(20)}
        claimed, lock = [], threading.Lock()

        def worker(i):
            s = self.db.store(track=False)
            try:
                while True:
                    job = s.claim(f"w{i}", ["cpu"])
                    if job is None:
                        return
                    with lock:
                        claimed.append(job.id)
            finally:
                s.close()

        threads = [threading.Thread(target=worker, args=(i,)) for i in range(6)]
        [t.start() for t in threads]
        [t.join() for t in threads]
        self.assertEqual(len(claimed), 20)
        self.assertEqual(set(claimed), ids)

    def test_dependencies_block_until_parent_succeeds(self):
        parent = self.enqueue("parent")
        self.enqueue("child", depends_on=[parent.id])
        got = self.a.claim("w1", ["cpu"])
        self.assertEqual(got.id, parent.id)
        self.assertIsNone(self.b.claim("w2", ["cpu"]))  # child still blocked
        self.a.complete(got, "w1")
        self.assertIsNotNone(self.b.claim("w2", ["cpu"]))

    def test_unknown_dependency_rejected(self):
        with self.assertRaises(ValueError):
            self.enqueue(depends_on=["nope"])

    def test_expired_lease_is_reclaimed_and_old_worker_fenced_out(self):
        self.enqueue()
        old = self.a.claim("w1", ["cpu"], 60)
        self.assertIsNone(self.b.claim("w2", ["cpu"]))  # lease still valid
        self.clock.advance(61)
        new = self.b.claim("w2", ["cpu"], 60)
        self.assertEqual((new.id, new.attempts, new.lease_owner), (old.id, 2, "w2"))
        with self.assertRaises(LeaseLost):
            self.a.complete(old, "w1")
        with self.assertRaises(LeaseLost):
            self.a.checkpoint(old, "w1", "s", 1)
        self.b.complete(new, "w2", {"ok": True})
        self.assertEqual(self.b.get(old.id).status, "succeeded")

    def test_late_completion_allowed_if_nobody_reclaimed(self):
        self.enqueue()
        job = self.a.claim("w1", ["cpu"], 60)
        self.clock.advance(500)  # lease expired, but nobody took over
        self.a.complete(job, "w1")
        self.assertEqual(self.a.get(job.id).status, "succeeded")

    def test_heartbeat_extends_lease(self):
        self.enqueue()
        job = self.a.claim("w1", ["cpu"], 60)
        self.clock.advance(50)
        self.a.heartbeat(job, "w1", 60)
        self.clock.advance(50)  # 100s since claim, but heartbeat moved expiry to t+110
        self.assertIsNone(self.b.claim("w2", ["cpu"]))
        self.assertEqual(self.a.get(job.id).status, "running")

    def test_retryable_failure_backs_off_exponentially(self):
        self.enqueue(max_attempts=4)
        j = self.a.claim("w", ["cpu"])
        self.assertEqual(self.a.fail(j, "w", "boom"), "retryable_failed")
        self.assertIsNone(self.a.claim("w", ["cpu"]))
        self.clock.advance(29)
        self.assertIsNone(self.a.claim("w", ["cpu"]))
        self.clock.advance(2)  # 31s >= 30s backoff
        j2 = self.a.claim("w", ["cpu"])
        self.assertEqual(j2.attempts, 2)
        self.a.fail(j2, "w", "boom")
        self.clock.advance(59)  # needs 60s now
        self.assertIsNone(self.a.claim("w", ["cpu"]))
        self.clock.advance(2)
        self.assertEqual(self.a.claim("w", ["cpu"]).attempts, 3)

    def test_retries_exhausted_goes_to_dead_letter(self):
        self.enqueue(max_attempts=2)
        for _ in range(2):
            j = self.a.claim("w", ["cpu"])
            status = self.a.fail(j, "w", "boom")
            self.clock.advance(10_000)
        self.assertEqual(status, "dead_letter")
        self.assertIsNone(self.a.claim("w", ["cpu"]))

    def test_repeated_lease_expiry_goes_to_dead_letter(self):
        job = self.enqueue(max_attempts=2)
        for i in range(2):  # worker "crashes" twice
            self.assertIsNotNone(self.a.claim(f"w{i}", ["cpu"], 60))
            self.clock.advance(61)
        self.assertIsNone(self.b.claim("w3", ["cpu"]))  # sweep happens here
        self.assertEqual(self.b.get(job.id).status, "dead_letter")

    def test_permanent_failure_not_retried(self):
        self.enqueue()
        j = self.a.claim("w", ["cpu"])
        self.assertEqual(self.a.fail(j, "w", "bad input", retryable=False), "permanently_failed")
        self.clock.advance(100_000)
        self.assertIsNone(self.a.claim("w", ["cpu"]))

    def test_attempt_history_recorded(self):
        job = self.enqueue()
        self.a.claim("w1", ["cpu"], 60)
        self.clock.advance(61)
        new = self.b.claim("w2", ["cpu"])
        self.b.complete(new, "w2")
        log = self.a.attempts_log(job.id)
        self.assertEqual([(r["attempt"], r["outcome"]) for r in log], [(1, "lease_expired"), (2, "succeeded")])


if __name__ == "__main__":
    unittest.main()
