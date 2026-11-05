"""Phase 1.1: external-operation ledger. A crash AFTER an external effect succeeds but BEFORE any
checkpoint/ledger record exists must not duplicate the effect (when the operation is idempotent
or probe-able). Checkpoints alone give at-least-once, not exactly-once."""
import hashlib
import unittest

from forge.idempotency import canonical_json, hash_obj, make_operation_id
from forge.jobs import LeaseLost, OperationConflict
from forge.worker import run_one
from tests.helpers import FakeClock, SharedDB
from tests.test_recovery import SimulatedCrash

DOC, STAGE = "indus_valley_001", "narration"


def sha(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


class FakeStorage:
    """Object store that is idempotent by key (like Supabase Storage with upsert, or S3 PUT)."""
    def __init__(self):
        self.objects, self.put_calls, self.effects = {}, 0, 0

    def put(self, key, data):
        self.put_calls += 1
        if key not in self.objects:
            self.objects[key] = data
            self.effects += 1
        return {"key": key, "sha256": sha(self.objects[key])}

    def head(self, key):
        return {"key": key, "sha256": sha(self.objects[key])} if key in self.objects else None


class OperationTests(unittest.TestCase):
    def setUp(self):
        self.clock = FakeClock()
        self.db = SharedDB(self.clock)
        self.s1, self.s2 = self.db.store(), self.db.store()
        self.storage = FakeStorage()
        self.calls = []        # every time an execute() body actually runs
        self.probes = []

    def tearDown(self):
        self.db.cleanup()

    def enqueue(self, key="n1"):
        return self.s1.enqueue(documentary_id=DOC, stage=STAGE, idempotency_key=key)

    def handler(self, crash_after_effect=False, use_probe=True, inputs=None, crash_after_external=False):
        inputs = inputs if inputs is not None else {"text": "The Indus cities were planned.", "voice": "v1"}

        def h(ctx):
            def execute(op_id):
                self.calls.append(op_id)
                out = self.storage.put(op_id, b"audio-bytes")   # the external effect happens here
                if crash_after_effect:
                    raise SimulatedCrash()                      # ...and the process dies right after
                return out

            def probe(op_id):
                self.probes.append(op_id)
                return self.storage.head(op_id)

            def body():
                out = ctx.external("upload_audio", inputs, execute, probe if use_probe else None)
                if crash_after_external:
                    raise SimulatedCrash()
                return out
            return ctx.step("narrate", body)
        return h

    # ---- the core scenario ------------------------------------------------------------
    def test_crash_after_effect_before_record_probe_prevents_reexecution(self):
        job = self.enqueue()
        with self.assertRaises(SimulatedCrash):
            run_one(self.s1, "w1", ["cpu"], {STAGE: self.handler(crash_after_effect=True)}, 60)
        self.assertEqual(self.storage.effects, 1)                       # effect happened
        self.assertEqual(self.s1.checkpoints(job.id), {})               # but nothing was checkpointed
        op_id = self.calls[0]
        self.assertEqual(self.s1.get_operation(op_id).status, "started")  # ledger shows it was begun

        self.clock.advance(61)
        self.assertEqual(run_one(self.s2, "w2", ["cpu"], {STAGE: self.handler()}, 60), "succeeded")

        self.assertEqual(len(self.calls), 1)          # execute NOT run a second time
        self.assertEqual(self.storage.effects, 1)
        self.assertEqual(self.storage.put_calls, 1)
        rec = self.s2.get_operation(op_id)
        self.assertEqual((rec.status, rec.executions), ("committed", 2))
        self.assertEqual(rec.output["key"], op_id)    # the deterministic ID was used as the object key
        self.assertEqual(self.s2.get(job.id).result["key"], op_id)

    def test_crash_without_probe_reexecutes_but_effect_stays_single(self):
        """At-least-once EXECUTION + idempotent operation = exactly-once EFFECT."""
        self.enqueue()
        with self.assertRaises(SimulatedCrash):
            run_one(self.s1, "w1", ["cpu"], {STAGE: self.handler(crash_after_effect=True, use_probe=False)}, 60)
        self.clock.advance(61)
        self.assertEqual(run_one(self.s2, "w2", ["cpu"], {STAGE: self.handler(use_probe=False)}, 60), "succeeded")
        self.assertEqual(len(self.calls), 2)                    # executed twice (at-least-once)
        self.assertEqual(self.calls[0], self.calls[1])          # with the SAME deterministic id
        self.assertEqual((self.storage.put_calls, self.storage.effects), (2, 1))  # one object only
        self.assertEqual(len(self.storage.objects), 1)

    def test_checkpoint_alone_is_at_least_once_not_exactly_once(self):
        """Documents the hazard: a plain checkpoint around a non-idempotent effect duplicates it."""
        appended = []

        def crashing(ctx):
            def body():
                appended.append("segment-1")     # non-idempotent external effect
                raise SimulatedCrash()           # dies before the checkpoint is written
            return ctx.step("export", body)

        def retry(ctx):
            return ctx.step("export", lambda: appended.append("segment-1") or "done")

        self.enqueue()
        with self.assertRaises(SimulatedCrash):
            run_one(self.s1, "w1", ["cpu"], {STAGE: crashing}, 60)
        self.clock.advance(61)
        run_one(self.s2, "w2", ["cpu"], {STAGE: retry}, 60)
        self.assertEqual(appended, ["segment-1", "segment-1"])   # duplicated: checkpoints != exactly-once

    def test_non_idempotent_effect_is_protected_by_ledger_plus_probe(self):
        appended = []                                            # append-only, NOT idempotent

        def make(crash):
            def h(ctx):
                def execute(op_id):
                    appended.append(op_id)
                    if crash:
                        raise SimulatedCrash()
                    return {"op": op_id}
                probe = lambda op_id: {"op": op_id} if op_id in appended else None
                return ctx.external("append_row", {"row": 1}, execute, probe)
            return h

        self.enqueue()
        with self.assertRaises(SimulatedCrash):
            run_one(self.s1, "w1", ["cpu"], {STAGE: make(True)}, 60)
        self.clock.advance(61)
        self.assertEqual(run_one(self.s2, "w2", ["cpu"], {STAGE: make(False)}, 60), "succeeded")
        self.assertEqual(len(appended), 1)

    def test_committed_op_not_reexecuted_when_crash_is_before_step_checkpoint(self):
        job = self.enqueue()
        with self.assertRaises(SimulatedCrash):
            run_one(self.s1, "w1", ["cpu"], {STAGE: self.handler(crash_after_external=True)}, 60)
        self.assertEqual(self.s1.checkpoints(job.id), {})        # step checkpoint never written
        self.clock.advance(61)
        self.assertEqual(run_one(self.s2, "w2", ["cpu"], {STAGE: self.handler()}, 60), "succeeded")
        self.assertEqual(len(self.calls), 1)                     # ledger said committed -> skipped
        self.assertEqual(self.storage.put_calls, 1)

    def test_probe_not_called_on_first_execution(self):
        self.enqueue()
        run_one(self.s1, "w1", ["cpu"], {STAGE: self.handler()}, 60)
        self.assertEqual(self.probes, [])
        self.assertEqual(len(self.calls), 1)

    def test_ordinary_exception_leaves_operation_started_and_retry_succeeds(self):
        state = {"fail": True}

        def h(ctx):
            def execute(op_id):
                self.calls.append(op_id)
                if state["fail"]:
                    raise RuntimeError("storage timeout")
                return {"key": op_id}
            return ctx.external("upload", {"n": 1}, execute)

        self.enqueue()
        self.assertEqual(run_one(self.s1, "w1", ["cpu"], {STAGE: h}), "retryable_failed")
        state["fail"] = False
        self.clock.advance(31)
        self.assertEqual(run_one(self.s2, "w2", ["cpu"], {STAGE: h}), "succeeded")
        self.assertEqual(len(self.calls), 2)
        self.assertEqual(self.calls[0], self.calls[1])

    def test_duplicate_detection_across_different_jobs(self):
        self.enqueue("first")
        run_one(self.s1, "w1", ["cpu"], {STAGE: self.handler()})
        self.enqueue("second")          # a different job, same documentary/stage/operation/inputs
        run_one(self.s2, "w2", ["cpu"], {STAGE: self.handler()})
        self.assertEqual(len(self.calls), 1)
        self.assertEqual(self.storage.put_calls, 1)

    def test_changed_inputs_are_a_new_operation(self):
        self.enqueue("a")
        run_one(self.s1, "w1", ["cpu"], {STAGE: self.handler(inputs={"text": "v1"})})
        self.enqueue("b")
        run_one(self.s1, "w1", ["cpu"], {STAGE: self.handler(inputs={"text": "v2"})})
        self.assertEqual(len(self.calls), 2)
        self.assertNotEqual(self.calls[0], self.calls[1])
        self.assertEqual(self.storage.effects, 2)

    # ---- fencing and conflicts ---------------------------------------------------------
    def test_stale_worker_cannot_begin_or_commit_after_takeover(self):
        self.enqueue()
        old = self.s1.claim("w1", ["cpu"], 60)
        rec = self.s1.begin_operation(old, "w1", "upload", {"n": 1})
        self.clock.advance(61)
        new = self.s2.claim("w2", ["cpu"], 60)
        with self.assertRaises(LeaseLost):
            self.s1.commit_operation(old, "w1", rec.operation_id, {"key": "x"})
        with self.assertRaises(LeaseLost):
            self.s1.begin_operation(old, "w1", "upload", {"n": 1})
        again = self.s2.begin_operation(new, "w2", "upload", {"n": 1})
        self.assertEqual((again.operation_id, again.executions, again.status), (rec.operation_id, 2, "started"))
        self.assertEqual(self.s2.commit_operation(new, "w2", rec.operation_id, {"key": "x"}).status, "committed")

    def test_operations_require_an_active_lease(self):
        job = self.enqueue()
        with self.assertRaises(LeaseLost):
            self.s1.begin_operation(job, "w1", "upload", {})   # never claimed

    def test_recommit_same_output_is_noop_different_output_conflicts(self):
        self.enqueue()
        job = self.s1.claim("w1", ["cpu"])
        rec = self.s1.begin_operation(job, "w1", "upload", {"n": 1})
        self.s1.commit_operation(job, "w1", rec.operation_id, {"key": "a"})
        self.s1.commit_operation(job, "w1", rec.operation_id, {"key": "a"})
        with self.assertRaises(OperationConflict):
            self.s1.commit_operation(job, "w1", rec.operation_id, {"key": "b"})
        self.assertEqual(self.s1.get_operation(rec.operation_id).output, {"key": "a"})

    def test_commit_of_unknown_operation_rejected(self):
        self.enqueue()
        job = self.s1.claim("w1", ["cpu"])
        with self.assertRaises(KeyError):
            self.s1.commit_operation(job, "w1", "op_doesnotexist", {})

    def test_committed_begin_does_not_increment_executions(self):
        self.enqueue()
        job = self.s1.claim("w1", ["cpu"])
        rec = self.s1.begin_operation(job, "w1", "u", {"n": 1})
        self.s1.commit_operation(job, "w1", rec.operation_id, "ok")
        self.assertEqual(self.s1.begin_operation(job, "w1", "u", {"n": 1}).executions, 1)

    # ---- deterministic ids and hashing ------------------------------------------------------
    def test_operation_id_is_deterministic_and_scoped(self):
        h = hash_obj({"a": 1})
        base = make_operation_id(DOC, STAGE, "upload", h)
        self.assertEqual(base, make_operation_id(DOC, STAGE, "upload", h))
        for other in (make_operation_id("other", STAGE, "upload", h),
                      make_operation_id(DOC, "export", "upload", h),
                      make_operation_id(DOC, STAGE, "download", h),
                      make_operation_id(DOC, STAGE, "upload", hash_obj({"a": 2}))):
            self.assertNotEqual(base, other)
        self.assertTrue(base.startswith("op_") and len(base) == 35)

    def test_operation_id_rejects_empty_or_ambiguous_parts(self):
        with self.assertRaises(ValueError):
            make_operation_id("", STAGE, "u", "h")
        with self.assertRaises(ValueError):
            make_operation_id(DOC, "a\x1fb", "u", "h")

    def test_ledger_id_matches_public_helper(self):
        self.enqueue()
        job = self.s1.claim("w1", ["cpu"])
        rec = self.s1.begin_operation(job, "w1", "upload", {"n": 1})
        self.assertEqual(rec.operation_id, make_operation_id(DOC, STAGE, "upload", hash_obj({"n": 1})))

    def test_hash_ignores_key_order_and_rejects_unserialisable(self):
        self.assertEqual(hash_obj({"a": 1, "b": 2}), hash_obj({"b": 2, "a": 1}))
        self.assertEqual(canonical_json({"b": 1, "a": 2}), '{"a":2,"b":1}')
        with self.assertRaises(TypeError):
            hash_obj({"x": b"bytes"})
        with self.assertRaises(ValueError):
            hash_obj({"x": float("inf")})


if __name__ == "__main__":
    unittest.main()
