# Architecture decisions (Phase 1)

**D1. Prove job recovery locally before touching any service.** The job engine is
implemented twice with identical rules: SQLite (`src/forge/jobs.py`, tested) and
Postgres functions (`supabase/migrations/0001_foundation.sql`, NOT yet executed).
Phase 2 must re-run the same scenarios against real Supabase.

**D2. Fencing by attempt number.** Every claim increments `attempts`. Heartbeat,
checkpoint, complete and fail only succeed for the matching worker AND attempt. A
stalled worker whose job was taken over gets `LeaseLost` and cannot overwrite results.

**D3. Late completion is allowed** if the lease expired but nobody reclaimed the job.
This saves work and is still safe because of D2.

**D4. Retries are bounded.** `max_attempts` (default 3) with exponential backoff
(30s, 60s, 120s... capped at 1h). Exhausted jobs go to `dead_letter` for human review.
Crashes (lease expiry) consume attempts the same way as errors.

**D5. Checkpoint = one row per (job, step).** Handlers use `ctx.step(name, fn)`;
a step with a stored result is skipped. Step results must be small JSON. Large
artifacts (audio, documents) go to Supabase Storage and the checkpoint stores only
the path and hash.

**D6. No external service is required to run the tests.** Standard library only.

**D7. Secrets.** Scanner and redaction run in CI and on every stored error message.
Voice/media files and `.env` are blocked by `.gitignore` and the scanner.

**D8. Delivery guarantees, stated precisely.**
- *Exactly-once execution*: NOT achievable and NOT claimed. A process can die between "the effect
  happened" and "we recorded it".
- *At-least-once execution*: what leases + retries + checkpoints give. A step may run again.
- *Idempotent external operation*: repeating it leaves the world as if it ran once (same deterministic
  key/path, same bytes). At-least-once execution + idempotent operation = exactly-once **effect**.
Checkpoints alone are therefore not enough for uploads, downloads, TTS calls or exports. Use
`ctx.external(name, inputs, execute, probe)`: a ledger row keyed by a deterministic operation ID
(documentary + stage + op name + hash of inputs) records begin/commit. A committed operation is never
re-executed (even by another job). After a crash, an optional `probe` can detect the effect in the
external system; without one, `execute` runs again and MUST be idempotent under the operation ID.
Changed inputs produce a new operation ID (a revised script is a new operation).

**D9. Job identity is scoped.** `(documentary_id, stage, idempotency_key)` is unique, with an
`input_hash` of `{payload, capability}`. Same identity + same inputs returns the existing job; same
identity + different inputs raises `IdempotencyConflict` (use a new key, e.g. include a version).
`depends_on` is applied only when a job is created.

**D10. Dependencies form a DAG.** Self-edges, cycles, cross-documentary edges and edits to started jobs
are rejected. Postgres enforces this in a trigger guarded by an advisory lock (static check only).

**D11. Nothing may be silently stuck.** A job with a transitive dependency that is permanently_failed,
dead_letter or cancelled becomes `needs_review` with `blocked_by` = root cause, and is listed by
`blocked_jobs()` / the `blocked_jobs` view. Operators use `cancel_job` or `requeue_job`; requeue never
resets `attempts` (it is the fencing token) and instead sets `max_attempts = attempts + extra`.
Jobs waiting on a dependency that is merely `needs_review` (human review) wait visibly behind it.

**D12. Scanner scope.** Default mode scans what Git would track (tracked + untracked-not-ignored) so a
local gitignored `.env` does not fail a developer's run; CI uses `--strict` (every file on disk).
Findings never include matched text and forbidden files are never read.

**D13. PostgreSQL is unverified.** See `docs/PARITY.md`.

## Known limitations
- Jobs waiting on a dependency in `needs_review` (human review) wait until a human acts; `blocked_jobs()`
  shows the review job and `since` so a dashboard can flag stale ones. No automatic escalation yet.
- `needs_review` is set only by dependency blocking; no worker API requests a review yet (Phase 7).
- `JobStore._reconcile` rescans the whole table (fine for the reference engine, not for scale).
- The Postgres functions return false/null/zero rows on lease loss; the Supabase client wrapper
  (Phase 2) must convert that into `LeaseLost`, redact errors before `fail_job`, and map SQLSTATEs DF001-DF004.
- Redaction is pattern-based; unknown key formats will not be caught.
- `ctx.step` results and `ctx.external` outputs must be small JSON; large artifacts go to storage and the
  record keeps only the path and hash.
- The ledger gives exactly-once *effect* only for idempotent operations or a reliable `probe`.
