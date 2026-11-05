# SQLite reference engine vs PostgreSQL migration — parity review (Phase 1.1)

**Verification status.** The SQLite engine (`src/forge/jobs.py`) is executed by the test suite.
The PostgreSQL migration (`supabase/migrations/0001_foundation.sql`) has **never been executed**
(no PostgreSQL was available). Its only checks are *static* text comparisons in
`tests/test_parity.py`. **PostgreSQL compatibility is NOT claimed.** Phase 2 must run the migration
in a real Supabase project and replay the same scenarios through the SQL functions.

## Differences found in Phase 1 and what was done

| # | Area | Phase 1 state | Phase 1.1 result |
|---|------|---------------|------------------|
| 1 | Claim ordering | SQLite `priority, seq`; Postgres `priority, created_at` (ties possible, non-deterministic) | **Fixed.** Postgres gets `seq identity`, both order by `priority, seq` |
| 2 | Idempotency | Global unique key in both | **Fixed.** `UNIQUE (documentary_id, stage, idempotency_key)` + `input_hash`; conflict on changed inputs (DF001 / `IdempotencyConflict`) |
| 3 | Dependencies: cycles | Neither engine detected cycles or self-edges | **Fixed.** Python check; Postgres `BEFORE INSERT` trigger with recursive CTE and an advisory lock (static only) |
| 4 | Dependencies: validity | SQLite checked existence in code; Postgres relied on FK; neither blocked cross-documentary edges or edits to started jobs | **Fixed** in both (trigger in Postgres). Error *types* still differ (see below) |
| 5 | Failed/cancelled dependency | Dependents stayed queued forever, silently | **Fixed.** `reconcile` moves dependents to `needs_review` with `blocked_by`; `blocked_jobs()` / view; operator `cancel_job`, `requeue_job` |
| 6 | Documentary FK | Postgres enforced `documentaries(id)`; SQLite did not | **Fixed.** SQLite `documentaries` table + `require_documentary=True` default (legacy tests opt out via `strict=False`) |
| 7 | Constraints | Postgres had status CHECK and `max_attempts >= 1`; SQLite had neither | **Fixed.** Added to SQLite |
| 8 | Retry/backoff | Same formula both | Unchanged; checked statically |
| 9 | Lease expiry | `lease_expires_at < now`, strict `<` in both | Unchanged; checked statically |
| 10 | Heartbeat / checkpoint / complete / fail fencing | Worker AND attempt in both | Unchanged; static check counts 6 fenced operations in each |
| 11 | Late completion | Allowed if lease expired but not reclaimed, both | Unchanged |
| 12 | External operations | Did not exist | **Added** to both (`external_operations`, `begin_operation`, `commit_operation`) |

## Differences that REMAIN (documented, not fixed)

1. **Time source.** SQLite uses an injected clock (tests control it). Postgres uses `now()` (transaction
   start time, database clock). Postgres is better for multi-machine workers (one clock, no skew).
2. **Lost-lease signalling.** SQLite raises `LeaseLost`. Postgres functions return `false`, `null` or
   zero rows. The Phase 2 client wrapper must translate these into `LeaseLost`.
3. **Error redaction.** `JobStore.fail()` redacts before storing; `fail_job()` cannot. The Postgres
   client must call `redact()` first. Not enforceable inside the database.
4. **Error types.** SQLite raises Python exceptions; Postgres raises SQLSTATEs `DF001`–`DF004`
   (unknown job/FK failures may surface as ordinary Postgres errors). The wrapper maps them.
5. **JSON storage.** SQLite stores canonical JSON text. Postgres `jsonb` normalises key order,
   whitespace and numeric formatting. Output hashes are therefore computed **client-side** from canonical
   JSON, never recomputed from a `jsonb` read-back. Very large integers/floats are untested.
6. **Concurrency model.** SQLite serialises writers (`BEGIN IMMEDIATE`). Postgres uses row locks with
   `FOR UPDATE SKIP LOCKED` and an advisory lock for dependency inserts. Real multi-session behaviour
   (READ COMMITTED races, lock waits) is UNTESTED.
7. **`reconcile`.** Python loads the whole table; SQL is set-based with recursive CTEs. Intended to be
   equivalent (root cause = smallest id, C collation); the SQL has not run.
8. **Postgres-only features.** RLS, role grants, `gen_random_uuid()`, `security`/privileges.
   No SQLite equivalent; none tested.

## Phase 2 conformance plan
Run the same scenarios (claim races, expiry, fencing, backoff, dependencies, cycles, blocking, operation
ledger, idempotency conflict) through a Supabase backend and compare with the SQLite results. Any
mismatch is a bug in one of the two and must be fixed before moving on.
