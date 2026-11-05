# Phase 1.1 hardening — corrections made

Existing architecture, repository layout, SQLite engine, worker, checkpoints, leases, retries,
migration file, test approach and CI workflow were **kept**. Changes are additive or targeted.
All 35 original tests were kept unchanged (only `tests/helpers.py` gained an opt-out flag and
`tests/test_structure.py` was updated for the new tables/functions).

| ID | Weakness | Correction | Where | Tests |
|----|----------|------------|-------|-------|
| C1 | A checkpoint cannot make an external effect exactly-once: a crash after the effect but before the checkpoint repeats it | Durable **external-operation ledger** with deterministic operation IDs (`documentary + stage + op + hash(inputs)`), input hashing, committed-result reuse, optional `probe`, fenced begin/commit; `ctx.external()` | `idempotency.py`, `jobs.py`, `worker.py`, SQL | `test_operations.py` (18) |
| C2 | Docs blurred exactly-once vs at-least-once | Explicit vocabulary and guarantees; a test that **demonstrates** the duplicate under a plain checkpoint | `idempotency.py` docstring, D8, test | `test_checkpoint_alone_is_at_least_once_not_exactly_once` |
| C3 | Cycles and self-dependencies were possible via later edits | `add_dependency` with cycle/self/cross-documentary/started-job checks; Postgres trigger + advisory lock | `jobs.py`, SQL | `test_dependencies.py` |
| C4 | Failed or cancelled dependencies left dependents stuck silently | `needs_review` + `blocked_by` (transitive), `blocked_jobs()` and view, `cancel_job`, `requeue_job` (never resets `attempts`) | `jobs.py`, SQL | `test_dependencies.py` |
| C5 | Idempotency key was global and could return an unrelated job | Identity `(documentary_id, stage, idempotency_key)` + `input_hash`; changed inputs raise `IdempotencyConflict` | `jobs.py`, SQL | `test_idempotency_scope.py` |
| C6 | Scanner missed `.env.local`, `.env.production`, Kaggle creds, video, model weights, etc. | Rewritten path classifier + content rules + notebook-output check; git-aware mode and `--strict`; still never prints matches | `redact.py`, `scripts/scan_secrets.py`, `.gitignore`, CI | `test_scanner_rules.py` (25) |
| C7 | Non-deterministic Postgres claim ordering | `seq` identity column; order by `priority, seq` | SQL | `test_parity.py` |
| C8 | SQLite lacked documentary FK and CHECK constraints | Added `documentaries`, status/`max_attempts` checks, schema version guard | `jobs.py` | `test_idempotency_scope.py` |
| C9 | Lease length of 0 or negative accepted | Validation | `jobs.py` | `test_idempotency_scope.py` |
| C10 | Postgres/SQLite drift risk | Static parity checks + `docs/PARITY.md` | tests, docs | `test_parity.py` (12) |

## Mutation check (does the suite actually catch regressions?)
Ten deliberate bugs were injected into a scratch copy; the suite failed for all ten:
unfenced `commit_operation`; cycle check removed; no blocking on failure; unscoped identity; ledger
skip disabled; probe disabled; `.env.*` rule removed; scanner printing matched content; `attempts`
reset on requeue; fencing removed from `fail`.

## Changes you may trip over
- `JobStore` now defaults to `require_documentary=True`: call `register_documentary()` first.
- The SQLite schema version is 2; an old Phase 1 database file is refused, not migrated.
- `0001_foundation.sql` was edited in place because it has never been applied anywhere. After it is applied
  to any real project, all further changes must be new migration files.
- `enqueue_job` has a new required argument `p_input_hash`.
