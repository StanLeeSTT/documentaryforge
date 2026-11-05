# Test results (Phase 1.1)

Command: `python -m unittest discover -s tests -t . -v` (Python 3.12.3, SQLite 3.45.1, git 2.43.0, no network)
Result: **Ran 130 tests — 130 passed, 0 failed, 0 errors, 0 skipped.**
Secret scan: `python scripts/scan_secrets.py --strict` -> `scan_secrets: clean`.

| Module | Tests | Origin |
|---|---|---|
| test_jobs | 16 | original Phase 1 (unchanged) |
| test_recovery | 7 | original Phase 1 (unchanged) |
| test_security | 10 | original Phase 1 (unchanged) |
| test_structure | 2 | original, updated for new tables/functions/files |
| test_idempotency_scope | 20 | new |
| test_operations | 18 | new |
| test_dependencies | 20 | new |
| test_scanner_rules | 25 | new |
| test_parity | 12 | new (static text checks only) |
| **Total** | **130** | 35 original + 95 new |

Mutation check: 10 injected bugs, 10 caught (see `PHASE_1_1_CORRECTIONS.md`).

## NOT verified (UNTESTED)
- `supabase/migrations/0001_foundation.sql` has never been executed. `test_parity` and `test_structure` only
  compare its text. No PostgreSQL behaviour is verified: claim races under row locks, `SKIP LOCKED`, the
  recursive CTEs, the dependency trigger, advisory lock, RLS, role grants, `jsonb` round-trips.
- `.github/workflows/ci.yml` has never run on GitHub.
- Supabase (database, storage, auth), Vercel, GitHub Actions dispatch, Kaggle, LLM providers: no service was
  contacted. Nothing in this phase needs them.
- Multi-process and multi-machine concurrency: only threads on one machine (SQLite) were exercised.
- Real crashes: killed processes are simulated by raising a `BaseException`, not by killing a process.
- Clock skew between machines, long-running leases and real backoff timing (a fake clock is used).
- The 6 git-mode scanner tests ran because git was installed here; they would be skipped, not failed, without git.
