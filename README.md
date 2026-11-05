# DocumentaryForge — Phase 1.1: foundation, job recovery, hardening

Evidence-First AI Documentary Factory. Project ID `documentaryforge_001`. First test documentary:
`indus_valley_001` (The Lost Civilization of the Indus Valley). The Indus Valley is a topic, not the product.

## What exists
- `src/forge/jobs.py` — job engine (SQLite reference): scoped idempotent enqueue, leases, fencing, retries with
  backoff, DAG dependencies, failed-dependency blocking, operator cancel/requeue, checkpoints, external-operation ledger
- `src/forge/worker.py` — `run_one()`, resumable `ctx.step()`, safe side effects via `ctx.external()`
- `src/forge/idempotency.py` — canonical hashing and deterministic operation IDs
- `src/forge/redact.py`, `scripts/scan_secrets.py` — redaction and repository scanner (never prints matches)
- `supabase/migrations/0001_foundation.sql` — the same engine for PostgreSQL/Supabase. **Never executed; untested.**
- `tests/` — standard-library `unittest`, no installs, no accounts
- `docs/` — `DECISIONS.md`, `PARITY.md`, `PHASE_1_1_CORRECTIONS.md`, `TEST_RESULTS.md`

## Run
```
python -m unittest discover -s tests -t . -v
python scripts/scan_secrets.py --strict
```
CI (`.github/workflows/ci.yml`) runs both on every push. It has not run on GitHub yet.

## Using the job engine
```python
from forge.jobs import JobStore
from forge.worker import run_one

store = JobStore("jobs.sqlite3")                       # require_documentary=True by default
store.register_documentary("indus_valley_001", "The Lost Civilization of the Indus Valley")
plan = store.enqueue(documentary_id="indus_valley_001", stage="plan", idempotency_key="plan:v1")

def narrate(ctx):
    # external effect: use the deterministic operation id as the object key
    return ctx.external("upload_audio", {"text": "..."}, execute=lambda op_id: upload(op_id), probe=lambda op_id: head(op_id))

run_one(store, "worker-1", ["cpu"], {"plan": lambda ctx: "ok"})
```
(`upload` / `head` are your storage calls; they do not exist yet.)

## Guarantees (read this)
At-least-once execution; exactly-once *effect* only for idempotent operations. Exactly-once execution is not claimed.
See `docs/DECISIONS.md` D8.

## Not built yet
Supabase connection, LLM router, research, verification, voice, Kaggle, video.
