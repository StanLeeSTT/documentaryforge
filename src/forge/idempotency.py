"""Deterministic hashing and operation IDs.

Vocabulary used throughout DocumentaryForge (see docs/DECISIONS.md, D8):

  * EXACTLY-ONCE EXECUTION  - code runs one time, never twice. NOT achievable once a
    process can crash between "the side effect happened" and "we recorded that it
    happened". This project does not claim it.
  * AT-LEAST-ONCE EXECUTION - a step may run again after a crash/timeout. This is what
    leases + retries + checkpoints give you.
  * IDEMPOTENT EXTERNAL OPERATION - running the effect twice leaves the world in the
    same state as running it once (same deterministic key/path, same content hash).
    At-least-once execution + idempotent operations = exactly-once EFFECT.
"""
from __future__ import annotations

import hashlib
import json
from typing import Any


def canonical_json(obj: Any) -> str:
    """Stable JSON: sorted keys, no whitespace, no NaN. Raises TypeError/ValueError
    for values that cannot be represented, so unhashable inputs fail loudly."""
    return json.dumps(obj, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False)


def sha256_hex(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def hash_obj(obj: Any) -> str:
    return sha256_hex(canonical_json(obj))


def make_operation_id(documentary_id: str, stage: str, op_name: str, input_hash: str) -> str:
    """Deterministic ID for one external operation on one exact input.

    Same documentary + stage + operation name + input => same ID, on any worker, in any
    attempt, in any job. Use it as the storage object key / idempotency-key header so a
    repeated execution lands on the same object instead of creating a duplicate.
    Changed input => different ID (a revised script is a new operation, never a silent reuse).
    """
    for part in (documentary_id, stage, op_name, input_hash):
        if not part or "\x1f" in part:
            raise ValueError("operation id parts must be non-empty strings")
    return "op_" + sha256_hex("\x1f".join((documentary_id, stage, op_name, input_hash)))[:32]
