"""Environment-based settings. Values are never printed or logged."""
from __future__ import annotations

import os
from dataclasses import dataclass, field


class MissingSetting(RuntimeError):
    pass


def require_env(name: str) -> str:
    value = os.environ.get(name, "")
    if not value:
        # Name only - never the value.
        raise MissingSetting(f"Environment variable {name} is not set")
    return value


@dataclass(frozen=True)
class SupabaseSettings:
    url: str
    service_role_key: str = field(repr=False)  # excluded from repr/logs

    @classmethod
    def from_env(cls) -> "SupabaseSettings":
        return cls(require_env("SUPABASE_URL"), require_env("SUPABASE_SERVICE_ROLE_KEY"))
