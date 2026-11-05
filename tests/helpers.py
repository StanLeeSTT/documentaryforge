import os
import tempfile

from forge.jobs import JobStore


class FakeClock:
    def __init__(self, t: float = 1_000_000.0):
        self.t = t

    def __call__(self) -> float:
        return self.t

    def advance(self, seconds: float) -> None:
        self.t += seconds


class SharedDB:
    """A temp-file SQLite DB so several JobStore connections (= several workers) share state."""

    def __init__(self, clock: FakeClock):
        self._dir = tempfile.TemporaryDirectory()
        self.path = os.path.join(self._dir.name, "jobs.sqlite3")
        self.clock = clock
        self._stores: list[JobStore] = []

    def store(self, track: bool = True, strict: bool = False) -> JobStore:
        """track=False for stores created inside threads; the thread must close its own.
        strict=False keeps the original Phase 1 tests valid: they enqueue for documentaries that were
        never registered. strict=True mirrors PostgreSQL's foreign key (the production behaviour)."""
        s = JobStore(self.path, clock=self.clock, require_documentary=strict)
        if track:
            self._stores.append(s)
        return s

    def cleanup(self) -> None:
        for s in self._stores:
            s.close()
        self._dir.cleanup()
