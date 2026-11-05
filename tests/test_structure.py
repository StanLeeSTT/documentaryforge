import os
import re
import unittest

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
REQUIRED = [
    "README.md", ".gitignore", ".env.example", "docs/DECISIONS.md",
    "supabase/migrations/0001_foundation.sql", "scripts/scan_secrets.py",
    "src/forge/jobs.py", "src/forge/worker.py", "src/forge/redact.py", "src/forge/config.py",
    ".github/workflows/ci.yml",
    "src/forge/idempotency.py", "docs/PARITY.md", "docs/PHASE_1_1_CORRECTIONS.md",
]


class StructureTests(unittest.TestCase):
    def test_required_files_exist(self):
        missing = [p for p in REQUIRED if not os.path.exists(os.path.join(REPO, p))]
        self.assertEqual(missing, [])

    def test_sql_defines_all_tables_and_functions(self):
        with open(os.path.join(REPO, "supabase/migrations/0001_foundation.sql")) as fh:
            sql = fh.read().lower()
        for table in ("documentaries", "pipeline_jobs", "job_dependencies", "job_attempts",
                      "job_checkpoints", "external_operations", "workers", "audit_events"):
            self.assertRegex(sql, rf"create table (if not exists )?public\.{table}\b")
        for fn in ("enqueue_job", "claim_job", "heartbeat_job", "checkpoint_job", "complete_job", "fail_job",
                   "add_dependency", "cancel_job", "requeue_job", "begin_operation", "commit_operation",
                   "reconcile_blocked_jobs"):
            self.assertRegex(sql, rf"create or replace function public\.{fn}\b")
        self.assertEqual(len(re.findall(r"enable row level security", sql)), 8)


if __name__ == "__main__":
    unittest.main()
