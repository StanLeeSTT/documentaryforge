import logging
import os
import tempfile
import unittest

from forge.config import MissingSetting, SupabaseSettings, require_env
from forge.redact import RedactingFilter, redact, scan_tree
from forge.worker import run_one
from tests.helpers import FakeClock, SharedDB

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
# Fake secrets are assembled at runtime so this file itself never trips the scanner.
FAKE_GROQ = "gsk_" + "a" * 30
FAKE_JWT = "eyJ" + "a" * 12 + "." + "b" * 12 + "." + "c" * 12
FAKE_GOOGLE = "AIza" + "x" * 35


class SecurityTests(unittest.TestCase):
    def test_redact_removes_known_secrets(self):
        text = f"call failed key={FAKE_GROQ} jwt={FAKE_JWT} g={FAKE_GOOGLE}"
        out = redact(text)
        for secret in (FAKE_GROQ, FAKE_JWT, FAKE_GOOGLE):
            self.assertNotIn(secret, out)
        self.assertIn("[REDACTED]", out)

    def test_logging_filter_redacts(self):
        logger = logging.getLogger("forge.test.redact")
        logger.propagate = False
        records = []

        class Capture(logging.Handler):
            def emit(self, record):
                records.append(record.getMessage())

        h = Capture()
        h.addFilter(RedactingFilter())
        logger.addHandler(h)
        logger.error("token %s leaked", FAKE_GROQ)
        self.assertNotIn(FAKE_GROQ, records[0])

    def test_stored_job_errors_are_redacted(self):
        clock = FakeClock()
        db = SharedDB(clock)
        try:
            s = db.store()
            job = s.enqueue(documentary_id="d", stage="x", idempotency_key="k")

            def leaky(ctx):
                raise RuntimeError(f"upstream said invalid key {FAKE_GROQ}")

            run_one(s, "w", ["cpu"], {"x": leaky})
            stored = s.get(job.id).last_error
            self.assertNotIn(FAKE_GROQ, stored)
            self.assertIn("[REDACTED]", stored)
        finally:
            db.cleanup()

    def test_scanner_flags_secret_without_echoing_it(self):
        with tempfile.TemporaryDirectory() as d:
            with open(os.path.join(d, "a.py"), "w") as fh:
                fh.write(f'KEY = "{FAKE_GROQ}"\n')
            findings = scan_tree(d)
            self.assertEqual([(f.path, f.line, f.rule) for f in findings], [("a.py", 1, "groq_key")])
            self.assertNotIn(FAKE_GROQ, str(findings[0]))

    def test_scanner_flags_env_and_voice_files(self):
        with tempfile.TemporaryDirectory() as d:
            for name in (".env", "my_voice.wav", "kaggle.json"):
                open(os.path.join(d, name), "w").close()
            rules = {f.rule for f in scan_tree(d)}
            self.assertEqual(rules, {"forbidden_file"})
            self.assertEqual(len(scan_tree(d)), 3)

    def test_repository_itself_is_clean(self):
        self.assertEqual([str(f) for f in scan_tree(REPO)], [])

    def test_gitignore_blocks_env_and_audio(self):
        with open(os.path.join(REPO, ".gitignore")) as fh:
            lines = fh.read().splitlines()
        for needed in (".env", "kaggle.json", "*.wav", "!.env.example"):
            self.assertIn(needed, lines)

    def test_env_example_has_no_values(self):
        with open(os.path.join(REPO, ".env.example")) as fh:
            for line in fh:
                if "=" in line and not line.startswith("#"):
                    self.assertEqual(line.split("=", 1)[1].strip(), "", line)

    def test_missing_env_reports_name_only(self):
        os.environ.pop("FORGE_TEST_MISSING", None)
        with self.assertRaises(MissingSetting) as cm:
            require_env("FORGE_TEST_MISSING")
        self.assertIn("FORGE_TEST_MISSING", str(cm.exception))

    def test_settings_repr_hides_key(self):
        s = SupabaseSettings("https://example.invalid", FAKE_JWT)
        self.assertNotIn(FAKE_JWT, repr(s))


if __name__ == "__main__":
    unittest.main()
