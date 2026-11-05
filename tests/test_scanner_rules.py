"""Phase 1.1: expanded scanner. Secrets are assembled at runtime so this file never trips the scanner."""
import json
import os
import subprocess
import sys
import tempfile
import unittest

from forge import redact as R
from forge.redact import classify_path, git_file_list, scan_tree

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
SCRIPT = os.path.join(REPO, "scripts", "scan_secrets.py")
HEX32 = "0123456789abcdef" * 2
FAKE_KAGGLE_KEY = "KAGGLE_" + "KEY=" + HEX32
FAKE_KAGGLE_TOKEN = "KAGGLE_API_" + "TOKEN=" + "Zx9" * 10
FAKE_SUPABASE = "SUPABASE_SERVICE_ROLE_" + "KEY=" + "eyJ" + "a" * 12 + "." + "b" * 12 + "." + "c" * 12
FAKE_GENERIC = "GEMINI_API_" + "KEY=" + "q" * 24


def make_tree(root, files):
    for rel, content in files.items():
        full = os.path.join(root, rel)
        os.makedirs(os.path.dirname(full), exist_ok=True)
        mode = "wb" if isinstance(content, bytes) else "w"
        with open(full, mode) as fh:
            fh.write(content)


class PathRuleTests(unittest.TestCase):
    def check(self, names, rule, category):
        for name in names:
            with self.subTest(name=name):
                self.assertEqual(classify_path(name), (rule, category))

    def test_env_files(self):
        self.check([".env", ".env.local", ".env.production", ".env.development", ".env.staging",
                    ".env.test", ".env.backup", "prod.env", ".envrc", "app/.env.local", "web/.ENV.Production"],
                   "forbidden_file", "env_file")

    def test_env_example_is_allowed(self):
        self.assertIsNone(classify_path(".env.example"))
        self.assertIsNone(classify_path("web/.env.example"))

    def test_kaggle_credentials(self):
        self.check(["kaggle.json", "notebooks/kaggle.json"], "forbidden_file", "kaggle_credentials")
        self.check([".kaggle/kaggle.json", ".kaggle/access_token", "home/.kaggle/anything.txt"],
                   "forbidden_directory", "kaggle_credentials")

    def test_audio_and_voice_recordings(self):
        self.check(["voice.wav", "ref.mp3", "a.flac", "b.m4a", "c.ogg", "d.aac", "e.opus", "f.aiff", "g.wma",
                    "VOICE_REFERENCE.WAV", "narration/seg_001.mp3"], "forbidden_file", "audio")
        self.check(["voice_references/notes.txt", "data/reference_audio/readme.md"],
                   "forbidden_directory", "audio")

    def test_video_files(self):
        self.check(["out.mp4", "final.mkv", "a.mov", "b.avi", "c.webm", "d.m4v", "e.wmv", "f.flv", "OUT.MP4"],
                   "forbidden_file", "video")

    def test_model_weights_archives_databases_private_keys(self):
        self.check(["m.pt", "m.pth", "m.ckpt", "m.safetensors", "m.onnx", "m.gguf"], "forbidden_file", "model_weights")
        self.check(["a.zip", "a.tar", "a.gz", "a.tgz", "a.7z", "a.rar"], "forbidden_file", "archive")
        self.check(["a.sqlite", "a.sqlite3", "a.db"], "forbidden_file", "database")
        self.check(["server.pem", "tls.key", "c.p12", "c.pfx", "id_rsa", "id_ed25519"], "forbidden_file", "private_key")

    def test_generated_media_directory(self):
        self.check(["generated_media/x.txt"], "forbidden_directory", "generated_media")

    def test_images_only_allowed_in_docs_and_web_public(self):
        self.check(["frame_001.png", "assets/map.jpg", "scenes/a.JPEG", "out/b.webp"], "forbidden_file", "image")
        for ok in ("docs/diagram.png", "web/public/logo.png", "docs/img/a.jpg"):
            with self.subTest(ok=ok):
                self.assertIsNone(classify_path(ok))

    def test_ordinary_source_files_are_not_flagged(self):
        for ok in ("src/forge/jobs.py", "README.md", "supabase/migrations/0001_foundation.sql",
                   ".github/workflows/ci.yml", "environment.md", "envelope.py"):
            with self.subTest(ok=ok):
                self.assertIsNone(classify_path(ok))

    def test_gitignore_covers_every_forbidden_suffix_and_directory(self):
        with open(os.path.join(REPO, ".gitignore")) as fh:
            lines = set(fh.read().splitlines())
        suffixes = (R.AUDIO_SUFFIXES + R.VIDEO_SUFFIXES + R.WEIGHT_SUFFIXES + R.ARCHIVE_SUFFIXES
                    + R.DATABASE_SUFFIXES + R.KEY_SUFFIXES)
        missing = [f"*{s}" for s in suffixes if f"*{s}" not in lines]
        self.assertEqual(missing, [])
        for needed in (".env", ".env.*", "!.env.example", "*.env", ".envrc", "kaggle.json", ".kaggle/",
                       "voice_references/", "reference_audio/", "generated_media/"):
            self.assertIn(needed, lines)


class ContentRuleTests(unittest.TestCase):
    def findings(self, files):
        with tempfile.TemporaryDirectory() as d:
            make_tree(d, files)
            return scan_tree(d)

    def test_kaggle_and_env_style_assignments_detected(self):
        for label, line in (("kaggle_key", FAKE_KAGGLE_KEY), ("kaggle_token", FAKE_KAGGLE_TOKEN),
                            ("supabase", FAKE_SUPABASE), ("generic", FAKE_GENERIC)):
            with self.subTest(label=label):
                f = self.findings({"notes.txt": f"hello\n{line}\n"})
                self.assertEqual(len(f), 1)
                self.assertEqual((f[0].path, f[0].line), ("notes.txt", 2))

    def test_kaggle_json_content_detected(self):
        body = json.dumps({"username": "someone", "key": HEX32})
        f = self.findings({"creds.txt": body})
        self.assertEqual([(x.rule, x.line) for x in f], [("kaggle_json_key", 1)])

    def test_empty_template_values_are_not_secrets(self):
        f = self.findings({".env.example": "SUPABASE_SERVICE_ROLE_KEY=\nKAGGLE_KEY=\nGEMINI_API_KEY=\n"})
        self.assertEqual(f, [])

    def test_one_finding_per_line_even_if_several_rules_match(self):
        f = self.findings({"a.txt": FAKE_SUPABASE + "\n"})
        self.assertEqual(len(f), 1)

    def test_forbidden_file_content_is_not_read_or_double_reported(self):
        f = self.findings({".env.local": FAKE_KAGGLE_KEY + "\n" + FAKE_GENERIC + "\n"})
        self.assertEqual([(x.path, x.line, x.rule, x.category) for x in f], [(".env.local", 0, "forbidden_file", "env_file")])

    def test_notebook_with_outputs_flagged_clean_notebook_not(self):
        def nb(outputs):
            return json.dumps({"cells": [{"cell_type": "code", "source": ["print(1)"], "outputs": outputs}]})
        dirty = self.findings({"w/dirty.ipynb": nb([{"output_type": "stream", "text": ["hi"]}])})
        self.assertEqual([(x.rule, x.category) for x in dirty], [("notebook_outputs", "notebook")])
        self.assertEqual(self.findings({"w/clean.ipynb": nb([])}), [])

    def test_notebook_secret_in_source_cell_still_found(self):
        nbk = json.dumps({"cells": [{"cell_type": "code", "source": [FAKE_KAGGLE_KEY], "outputs": []}]})
        f = self.findings({"n.ipynb": nbk})
        self.assertEqual([x.rule for x in f], ["env_assignment_secret"])

    def test_redact_covers_env_assignments(self):
        out = R.redact(f"failed with {FAKE_GENERIC} while calling")
        self.assertNotIn("q" * 24, out)
        self.assertIn("[REDACTED]", out)


class NeverPrintSecretsTests(unittest.TestCase):
    def run_script(self, root, *extra):
        return subprocess.run([sys.executable, SCRIPT, "--root", root, *extra],
                              capture_output=True, text=True, timeout=60)

    def test_script_output_contains_no_secret_text(self):
        with tempfile.TemporaryDirectory() as d:
            make_tree(d, {
                "leak1.txt": FAKE_KAGGLE_KEY + "\n",
                "leak2.txt": FAKE_SUPABASE + "\n",
                ".env.production": FAKE_GENERIC + "\n",
                "voice.wav": b"RIFF....",
                "render.mp4": b"\x00\x00\x00",
                "clip.mkv": b"\x1aE\xdf\xa3",
                ".kaggle/kaggle.json": json.dumps({"username": "u", "key": HEX32}),
            })
            proc = self.run_script(d, "--strict")
        self.assertEqual(proc.returncode, 1)
        out = proc.stdout + proc.stderr
        for secret in (HEX32, "q" * 24, "Zx9" * 10, "eyJ" + "a" * 12, "RIFF"):
            self.assertNotIn(secret, out)
        for expected in ("leak1.txt:1", "leak2.txt:1", ".env.production", "voice.wav", "render.mp4", "clip.mkv", "kaggle.json"):
            self.assertIn(expected, out)
        self.assertIn("7 finding(s)", out)

    def test_script_clean_tree_exits_zero(self):
        with tempfile.TemporaryDirectory() as d:
            make_tree(d, {"a.py": "x = 1\n", ".env.example": "KEY=\n"})
            proc = self.run_script(d, "--strict")
        self.assertEqual((proc.returncode, "clean" in proc.stdout), (0, True))

    def test_repository_is_clean_in_strict_mode(self):
        proc = subprocess.run([sys.executable, SCRIPT, "--strict"], capture_output=True, text=True, timeout=60)
        self.assertEqual((proc.returncode, proc.stdout.strip().splitlines()[-1]), (0, "scan_secrets: clean"))


def _git_ok() -> bool:
    try:
        subprocess.run(["git", "--version"], capture_output=True, check=True)
        return True
    except (OSError, subprocess.SubprocessError):
        return False


@unittest.skipUnless(_git_ok(), "git not installed - UNVERIFIED")
class GitModeTests(unittest.TestCase):
    def test_git_mode_ignores_gitignored_local_files_but_strict_does_not(self):
        with tempfile.TemporaryDirectory() as d:
            subprocess.run(["git", "init", "-q", d], check=True)
            make_tree(d, {".gitignore": ".env.local\n*.wav\n", ".env.local": FAKE_GENERIC + "\n",
                          "voice.wav": b"RIFF", "ok.py": "x = 1\n"})
            files = git_file_list(d)
            self.assertIsNotNone(files)
            self.assertEqual(scan_tree(d, files), [])                          # ignored -> cannot enter Git
            self.assertEqual({f.path for f in scan_tree(d)}, {".env.local", "voice.wav"})  # strict sees them

    def test_git_mode_flags_untracked_unignored_leaks_and_staged_files(self):
        with tempfile.TemporaryDirectory() as d:
            subprocess.run(["git", "init", "-q", d], check=True)
            make_tree(d, {".gitignore": "", "leak.mp4": b"\x00", ".env.production": "A=1\n"})
            subprocess.run(["git", "-C", d, "add", ".env.production"], check=True)  # staged
            found = {f.path for f in scan_tree(d, git_file_list(d))}
            self.assertEqual(found, {"leak.mp4", ".env.production"})

    def test_script_default_mode_uses_git(self):
        with tempfile.TemporaryDirectory() as d:
            subprocess.run(["git", "init", "-q", d], check=True)
            make_tree(d, {".gitignore": "*.wav\n", "voice.wav": b"RIFF", "a.py": "x=1\n"})
            proc = subprocess.run([sys.executable, SCRIPT, "--root", d], capture_output=True, text=True)
            self.assertEqual(proc.returncode, 0)
            self.assertIn("git (tracked", proc.stdout)

    def test_git_file_list_returns_none_outside_a_repo(self):
        with tempfile.TemporaryDirectory() as d:
            self.assertIsNone(git_file_list(d))


if __name__ == "__main__":
    unittest.main()
