"""Secret redaction and repository scanning.

Rules enforced here:
  * log/error text is redacted before storage or printing;
  * the scanner reports path, line number, rule and category - NEVER the matched text;
  * files that must never enter Git (env files, credentials, voice/audio, video, model weights,
    archives, databases, private keys, notebook outputs) are reported by name/type alone.
"""
from __future__ import annotations

import json
import logging
import os
import re
import subprocess
from dataclasses import dataclass
from typing import Iterable, Iterator, Optional

# (rule name, compiled pattern). Specific rules come first; the generic assignment rule is last.
# Only the FIRST matching rule is reported per line.
RULES: tuple[tuple[str, re.Pattern[str]], ...] = (
    ("jwt", re.compile(r"eyJ[A-Za-z0-9_-]{10,}\.[A-Za-z0-9_-]{10,}\.[A-Za-z0-9_-]{10,}")),
    ("supabase_secret_key", re.compile(r"sb_secret_[A-Za-z0-9_-]{10,}")),
    ("google_api_key", re.compile(r"AIza[0-9A-Za-z_-]{35}")),
    ("groq_key", re.compile(r"gsk_[A-Za-z0-9]{20,}")),
    ("openai_style_key", re.compile(r"sk-[A-Za-z0-9_-]{20,}")),
    ("github_token", re.compile(r"(?:ghp|gho|ghs|ghu)_[A-Za-z0-9]{30,}|github_pat_[A-Za-z0-9_]{30,}")),
    ("huggingface_token", re.compile(r"hf_[A-Za-z0-9]{30,}")),
    ("freellmapi_key", re.compile(r"freellmapi-[A-Za-z0-9]{8,}")),
    ("bearer_token", re.compile(r"Bearer\s+[A-Za-z0-9._~+/=-]{20,}")),
    ("private_key_block", re.compile(r"-----BEGIN [A-Z ]*PRIVATE KEY-----")),
    ("kaggle_json_key", re.compile(r"\"key\"\s*:\s*\"[0-9a-f]{32}\"")),
    # KAGGLE_KEY=<32 hex>, KAGGLE_API_TOKEN=<value>, SUPABASE_SERVICE_ROLE_KEY=<value>, FOO_API_KEY=<value> ...
    ("env_assignment_secret", re.compile(
        r"\b[A-Z][A-Z0-9_]*(?:_KEY|API_TOKEN|_TOKEN|_SECRET|SECRET|_PASSWORD|PASSWORD)\s*[=:]\s*[\"']?[A-Za-z0-9/+_.\-]{20,}")),
)

AUDIO_SUFFIXES = (".wav", ".mp3", ".flac", ".m4a", ".ogg", ".aac", ".opus", ".aiff", ".wma")
VIDEO_SUFFIXES = (".mp4", ".mkv", ".mov", ".avi", ".webm", ".m4v", ".wmv", ".flv")
WEIGHT_SUFFIXES = (".pt", ".pth", ".ckpt", ".safetensors", ".onnx", ".gguf")
ARCHIVE_SUFFIXES = (".zip", ".tar", ".gz", ".tgz", ".7z", ".rar")
DATABASE_SUFFIXES = (".sqlite", ".sqlite3", ".db")
KEY_SUFFIXES = (".pem", ".key", ".p12", ".pfx")
# Images are allowed only in documentation/branding locations (generated frames/assets must not be committed).
IMAGE_SUFFIXES = (".png", ".jpg", ".jpeg", ".gif", ".webp", ".bmp", ".tif", ".tiff")
IMAGE_ALLOWED_PREFIXES = ("docs/", "web/public/")

SUFFIX_CATEGORIES: tuple[tuple[tuple[str, ...], str], ...] = (
    (AUDIO_SUFFIXES, "audio"),
    (VIDEO_SUFFIXES, "video"),
    (WEIGHT_SUFFIXES, "model_weights"),
    (ARCHIVE_SUFFIXES, "archive"),
    (DATABASE_SUFFIXES, "database"),
    (KEY_SUFFIXES, "private_key"),
)
# Back-compat names used by earlier code/tests.
FORBIDDEN_FILE_SUFFIXES = AUDIO_SUFFIXES + VIDEO_SUFFIXES + WEIGHT_SUFFIXES + ARCHIVE_SUFFIXES + DATABASE_SUFFIXES + KEY_SUFFIXES
FORBIDDEN_FILE_NAMES = (".env", "kaggle.json")

ALLOWED_ENV_TEMPLATES = {".env.example"}
PRIVATE_KEY_NAMES = {"id_rsa", "id_dsa", "id_ecdsa", "id_ed25519"}
FORBIDDEN_DIRS = {
    ".kaggle": "kaggle_credentials",
    "voice_references": "audio",
    "reference_audio": "audio",
    "generated_media": "generated_media",
}
SKIP_DIRS = {".git", "__pycache__", ".venv", "node_modules"}
MAX_SCAN_BYTES = 5 * 1024 * 1024

REDACTED = "[REDACTED]"


def redact(text: str) -> str:
    """Replace anything matching a known secret pattern."""
    for _name, pattern in RULES:
        text = pattern.sub(REDACTED, text)
    return text


class RedactingFilter(logging.Filter):
    """Attach to a logger/handler so secrets never reach log output."""

    def filter(self, record: logging.LogRecord) -> bool:
        record.msg = redact(str(record.getMessage()))
        record.args = ()
        return True


@dataclass(frozen=True)
class Finding:
    path: str
    line: int          # 0 for file-level findings
    rule: str
    category: str = ""

    def __str__(self) -> str:  # never includes matched text
        where = self.path if self.line == 0 else f"{self.path}:{self.line}"
        label = f"{self.rule}:{self.category}" if self.category else self.rule
        return f"{where}  [{label}]"


def classify_path(rel_path: str) -> Optional[tuple[str, str]]:
    """Return (rule, category) if a file at this repo-relative path must never enter Git."""
    norm = rel_path.replace(os.sep, "/")
    parts = norm.split("/")
    name = parts[-1].lower()
    for directory in parts[:-1]:
        if directory.lower() in FORBIDDEN_DIRS:
            return "forbidden_directory", FORBIDDEN_DIRS[directory.lower()]
    if name == ".env" or name == ".envrc" or name.endswith(".env") or (
            name.startswith(".env.") and name not in ALLOWED_ENV_TEMPLATES):
        return "forbidden_file", "env_file"
    if name == "kaggle.json":
        return "forbidden_file", "kaggle_credentials"
    if name in PRIVATE_KEY_NAMES:
        return "forbidden_file", "private_key"
    for suffixes, category in SUFFIX_CATEGORIES:
        if name.endswith(suffixes):
            return "forbidden_file", category
    if name.endswith(IMAGE_SUFFIXES) and not norm.lower().startswith(IMAGE_ALLOWED_PREFIXES):
        return "forbidden_file", "image"
    return None


def _notebook_has_outputs(path: str) -> bool:
    try:
        with open(path, "r", encoding="utf-8") as fh:
            nb = json.load(fh)
    except (OSError, ValueError):
        return False
    return any(cell.get("outputs") for cell in nb.get("cells", []) if isinstance(cell, dict))


def git_file_list(root: str) -> Optional[list[str]]:
    """Files git would track or add (tracked + untracked-not-ignored). None if not a git work tree."""
    try:
        out = subprocess.run(
            ["git", "-C", root, "ls-files", "-z", "--cached", "--others", "--exclude-standard"],
            capture_output=True, check=True, timeout=30)
    except (OSError, subprocess.SubprocessError):
        return None
    return [p for p in out.stdout.decode("utf-8", "replace").split("\0") if p]


def walk_files(root: str) -> list[str]:
    found: list[str] = []
    for dirpath, dirnames, filenames in os.walk(root):
        dirnames[:] = [d for d in dirnames if d not in SKIP_DIRS]
        for name in filenames:
            found.append(os.path.relpath(os.path.join(dirpath, name), root))
    return found


def scan_tree(root: str, files: Optional[Iterable[str]] = None) -> list[Finding]:
    """Scan every file under root (strict), or only `files` (repo-relative) when given."""
    return list(_scan(root, list(files) if files is not None else walk_files(root)))


def _scan(root: str, rel_files: list[str]) -> Iterator[Finding]:
    for rel in sorted(rel_files):
        full = os.path.join(root, rel)
        if not os.path.isfile(full):
            continue
        verdict = classify_path(rel)
        if verdict is not None:
            yield Finding(rel, 0, verdict[0], verdict[1])
            continue  # never read the content of a forbidden file
        if rel.lower().endswith(".ipynb") and _notebook_has_outputs(full):
            yield Finding(rel, 0, "notebook_outputs", "notebook")
        try:
            if os.path.getsize(full) > MAX_SCAN_BYTES:
                continue
            with open(full, "r", encoding="utf-8") as fh:
                for lineno, line in enumerate(fh, start=1):
                    for rule, pattern in RULES:
                        if pattern.search(line):
                            yield Finding(rel, lineno, rule, "secret")
                            break
        except (UnicodeDecodeError, OSError):
            continue  # binary or unreadable files are not text-scanned
