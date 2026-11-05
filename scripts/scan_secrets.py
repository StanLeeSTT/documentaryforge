#!/usr/bin/env python3
"""Fail (exit 1) if the repo contains secrets, env files, credentials, voice/audio/video/model files.

Default: scan what Git would track (tracked + untracked-not-ignored) when run inside a git
work tree, so a local, gitignored .env does not trip it. --strict scans every file on disk
(used by CI, where the checkout contains only tracked files). Matched text is never printed.
"""
import argparse
import os
import sys

HERE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(HERE, "src"))

from forge.redact import git_file_list, scan_tree  # noqa: E402


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--root", default=HERE, help="directory to scan (default: repository root)")
    ap.add_argument("--strict", action="store_true", help="scan every file on disk, ignore .gitignore")
    args = ap.parse_args(argv)

    files = None
    mode = "strict (all files on disk)"
    if not args.strict:
        files = git_file_list(args.root)
        mode = "git (tracked + untracked-not-ignored)" if files is not None else "strict (no git work tree found)"
    findings = scan_tree(args.root, files)
    print(f"scan_secrets mode: {mode}")
    for f in findings:
        print(f"FOUND  {f}")
    if findings:
        print(f"{len(findings)} finding(s). Matched text is intentionally not shown.")
        return 1
    print("scan_secrets: clean")
    return 0


if __name__ == "__main__":
    sys.exit(main())
