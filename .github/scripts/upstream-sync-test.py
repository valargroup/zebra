#!/usr/bin/env python3
"""Offline fixture checks for the upstream sync discovery script."""

from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]


def main() -> int:
    output_dir = ROOT / ".github" / "upstream-sync" / "work-fixture-test"
    fixture = ROOT / ".github" / "upstream-sync" / "fixtures" / "current-compare.json"
    if output_dir.exists():
        subprocess.check_call(["rm", "-rf", str(output_dir)])

    subprocess.check_call(
        [
            sys.executable,
            str(ROOT / ".github" / "scripts" / "upstream-sync-discover.py"),
            "--fixture",
            str(fixture),
            "--output-dir",
            str(output_dir),
            "--limit",
            "1",
        ],
        cwd=ROOT,
    )

    candidate = json.loads((output_dir / "candidate.json").read_text(encoding="utf-8"))
    assert candidate["status"] == "candidate"
    assert candidate["source_pr"] == 10676
    assert candidate["source_merge_commit"].startswith("8ead00cab")
    assert candidate["branch_name"] == "adam/upstream-pr-10676"

    subprocess.check_call(["rm", "-rf", str(output_dir)])
    print("OK: fixture discovery selects upstream PR 10676")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
