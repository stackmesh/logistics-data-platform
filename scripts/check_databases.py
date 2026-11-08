#!/usr/bin/env python3
"""Check PostgreSQL 17 source and warehouse readiness."""

from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from logistics.readiness import run_checks


def main() -> int:
    report = run_checks()
    return 0 if report.ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
