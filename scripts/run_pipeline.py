"""Run the platform end to end, or one part of it.

    python -m scripts.run_pipeline                     # full: health checks .. ETL .. mart .. ML .. report
    python -m scripts.run_pipeline --mode etl
    python -m scripts.run_pipeline --mode backfill --start-date 2026-09-01 --end-date 2026-09-30
    python -m scripts.run_pipeline --mode score --dry-run

Exit code 0: every task succeeded or was skipped by design. 1: a task failed or did not run. 2: usage or
configuration error.
"""

from __future__ import annotations

import argparse
import sys
from datetime import date
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from logistics.settings import SettingsError  # noqa: E402
from orchestration.dag import plan  # noqa: E402
from orchestration.tasks import MODES, TASKS  # noqa: E402

WINDOW_MODES = {"etl": "optional", "backfill": "required"}


def _date(value: str) -> date:
    try:
        return date.fromisoformat(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError(f"expected YYYY-MM-DD, got {value!r}") from exc


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="python -m scripts.run_pipeline", description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--mode", choices=sorted(MODES), default="full")
    parser.add_argument("--start-date", type=_date, help="first loading_date (etl, backfill)")
    parser.add_argument("--end-date", type=_date, help="last loading_date, inclusive (etl, backfill)")
    parser.add_argument("--force-mart", action="store_true", help="rebuild the mart even if its inputs are unchanged")
    parser.add_argument("--dry-run", action="store_true", help="print the task order and exit")
    return parser


def parse(argv: list[str] | None = None):
    from etl.config import DateWindow

    parser = build_parser()
    args = parser.parse_args(argv)
    has_window = args.start_date is not None or args.end_date is not None
    rule = WINDOW_MODES.get(args.mode)
    if rule is None and has_window:
        parser.error(f"--mode {args.mode} loads no window; --start-date/--end-date apply to etl and backfill")
    if rule == "required" and not (args.start_date and args.end_date):
        parser.error("--mode backfill needs --start-date and --end-date")
    try:
        window = DateWindow(args.start_date, args.end_date) if has_window else DateWindow()
    except ValueError as exc:
        parser.error(str(exc))
    return args, window


def main(argv: list[str] | None = None) -> int:
    args, window = parse(argv)
    if args.dry_run:
        for task in plan(TASKS, MODES[args.mode]):
            deps = ", ".join(dep for dep in task.depends_on if dep in MODES[args.mode]) or "-"
            print(f"{task.step:>4}  {task.name:<16} after: {deps:<45} {task.description}")
        print("  12  report           always    build and store the execution report")
        return 0

    from orchestration.runner import run

    try:
        code, _ = run(args.mode, window, force_mart=args.force_mart)
    except SettingsError as exc:
        print(f"configuration error: {exc}", file=sys.stderr)
        return 2
    return code


if __name__ == "__main__":
    sys.exit(main())
