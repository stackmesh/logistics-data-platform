"""Command line entry point.

    python -m etl.main --start-date 2026-07-26 --end-date 2026-10-26
    python -m etl.main --full
"""

from __future__ import annotations

import argparse
import sys
from datetime import date

from logistics.settings import SettingsError


def _date(value: str) -> date:
    try:
        return date.fromisoformat(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError(f"expected YYYY-MM-DD, got {value!r}") from exc


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="python -m etl.main",
        description="Load deals from deals_bigdata into logistics_dwh core. "
        "The window filters prices.loading_date and includes both bounds.",
    )
    parser.add_argument("--start-date", type=_date, help="first loading_date to load (YYYY-MM-DD)")
    parser.add_argument("--end-date", type=_date, help="last loading_date to load (YYYY-MM-DD)")
    parser.add_argument("--full", action="store_true", help="load the complete source history")
    return parser


def main(argv: list[str] | None = None) -> int:
    from etl.config import DateWindow

    parser = build_parser()
    args = parser.parse_args(argv)
    if args.full and (args.start_date or args.end_date):
        parser.error("--full cannot be combined with --start-date/--end-date")
    if not args.full and not (args.start_date and args.end_date):
        parser.error("give --start-date and --end-date, or --full")
    try:
        window = DateWindow() if args.full else DateWindow(args.start_date, args.end_date)
    except ValueError as exc:
        parser.error(str(exc))

    from etl.pipeline.runner import format_summary, run_pipeline

    try:
        summary = run_pipeline(window)
    except SettingsError as exc:
        print(f"configuration error: {exc}", file=sys.stderr)
        return 2
    except Exception:
        return 1
    print(format_summary(summary), file=sys.stderr)
    return 0


if __name__ == "__main__":
    sys.exit(main())
