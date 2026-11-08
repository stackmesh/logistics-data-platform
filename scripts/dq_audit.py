"""Collect the evidence used in docs/data_quality_report.md.

Runs committed ETL batches (idempotent on a loaded warehouse) and prints JSON:
reconciliation per window, idempotency fingerprints, and overlapping-window results.

    python scripts/dq_audit.py
"""

from __future__ import annotations

import json
import sys
from datetime import date
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from etl.config import DateWindow, load_config, warehouse_engine  # noqa: E402
from etl.log import configure_logging  # noqa: E402
from etl.pipeline.runner import run_pipeline  # noqa: E402
from etl.quality.fingerprint import fingerprint  # noqa: E402
from etl.quality.reconcile import reconcile_window  # noqa: E402

BRIEF = DateWindow(date(2026, 7, 26), date(2026, 10, 26))
A = DateWindow(date(2026, 8, 1), date(2026, 9, 30))
B = DateWindow(date(2026, 9, 1), date(2026, 10, 31))
OVERLAP = DateWindow(date(2026, 9, 1), date(2026, 9, 30))


def _run(window, config):
    summary = run_pipeline(window, config)
    load = summary["load"]
    return {
        "batch_id": summary["batch_id"],
        "extracted": summary["extracted_count"],
        "accepted": summary["accepted_count"],
        "rejected": summary["rejected_count"],
        "fact": load["core"]["fact_deal"],
        "quarantine": load["quarantine"],
        "raw_prices": load["raw"]["prices"],
        "seconds": summary["timings"]["total_s"],
    }


def main() -> int:
    configure_logging("WARNING")
    config = load_config()
    engine = warehouse_engine(config)

    def fp(window):
        with engine.connect() as conn:
            return fingerprint(conn, window)

    evidence = {"reconciliation": {}}
    for name, window in {"full": DateWindow(), "brief_window": BRIEF}.items():
        report = reconcile_window(window, monthly=(name == "full"))
        evidence["reconciliation"][name] = report

    first = _run(BRIEF, config)
    fp_first = fp(BRIEF)
    second = _run(BRIEF, config)
    fp_second = fp(BRIEF)
    evidence["idempotency"] = {
        "window": BRIEF.describe(),
        "run_1": first,
        "run_2": second,
        "fingerprint_run_1": fp_first,
        "fingerprint_run_2": fp_second,
        "identical": fp_first == fp_second,
    }

    full_before = fp(DateWindow())
    overlap_before = fp(OVERLAP)
    run_a = _run(A, config)
    run_b = _run(B, config)
    evidence["backfill"] = {
        "window_a": A.describe(),
        "window_b": B.describe(),
        "overlap": OVERLAP.describe(),
        "run_a": run_a,
        "run_b": run_b,
        "overlap_fact_rows_before": overlap_before["fact_rows"],
        "overlap_fact_rows_after": fp(OVERLAP)["fact_rows"],
        "overlap_identical": fp(OVERLAP) == overlap_before,
        "full_identical": fp(DateWindow()) == full_before,
        "full_fingerprint": full_before,
    }
    engine.dispose()
    print(json.dumps(evidence, indent=2, default=str))
    return 0


if __name__ == "__main__":
    sys.exit(main())
