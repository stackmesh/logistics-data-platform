"""Benchmark the ETL on the full source snapshot.

cold: extract and transform everything, then load into emptied targets inside one
      transaction that is rolled back, so the warehouse keeps its current contents.
warm: a normal committed full run over an already loaded warehouse.

    python scripts/benchmark_etl.py [--skip-warm]
"""

from __future__ import annotations

import argparse
import json
import resource
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from sqlalchemy import text  # noqa: E402

from etl.config import DateWindow, load_config, source_engine, warehouse_engine  # noqa: E402
from etl.extract.source import extract_snapshot  # noqa: E402
from etl.load.warehouse import load_snapshot  # noqa: E402
from etl.log import configure_logging  # noqa: E402
from etl.pipeline.runner import run_pipeline  # noqa: E402
from etl.transform.normalize import transform_snapshot  # noqa: E402

COLD_TRUNCATE = (
    "TRUNCATE raw.price_body_types, raw.price_dates, raw.prices, raw.cities, raw.body_types, "
    "raw.exchange_rates, core.bridge_deal_body_group, core.fact_deal_date, core.fact_deal, "
    "core.dim_city, core.dim_exchange_rate, audit.quarantine_price CASCADE"
)


def _peak_rss_mb() -> float:
    return round(resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1024, 1)


def _frame_mb(*frames) -> float:
    return round(sum(frame.estimated_size() for frame in frames) / 1024 / 1024, 1)


def cold(config) -> dict:
    window = DateWindow()
    source = source_engine(config)
    warehouse = warehouse_engine(config)
    try:
        started = time.perf_counter()
        snapshot = extract_snapshot(source, window, config.fetch_size)
        extract_s = time.perf_counter() - started
        rss = {"after_extract_mb": _peak_rss_mb()}

        started = time.perf_counter()
        result = transform_snapshot(snapshot, config.as_of_date)
        transform_s = time.perf_counter() - started
        rss["after_transform_mb"] = _peak_rss_mb()

        with warehouse.connect() as conn:
            transaction = conn.begin()
            try:
                conn.execute(text(COLD_TRUNCATE))
                batch_id = conn.execute(
                    text("INSERT INTO audit.etl_batch (source_system, mode, status) "
                         "VALUES ('benchmark', 'full', 'running') RETURNING batch_id")
                ).scalar_one()
                started = time.perf_counter()
                load = load_snapshot(conn, batch_id=batch_id, window=window, snapshot=snapshot,
                                     result=result, source_system="benchmark")
                load_s = time.perf_counter() - started
            finally:
                transaction.rollback()
        rss["after_load_mb"] = _peak_rss_mb()
    finally:
        source.dispose()
        warehouse.dispose()

    rows = snapshot.prices.height
    total = extract_s + transform_s + load_s
    return {
        "rows": rows,
        "source_rows_all_tables": sum(snapshot.counts().values()),
        "extract_s": round(extract_s, 2),
        "transform_s": round(transform_s, 2),
        "load_s": round(load_s, 2),
        "load_steps": load["timings"],
        "total_s": round(total, 2),
        "deals_per_s": round(rows / total),
        "fact_inserted": load["core"]["fact_deal"]["inserted"],
        "peak_rss_checkpoints": rss,
        "source_frames_mb": _frame_mb(snapshot.prices, snapshot.price_body_types, snapshot.price_dates,
                                      snapshot.cities, snapshot.body_types, snapshot.exchange_rates),
        "transformed_frames_mb": _frame_mb(result.deals, result.deal_dates, result.bridge, result.quarantine),
    }


def warm(config) -> dict:
    started = time.perf_counter()
    summary = run_pipeline(DateWindow(), config)
    total = time.perf_counter() - started
    fact = summary["load"]["core"]["fact_deal"]
    return {
        "batch_id": summary["batch_id"],
        "rows": summary["extracted_count"],
        "total_s": round(total, 2),
        "deals_per_s": round(summary["extracted_count"] / total),
        "timings": summary["timings"],
        "load_steps": summary["load"]["timings"],
        "fact_changes": {key: fact[key] for key in ("inserted", "updated", "deleted")},
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--skip-warm", action="store_true")
    args = parser.parse_args()
    configure_logging("WARNING")
    config = load_config()
    report = {"cold_full_load": cold(config)}
    if not args.skip_warm:
        report["warm_full_rerun"] = warm(config)
    report["peak_rss_mb"] = _peak_rss_mb()
    print(json.dumps(report, indent=2))
    return 0


if __name__ == "__main__":
    sys.exit(main())
