"""Extract, validate, normalize, and load one window as one audited batch."""

from __future__ import annotations

import logging
import time

from etl.config import DateWindow, EtlConfig, load_config, source_engine, warehouse_engine
from etl.extract.source import extract_snapshot
from etl.load.warehouse import load_snapshot
from etl.log import configure_logging, log_event
from etl.pipeline.batch import complete_batch, fail_batch, start_batch
from etl.transform.normalize import transform_snapshot

logger = logging.getLogger("etl.pipeline")


def run_pipeline(window: DateWindow, config: EtlConfig | None = None) -> dict:
    configure_logging()
    config = config or load_config()
    source = source_engine(config)
    warehouse = warehouse_engine(config)
    batch_id: int | None = None
    timings: dict[str, float] = {}
    started = time.perf_counter()
    try:
        batch_id = start_batch(warehouse, config, window)
        log_event(logger, "batch_started", batch_id=batch_id, mode=window.mode,
                  start_date=window.start_date, end_date=window.end_date, as_of_date=config.as_of_date)

        step = time.perf_counter()
        snapshot = extract_snapshot(source, window, config.fetch_size)
        timings["extract_s"] = round(time.perf_counter() - step, 3)
        log_event(logger, "extract_completed", batch_id=batch_id, rows=snapshot.counts(),
                  extracted_at=snapshot.extracted_at, duration_s=timings["extract_s"])

        step = time.perf_counter()
        result = transform_snapshot(snapshot, config.as_of_date)
        timings["transform_s"] = round(time.perf_counter() - step, 3)
        log_event(logger, "transform_completed", batch_id=batch_id,
                  accepted=result.stats["accepted_count"], rejected=result.stats["rejected_count"],
                  reason_counts=result.stats["reason_counts"], duration_s=timings["transform_s"])

        step = time.perf_counter()
        with warehouse.connect() as conn:
            with conn.begin():
                load = load_snapshot(
                    conn,
                    batch_id=batch_id,
                    window=window,
                    snapshot=snapshot,
                    result=result,
                    source_system=config.source_system,
                )
                fact = load["core"]["fact_deal"]
                fact["unchanged"] = result.stats["accepted_count"] - fact["inserted"] - fact["updated"]
                timings["load_s"] = round(time.perf_counter() - step, 3)
                timings["total_s"] = round(time.perf_counter() - started, 3)
                summary = {
                    "batch_id": batch_id,
                    "status": "succeeded",
                    "mode": window.mode,
                    "start_date": window.start_date,
                    "end_date": window.end_date,
                    "as_of_date": config.as_of_date,
                    "extracted_at": snapshot.extracted_at,
                    "source_rows": snapshot.counts(),
                    **result.stats,
                    "load": load,
                    "timings": timings,
                }
                complete_batch(conn, batch_id, summary)
        log_event(logger, "batch_completed", **summary)
        return summary
    except Exception as exc:
        if batch_id is not None:
            try:
                fail_batch(warehouse, batch_id, exc)
            except Exception:
                logger.exception("could not mark batch %s as failed", batch_id)
        log_event(logger, "batch_failed", logging.ERROR, batch_id=batch_id,
                  error=f"{type(exc).__name__}: {exc}")
        raise
    finally:
        source.dispose()
        warehouse.dispose()


def format_summary(summary: dict) -> str:
    fact = summary["load"]["core"]["fact_deal"]
    quarantine = summary["load"]["quarantine"]
    profile = summary["accepted_profile"]
    window = (
        "full history" if summary["mode"] == "full"
        else f"{summary['start_date']} .. {summary['end_date']} (loading_date, inclusive)"
    )
    lines = [
        f"ETL batch {summary['batch_id']} {summary['status']}",
        f"  window              {window}",
        f"  extracted           {summary['extracted_count']}",
        f"  accepted            {summary['accepted_count']}",
        f"  rejected            {summary['rejected_count']} deals, {summary['reason_rows']} reason rows",
    ]
    lines += [f"    {code:<27} {count}" for code, count in summary["reason_counts"].items() if count]
    lines += [
        f"  core.fact_deal      inserted {fact['inserted']}, updated {fact['updated']}, "
        f"unchanged {fact['unchanged']}, deleted {fact['deleted']}",
        f"  quarantine          inserted {quarantine['inserted']}, updated {quarantine['updated']}, "
        f"deleted {quarantine['deleted']}",
        f"  accepted profile    future {profile['future_loading_date']}, non-fact {profile['non_fact']}, "
        f"same-city {profile['same_city_routes']}, outlier-marked {profile['outlier_marked']}, "
        f"created on as-of date {profile['created_on_as_of_date']} "
        f"({profile['created_on_as_of_date_outlier_marked']} outlier-marked)",
        f"  timings             {summary['timings']}",
    ]
    return "\n".join(lines)
