"""The execution report: counts, model metrics, durations, failures, and logical fingerprints.

Every section is collected independently, so a failed run still reports what can be read.
"""

from __future__ import annotations

import json
import logging
from datetime import date, datetime, timezone
from decimal import Decimal
from pathlib import Path

from sqlalchemy import text

from etl.config import DateWindow
from etl.log import log_event
from orchestration.dag import FAILED, NOT_RUN, SUCCEEDED

logger = logging.getLogger("pipeline")

MART_COLUMNS = (
    "price_month, loading_city_gcid, unloading_city_gcid, body_group_code, weight_category, origin_city_name, "
    "destination_city_name, body_group_name, p25_trip, median_trip, p75_trip, avg_trip, p25_km, median_km, "
    "p75_km, trips_count, carriers_count, cv_rate, mom_pct, yoy_pct, max_price_date, freshness_days, "
    "reliability_level, as_of_date"
)

MART_FINGERPRINT_SQL = f"""
SELECT count(*) AS rows,
       md5(coalesce(string_agg(md5(ROW({MART_COLUMNS})::text), ''
           ORDER BY price_month, loading_city_gcid, unloading_city_gcid, body_group_code, weight_category), ''))
           AS logical
FROM mart.mart_market_corridor
"""

PREDICTION_FINGERPRINT_SQL = """
SELECT count(*) AS rows,
       md5(coalesce(string_agg(md5((to_jsonb(p) - 'scored_at' - 'scoring_batch_id')::text), ''
           ORDER BY price_id, model_name, model_version), '')) AS logical
FROM ml.ml_price_predictions AS p
"""

REGISTRY_FINGERPRINT_SQL = """
SELECT count(*) AS rows,
       md5(coalesce(string_agg(md5(ROW(model_name, model_version, model_sha256, metrics, baseline_metrics,
                                        training_rows, validation_rows, best_iteration)::text), ''
           ORDER BY model_name, model_version), '')) AS logical
FROM ml.ml_model_registry
"""

LATEST_ETL_SQL = """
SELECT batch_id, mode, start_date, end_date, extracted_count, accepted_count, rejected_count, completed_at
FROM audit.etl_batch
WHERE status = 'succeeded' AND mode = 'full'
ORDER BY batch_id DESC
LIMIT 1
"""

PREVIOUS_RUN_SQL = """
SELECT run_id, mode, started_at, report -> 'fingerprints' AS fingerprints
FROM audit.pipeline_run
WHERE status = 'succeeded' AND run_id <> :run_id
ORDER BY started_at DESC
LIMIT 1
"""

INSERT_RUN_SQL = """
INSERT INTO audit.pipeline_run (run_id, mode, status, start_date, end_date, as_of_date, started_at, completed_at,
                                duration_s, failed_task, error_message, tasks, report)
VALUES (:run_id, :mode, :status, :start_date, :end_date, :as_of_date, :started_at, :completed_at,
        :duration_s, :failed_task, :error_message, CAST(:tasks AS jsonb), CAST(:report AS jsonb))
"""


def _json_default(value):
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if isinstance(value, Decimal):
        return float(value)
    if isinstance(value, Path):
        return str(value)
    return str(value)


def dumps(payload, **kwargs) -> str:
    return json.dumps(payload, default=_json_default, ensure_ascii=False, **kwargs)


def _section(report: dict, name: str, collect) -> None:
    try:
        report[name] = collect()
    except Exception as error:  # noqa: BLE001 - a report section must not hide the run result
        report[name] = {"error": " ".join(f"{type(error).__name__}: {error}".split())[:500]}
        log_event(logger, "report_section_failed", logging.WARNING, section=name, error=report[name]["error"])


def _output(ctx, task: str) -> dict | None:
    result = ctx.results.get(task)
    return result.output if result is not None and result.status == SUCCEEDED else None


def _scalar(conn, sql: str, params: dict | None = None):
    return conn.execute(text(sql), params or {}).scalar_one()


def collect_counts(ctx) -> dict:
    counts: dict = {}
    source = _output(ctx, "source_ready")
    if source:
        counts["source_records"] = source["source_prices"]
    else:
        with ctx.source().connect() as conn:
            counts["source_records"] = int(_scalar(conn, "SELECT count(*) FROM public.prices"))

    etl = _output(ctx, "etl")
    with ctx.warehouse().connect() as conn:
        if etl:
            counts.update(etl_batch_id=etl["batch_id"], etl_from_this_run=True, etl_window=etl["mode"],
                          extracted_records=etl["extracted_count"], accepted_records=etl["accepted_count"],
                          quarantined_records=etl["rejected_count"], core_changes=etl["fact_deal"])
        else:
            latest = conn.execute(text(LATEST_ETL_SQL)).mappings().one_or_none()
            if latest:
                counts.update(etl_batch_id=latest["batch_id"], etl_from_this_run=False, etl_window=latest["mode"],
                              extracted_records=latest["extracted_count"], accepted_records=latest["accepted_count"],
                              quarantined_records=latest["rejected_count"])
        counts["quarantined_deals_in_warehouse"] = int(
            _scalar(conn, "SELECT count(DISTINCT price_id) FROM audit.quarantine_price"))
        counts["core_dwh_deals"] = int(_scalar(conn, "SELECT count(*) FROM core.fact_deal"))
        counts["mart_rows"] = int(_scalar(conn, "SELECT count(*) FROM mart.mart_market_corridor"))
        version = ctx.model_version()
        counts["model_version"] = version
        if version:
            registry = conn.execute(text(
                "SELECT training_rows, validation_rows FROM ml.ml_model_registry WHERE model_version = :v"
            ), {"v": version}).mappings().one_or_none()
            counts["ml_training_rows"] = registry["training_rows"] if registry else None
            counts["ml_validation_rows"] = registry["validation_rows"] if registry else None
            rows = conn.execute(text(
                "SELECT split_role, count(*) FROM ml.ml_price_predictions WHERE model_version = :v GROUP BY 1"
            ), {"v": version}).all()
            by_role = dict(rows)
            counts["ml_test_rows"] = int(by_role.get("test", 0))
            counts["ml_future_rows"] = int(by_role.get("future", 0))
    return counts


def collect_model(ctx) -> dict:
    version = ctx.model_version()
    if version is None:
        return {"model_version": None}
    with ctx.warehouse().connect() as conn:
        row = conn.execute(text(
            "SELECT model_name, model_version, model_sha256, best_iteration, training_rows, validation_rows, "
            "metrics, baseline_metrics, train_start, train_end, test_start, test_end, as_of_date "
            "FROM ml.ml_model_registry WHERE model_version = :v"
        ), {"v": version}).mappings().one_or_none()
    if row is None:
        return {"model_version": version, "registered": False}
    model = dict(row)
    model["trained_in_this_run"] = _output(ctx, "train") is not None
    for name, task in (("scoring_test", "score_test"), ("scoring_future", "score_future")):
        output = _output(ctx, task)
        if output:
            model[name] = {key: output[key] for key in ("scoring_batch_id", "rows_test", "rows_future",
                                                        "rows_inserted", "rows_updated", "rows_unchanged",
                                                        "rows_deleted", "duration_s", "summary")}
    return model


def collect_fingerprints(ctx) -> dict:
    from etl.quality.fingerprint import fingerprint, logical

    with ctx.warehouse().connect() as conn, conn.begin():
        conn.execute(text("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY"))
        core = logical(fingerprint(conn, DateWindow()))
        mart = dict(conn.execute(text(MART_FINGERPRINT_SQL)).mappings().one())
        predictions = dict(conn.execute(text(PREDICTION_FINGERPRINT_SQL)).mappings().one())
        registry = dict(conn.execute(text(REGISTRY_FINGERPRINT_SQL)).mappings().one())
    return {"core": core, "mart": mart, "predictions": predictions, "model_registry": registry}


def compare(previous: dict | None, current: dict) -> dict:
    """Per layer: unchanged, changed, or not comparable."""
    if not previous:
        return {}
    result = {}
    for layer, values in current.items():
        before = previous.get(layer)
        if not isinstance(values, dict) or "error" in values or not isinstance(before, dict) or "error" in before:
            result[layer] = "not comparable"
            continue
        changed = sorted(key for key in values if before.get(key) != values[key])
        result[layer] = "unchanged" if not changed else {"changed": changed}
    return result


def build_report(ctx, results: dict, started_at: datetime, finished_at: datetime, duration_s: float) -> dict:
    failed = [r for r in results.values() if r.status == FAILED]
    not_run = [r for r in results.values() if r.status == NOT_RUN]
    status = "failed" if failed or not_run else "succeeded"
    report: dict = {
        "run": {
            "run_id": str(ctx.run_id), "mode": ctx.mode, "status": status, "window": ctx.window.describe(),
            "start_date": ctx.window.start_date, "end_date": ctx.window.end_date, "as_of_date": ctx.config.as_of_date,
            "started_at": started_at, "completed_at": finished_at, "duration_s": duration_s,
            "failed_tasks": [{"task": r.name, "step": r.step, "attempts": r.attempts, **(r.error or {})}
                             for r in failed],
            "not_run_tasks": [{"task": r.name, "step": r.step, "reason": r.reason} for r in not_run],
        },
        "tasks": [{"task": r.name, "step": r.step, "status": r.status, "attempts": r.attempts,
                   "duration_s": r.duration_s, "reason": r.reason} for r in results.values()],
        "durations_s": {**{r.name: r.duration_s for r in results.values()}, "total": duration_s},
    }
    _section(report, "counts", lambda: collect_counts(ctx))
    _section(report, "model", lambda: collect_model(ctx))
    verify = _output(ctx, "verify")
    report["integrity"] = verify["results"] if verify else None
    _section(report, "fingerprints", lambda: collect_fingerprints(ctx))

    def previous():
        with ctx.warehouse().connect() as conn:
            row = conn.execute(text(PREVIOUS_RUN_SQL), {"run_id": ctx.run_id}).mappings().one_or_none()
        if row is None:
            return None
        return {"run_id": str(row["run_id"]), "mode": row["mode"], "started_at": row["started_at"],
                "comparison": compare(row["fingerprints"], report.get("fingerprints", {}))}

    _section(report, "previous_run", previous)
    report["task_outputs"] = {r.name: r.output for r in results.values() if r.output}
    return report


def persist(ctx, report: dict, results: dict) -> dict:
    """Write the JSON file and the audit.pipeline_run row. Returns where the report went."""
    stamp = report["run"]["started_at"].strftime("%Y%m%dT%H%M%SZ")
    path = ctx.report_dir / f"pipeline_run_{stamp}_{str(ctx.run_id)[:8]}.json"
    written: dict = {"file": str(path)}
    run = report["run"]
    failed = run["failed_tasks"] or run["not_run_tasks"]
    try:
        with ctx.warehouse().begin() as conn:
            conn.execute(text(INSERT_RUN_SQL), {
                "run_id": ctx.run_id, "mode": ctx.mode, "status": run["status"],
                "start_date": ctx.window.start_date, "end_date": ctx.window.end_date,
                "as_of_date": ctx.config.as_of_date, "started_at": run["started_at"],
                "completed_at": run["completed_at"], "duration_s": run["duration_s"],
                "failed_task": failed[0]["task"] if failed else None,
                "error_message": (run["failed_tasks"][0].get("message") if run["failed_tasks"]
                                  else failed[0]["reason"] if failed else None),
                "tasks": dumps([r.to_dict() for r in results.values()]),
                "report": dumps(report),
            })
        written["audit.pipeline_run"] = str(ctx.run_id)
    except Exception as error:  # noqa: BLE001 - the file report is still written
        written["audit.pipeline_run_error"] = f"{type(error).__name__}: {error}"[:500]
        log_event(logger, "report_not_persisted", logging.WARNING, error=written["audit.pipeline_run_error"])
    report["persisted"] = written
    ctx.report_dir.mkdir(parents=True, exist_ok=True)
    body = dumps(report, indent=2) + "\n"
    path.write_text(body, encoding="utf-8")
    (ctx.report_dir / "latest.json").write_text(body, encoding="utf-8")
    return written


def _metric(metrics: dict | None, key: str) -> str:
    if not metrics or metrics.get(key) is None:
        return "n/a"
    return f"{metrics[key]:,.4f}" if key == "r2" else f"{metrics[key]:,.2f}"


def format_report(report: dict) -> str:
    run = report["run"]
    lines = [f"Pipeline run {run['run_id']} mode={run['mode']} {run['status'].upper()} in {run['duration_s']:.1f}s",
             "  tasks:"]
    for task in report["tasks"]:
        extra = f" ({task['reason']})" if task["reason"] else ""
        attempts = f", {task['attempts']} attempts" if task["attempts"] > 1 else ""
        lines.append(f"    {task['step']:>4} {task['task']:<16} {task['status']:<9} {task['duration_s']:>8.2f}s"
                     f"{attempts}{extra}")
    counts = report.get("counts", {})
    if "error" in counts:
        lines.append(f"  counts: unavailable ({counts['error']})")
    else:
        source_note = "this run" if counts.get("etl_from_this_run") else f"batch {counts.get('etl_batch_id')}"
        lines += [
            f"  source records        {counts.get('source_records')}",
            f"  extracted / accepted / quarantined ({source_note})  "
            f"{counts.get('extracted_records')} / {counts.get('accepted_records')} / {counts.get('quarantined_records')}",
            f"  core DWH deals        {counts.get('core_dwh_deals')}",
            f"  mart rows             {counts.get('mart_rows')}",
            f"  ML train / test / future  {counts.get('ml_training_rows')} / {counts.get('ml_test_rows')} / "
            f"{counts.get('ml_future_rows')}  (model {counts.get('model_version')})",
        ]
    model = report.get("model", {})
    test = (model.get("metrics") or {}).get("test") if isinstance(model, dict) else None
    if test:
        lines.append(f"  test metrics          R2 {_metric(test, 'r2')}  MAPE {_metric(test, 'mape_pct')}%  "
                     f"RMSE {_metric(test, 'rmse_rub')}  MAE {_metric(test, 'mae_rub')}")
    for failure in run["failed_tasks"]:
        lines.append(f"  FAILED {failure['task']} after {failure['attempts']} attempt(s): "
                     f"{failure.get('type')}: {failure.get('message')}")
    if run["not_run_tasks"]:
        lines.append(f"  not run: {', '.join(task['task'] for task in run['not_run_tasks'])}")
    previous = report.get("previous_run")
    if isinstance(previous, dict) and "comparison" in previous:
        lines.append(f"  vs previous run {previous['run_id'][:8]} ({previous['mode']}): {dumps(previous['comparison'])}")
    return "\n".join(lines)


def utc_now() -> datetime:
    return datetime.now(timezone.utc)
