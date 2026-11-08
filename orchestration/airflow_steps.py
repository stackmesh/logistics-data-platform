"""One task of the Airflow DAG, run in the pipeline's own interpreter.

    python -m orchestration.airflow_steps STEP --run-key KEY --start-date D --end-date D --result-file PATH
        [--model-version V] [--force-mart] [--retrain] [--input-file PATH]

dags/smart_logistics_data_pipeline.py runs every step in a subprocess, so the DAG uses the platform's pinned
dependencies instead of Airflow's. Each step calls the task functions scripts.run_pipeline uses.

Exit code 0: done, {"result": ...} is in --result-file. 75: transient failure, safe to retry.
1: permanent failure, {"error": ...} is in --result-file. 2: usage or configuration error.
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
import uuid
from datetime import date, datetime
from pathlib import Path
from typing import Callable

from sqlalchemy import text

from etl.config import DateWindow, load_config
from etl.log import configure_logging, log_event
from logistics.settings import SettingsError
from orchestration import tasks
from orchestration.dag import FAILED, NOT_RUN, SKIPPED, SUCCEEDED, TaskResult, describe_error, is_transient
from orchestration.report import build_report, dumps, persist, utc_now

logger = logging.getLogger("pipeline")

MODE = "airflow"
EXIT_OK, EXIT_FAILED, EXIT_USAGE, EXIT_TRANSIENT = 0, 1, 2, 75
RUN_ID_NAMESPACE = uuid.UUID("8b0f6c1e-6f4b-4a4e-9a36-2f1d4e0c7a51")

STEPS: dict[str, Callable] = {}


def step(name: str):
    def register(function):
        STEPS[name] = function
        return function
    return register


def airflow_run_uuid(run_key: str) -> uuid.UUID:
    """audit.pipeline_run.run_id for one Airflow DAG run; stable across task retries and cleared reruns."""
    return uuid.uuid5(RUN_ID_NAMESPACE, run_key)


def _read_only_dataset(ctx):
    from ml.features.dataset import build_dataset

    with ctx.warehouse().connect() as conn, conn.begin():
        conn.execute(text("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY"))
        return build_dataset(conn)


@step("check_source_connectivity")
def check_source_connectivity(ctx, args) -> dict:
    """One connection to each database; the Airflow retry policy does the waiting."""
    result = {}
    for target, engine in (("source", ctx.source()), ("warehouse", ctx.warehouse())):
        with engine.connect() as conn:
            result[target] = {"database": conn.execute(text("SELECT current_database()")).scalar_one(),
                              "server_version": conn.execute(text("SHOW server_version")).scalar_one()}
    return result


@step("validate_source_data")
def validate_source_data(ctx, args) -> dict:
    ready = tasks.source_ready(ctx)
    with ctx.source().connect() as conn:
        in_window = conn.execute(text(
            "SELECT count(*) FROM public.prices WHERE loading_date BETWEEN :start_date AND :end_date"
        ), ctx.window.sql_params()).scalar_one()
    return {**ready, "window": ctx.window.describe(), "window_source_deals": int(in_window)}


@step("prepare_warehouse")
def prepare_warehouse(ctx, args) -> dict:
    return {"readiness": tasks.warehouse_ready(ctx), "migrations": tasks.migrate(ctx)}


@step("run_etl")
def run_etl(ctx, args) -> dict:
    return tasks.etl(ctx)


@step("run_data_quality")
def run_data_quality(ctx, args) -> dict:
    return tasks.data_quality(ctx)


@step("refresh_marts")
def refresh_marts(ctx, args) -> dict:
    return tasks.mart(ctx)


@step("generate_features")
def generate_features(ctx, args) -> dict:
    return tasks.features(ctx)


def reusable_model(ctx, version: str) -> tuple[dict | None, str]:
    """The registered model of this version, if its artifacts are present and match the registry."""
    from ml.model.artifacts import load_model

    with ctx.warehouse().connect() as conn:
        registered = conn.execute(text(
            "SELECT model_sha256 FROM ml.ml_model_registry WHERE model_version = :version"
        ), {"version": version}).scalar_one_or_none()
    if registered is None:
        return None, f"model {version} is not registered"
    try:
        _, card, _ = load_model(ctx.artifact_root, version)
    except (OSError, ValueError, KeyError) as error:
        return None, f"artifacts of {version} unusable: {type(error).__name__}: {error}"
    if card["model_sha256"] != registered:
        return None, f"artifacts of {version} differ from the registry"
    return card, f"model {version} is registered and its artifacts match"


@step("train_or_load_model")
def train_or_load_model(ctx, args) -> dict:
    """The version is a hash of the training data and config, so an unchanged train set reuses the model."""
    from ml.model.artifacts import publish_latest
    from ml.model.config import TrainingConfig
    from ml.model.train import model_version

    dataset = _read_only_dataset(ctx)
    version = model_version(dataset, TrainingConfig())
    card, reason = (None, "retrain requested") if args.retrain else reusable_model(ctx, version)
    if card is not None:
        publish_latest(ctx.artifact_root, card)
        return {"action": "loaded", "reason": reason, "model_name": card["model_name"],
                "model_version": card["model_version"], "model_sha256": card["model_sha256"],
                "metrics": card["metrics"]}
    ctx.state["dataset"] = dataset
    trained = tasks.train(ctx)
    return {"action": "trained", "reason": reason, **trained}


def _require_model(ctx, args) -> None:
    if not args.model_version:
        raise SettingsError("--model-version is required for this step")
    ctx.state["model_version"] = args.model_version


@step("run_batch_inference")
def run_batch_inference(ctx, args) -> dict:
    _require_model(ctx, args)
    return {"test": tasks.score_test(ctx), "future": tasks.score_future(ctx)}


@step("validate_prediction_records")
def validate_prediction_records(ctx, args) -> dict:
    _require_model(ctx, args)
    return tasks.verify(ctx)


AIRFLOW_STATES = {"success": SUCCEEDED, "skipped": SKIPPED, "failed": FAILED}


def _results_from_airflow(ctx, airflow_tasks: list[dict]) -> dict[str, TaskResult]:
    """Airflow task states and outputs as TaskResults; ctx.results gets the runner's names the report reads."""
    results: dict[str, TaskResult] = {}
    for index, item in enumerate(airflow_tasks, start=1):
        result = TaskResult(item["task_id"], str(index), status=AIRFLOW_STATES.get(item.get("state"), NOT_RUN),
                            attempts=int(item.get("attempts") or 0), duration_s=float(item.get("duration_s") or 0.0),
                            output=item.get("output") or {}, error=item.get("error"))
        if result.status == NOT_RUN:
            result.reason = f"airflow state {item.get('state')}"
        results[result.name] = result
    aliases = {"validate_source_data": "source_ready", "run_etl": "etl", "validate_prediction_records": "verify"}
    for name, alias in aliases.items():
        if name in results:
            ctx.results[alias] = results[name]
    model = results.get("train_or_load_model")
    if model and model.status == SUCCEEDED:
        ctx.state["model_version"] = model.output["model_version"]
        if model.output.get("action") == "trained":
            ctx.results["train"] = model
    inference = results.get("run_batch_inference")
    if inference and inference.status == SUCCEEDED:
        for role in ("test", "future"):
            ctx.results[f"score_{role}"] = TaskResult(f"score_{role}", inference.step, SUCCEEDED,
                                                      output=inference.output[role])
    return results


@step("publish_pipeline_statistics")
def publish_pipeline_statistics(ctx, args) -> dict:
    if not args.input_file:
        raise SettingsError("--input-file with the Airflow task states is required for this step")
    payload = json.loads(Path(args.input_file).read_text(encoding="utf-8"))
    results = _results_from_airflow(ctx, payload["tasks"])
    started_at = datetime.fromisoformat(payload["started_at"]) if payload.get("started_at") else utc_now()
    tasks_s = round(sum(result.duration_s for result in results.values()), 3)
    report = build_report(ctx, results, started_at, utc_now(), tasks_s)
    report["airflow"] = payload.get("airflow", {})
    report["run"]["duration_s"] = round((report["run"]["completed_at"] - started_at).total_seconds(), 3)
    persisted = persist(ctx, report, results)
    run = report["run"]
    return {"run_id": run["run_id"], "status": run["status"], "window": run["window"],
            "failed_tasks": [task["task"] for task in run["failed_tasks"]],
            "not_run_tasks": [task["task"] for task in run["not_run_tasks"]],
            "counts": report.get("counts"), "persisted": persisted}


def _date(value: str) -> date:
    try:
        return date.fromisoformat(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError(f"expected YYYY-MM-DD, got {value!r}") from exc


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="python -m orchestration.airflow_steps", description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("step", choices=list(STEPS))
    parser.add_argument("--run-key", required=True, help="Airflow dag_id/run_id; names the audit.pipeline_run row")
    parser.add_argument("--start-date", type=_date, required=True, help="first loading_date of the window")
    parser.add_argument("--end-date", type=_date, required=True, help="last loading_date, inclusive")
    parser.add_argument("--result-file", type=Path, required=True)
    parser.add_argument("--model-version", help="run_batch_inference, validate_prediction_records")
    parser.add_argument("--force-mart", action="store_true", help="refresh_marts: rebuild even if inputs unchanged")
    parser.add_argument("--retrain", action="store_true", help="train_or_load_model: train even if registered")
    parser.add_argument("--input-file", type=Path, help="publish_pipeline_statistics: Airflow task states")
    return parser


def _write(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(dumps(payload) + "\n", encoding="utf-8")


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    configure_logging()
    try:
        window = DateWindow(args.start_date, args.end_date)
        ctx = tasks.RunContext.from_environment(MODE, window, load_config(), args.force_mart)
    except (SettingsError, ValueError) as error:
        _write(args.result_file, {"error": describe_error(error)})
        log_event(logger, "step_config_error", logging.ERROR, step=args.step, error=str(error))
        return EXIT_USAGE
    ctx.run_id = airflow_run_uuid(args.run_key)
    log_event(logger, "step_started", step=args.step, run_key=args.run_key, run_id=str(ctx.run_id),
              window=window.describe())
    try:
        result = STEPS[args.step](ctx, args)
    except SettingsError as error:
        _write(args.result_file, {"error": describe_error(error)})
        log_event(logger, "step_config_error", logging.ERROR, step=args.step, error=str(error))
        return EXIT_USAGE
    except Exception as error:  # noqa: BLE001 - the exit code tells Airflow whether to retry
        transient = is_transient(error) or isinstance(error, tasks.DatabaseNotReady)
        described = {**describe_error(error), "transient": transient}
        _write(args.result_file, {"error": described})
        log_event(logger, "step_failed", logging.ERROR, step=args.step, error=described)
        return EXIT_TRANSIENT if transient else EXIT_FAILED
    finally:
        ctx.close()
    _write(args.result_file, {"result": result})
    log_event(logger, "step_succeeded", step=args.step, run_id=str(ctx.run_id))
    return EXIT_OK


if __name__ == "__main__":
    sys.exit(main())
