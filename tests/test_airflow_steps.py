"""orchestration.airflow_steps: exit codes, run identity, and the report built from Airflow task states."""

from __future__ import annotations

import ast
import json
import uuid
from datetime import date, datetime, timezone
from pathlib import Path
from types import SimpleNamespace

import psycopg
import pytest
from sqlalchemy import text

from etl.config import DateWindow
from logistics.settings import SettingsError
from orchestration import airflow_steps
from orchestration.dag import FAILED, NOT_RUN, SUCCEEDED
from orchestration.tasks import DatabaseNotReady, RunContext

DAG_FILE = Path(__file__).resolve().parents[1] / "dags" / "smart_logistics_data_pipeline.py"
WINDOW_ARGS = ["--run-key", "smart_logistics_data_pipeline/manual__x", "--start-date", "2026-09-01",
               "--end-date", "2026-09-01"]


def dag_step_names() -> list[str]:
    tree = ast.parse(DAG_FILE.read_text(encoding="utf-8"))
    for node in ast.walk(tree):
        if isinstance(node, ast.AnnAssign) and getattr(node.target, "id", None) == "STEPS":
            return [key.value for key in node.value.keys]
    raise AssertionError("STEPS not found in the DAG file")


def test_every_dag_task_has_a_pipeline_step():
    assert dag_step_names() + ["publish_pipeline_statistics"] == list(airflow_steps.STEPS)


def test_run_uuid_is_stable_per_airflow_run():
    first = airflow_steps.airflow_run_uuid("smart_logistics_data_pipeline/backfill__2026-09-02T00:00:00+00:00")
    assert first == airflow_steps.airflow_run_uuid("smart_logistics_data_pipeline/backfill__2026-09-02T00:00:00+00:00")
    assert first != airflow_steps.airflow_run_uuid("smart_logistics_data_pipeline/backfill__2026-09-03T00:00:00+00:00")
    assert first.version == 5


@pytest.fixture
def offline(monkeypatch):
    """RunContext without databases; the step under test is replaced."""
    monkeypatch.setattr(airflow_steps, "load_config", lambda: SimpleNamespace(as_of_date=date(2026, 10, 26)))
    monkeypatch.setattr(RunContext, "close", lambda self: None)

    def use(behaviour):
        monkeypatch.setitem(airflow_steps.STEPS, "run_etl", behaviour)
    return use


def run_main(tmp_path, step="run_etl", extra=()):
    result_file = tmp_path / "result.json"
    code = airflow_steps.main([step, *WINDOW_ARGS, "--result-file", str(result_file), *extra])
    return code, json.loads(result_file.read_text())


def test_success_writes_the_result(offline, tmp_path):
    seen = {}

    def step(ctx, args):
        seen.update(window=ctx.window, mode=ctx.mode, run_id=ctx.run_id)
        return {"rows": 3, "day": date(2026, 9, 1)}

    offline(step)
    code, payload = run_main(tmp_path)
    assert code == airflow_steps.EXIT_OK
    assert payload == {"result": {"rows": 3, "day": "2026-09-01"}}
    assert seen["window"] == DateWindow(date(2026, 9, 1), date(2026, 9, 1))
    assert seen["mode"] == "airflow"
    assert seen["run_id"] == airflow_steps.airflow_run_uuid("smart_logistics_data_pipeline/manual__x")


@pytest.mark.parametrize("error, expected", [
    (psycopg.OperationalError("server closed the connection unexpectedly"), airflow_steps.EXIT_TRANSIENT),
    (DatabaseNotReady("source not ready after 60s"), airflow_steps.EXIT_TRANSIENT),
    (psycopg.OperationalError('password authentication failed for user "dwh_etl"'), airflow_steps.EXIT_FAILED),
    (RuntimeError("3 unexplained deals"), airflow_steps.EXIT_FAILED),
    (SettingsError("--model-version is required"), airflow_steps.EXIT_USAGE),
])
def test_exit_code_says_whether_to_retry(offline, tmp_path, error, expected):
    def step(ctx, args):
        raise error

    offline(step)
    code, payload = run_main(tmp_path)
    assert code == expected
    assert payload["error"]["type"] == type(error).__name__
    assert payload["error"]["transient"] is (expected == airflow_steps.EXIT_TRANSIENT)


def test_as_of_mismatch_is_a_configuration_error(monkeypatch, tmp_path):
    monkeypatch.setattr(airflow_steps, "load_config", lambda: SimpleNamespace(as_of_date=date(2026, 11, 1)))
    code, payload = run_main(tmp_path)
    assert code == airflow_steps.EXIT_USAGE
    assert "differs from the ML split" in payload["error"]["message"]


def test_inverted_window_is_rejected(tmp_path):
    result_file = tmp_path / "result.json"
    code = airflow_steps.main(["run_etl", "--run-key", "k", "--start-date", "2026-09-02", "--end-date", "2026-09-01",
                               "--result-file", str(result_file)])
    assert code == airflow_steps.EXIT_USAGE
    assert "after end_date" in json.loads(result_file.read_text())["error"]["message"]


def test_inference_needs_the_model_version(offline, tmp_path):
    code, payload = run_main(tmp_path, step="run_batch_inference")
    assert code == airflow_steps.EXIT_USAGE
    assert "--model-version" in payload["error"]["message"]


def airflow_items(model_action="loaded", failed=None):
    items = []
    for name in airflow_steps.STEPS:
        if name == "publish_pipeline_statistics":
            continue
        output = {"step": name}
        if name == "validate_source_data":
            output = {"source_prices": 300000}
        if name == "run_etl":
            output = {"batch_id": 1, "mode": "window", "extracted_count": 3, "accepted_count": 3,
                      "rejected_count": 0, "fact_deal": {"unchanged": 3}}
        if name == "validate_prediction_records":
            output = {"checks": 1, "passed": 1, "results": [{"name": "foreign_keys_hold", "ok": True}]}
        if name == "train_or_load_model":
            output = {"action": model_action, "model_version": "20261026-abc"}
        if name == "run_batch_inference":
            output = {"test": {"rows_test": 5}, "future": {"rows_future": 2}}
        items.append({"task_id": name, "state": "success", "attempts": 1, "duration_s": 1.0, "output": output})
    if failed:
        index = [item["task_id"] for item in items].index(failed)
        items[index].update(state="failed", output=None, attempts=4, error={"type": "OperationalError"})
        for item in items[index + 1:]:
            item.update(state="upstream_failed", output=None, attempts=0)
    return items


def test_airflow_states_become_task_results():
    ctx = SimpleNamespace(results={}, state={})
    results = airflow_steps._results_from_airflow(ctx, airflow_items())
    assert list(results) == [name for name in airflow_steps.STEPS if name != "publish_pipeline_statistics"]
    assert all(result.status == SUCCEEDED for result in results.values())
    assert ctx.state["model_version"] == "20261026-abc"
    assert ctx.results["etl"] is results["run_etl"]
    assert ctx.results["verify"] is results["validate_prediction_records"]
    assert "train" not in ctx.results, "a loaded model was not trained in this run"
    assert ctx.results["score_test"].output == {"rows_test": 5}
    assert ctx.results["score_future"].output == {"rows_future": 2}


def test_trained_model_and_failures_are_reported():
    ctx = SimpleNamespace(results={}, state={})
    results = airflow_steps._results_from_airflow(ctx, airflow_items(model_action="trained"))
    assert ctx.results["train"] is results["train_or_load_model"]

    ctx = SimpleNamespace(results={}, state={})
    results = airflow_steps._results_from_airflow(ctx, airflow_items(failed="run_etl"))
    assert results["run_etl"].status == FAILED and results["run_etl"].attempts == 4
    assert results["run_data_quality"].status == NOT_RUN
    assert results["run_data_quality"].reason == "airflow state upstream_failed"
    assert "model_version" not in ctx.state


@pytest.mark.integration
def test_publishing_the_same_airflow_run_twice_keeps_one_row(tmp_path, monkeypatch, dwh_engine):
    run_key = f"smart_logistics_data_pipeline/test__{uuid.uuid4()}"
    run_id = airflow_steps.airflow_run_uuid(run_key)
    monkeypatch.setenv("PIPELINE_REPORT_DIR", str(tmp_path / "reports"))
    started = datetime.now(timezone.utc).isoformat()
    try:
        for failed in ("run_etl", None):
            input_file = tmp_path / "input.json"
            input_file.write_text(json.dumps({"tasks": airflow_items(failed=failed), "started_at": started,
                                              "airflow": {"run_id": run_key}}))
            code = airflow_steps.main(["publish_pipeline_statistics", "--run-key", run_key, "--start-date",
                                       "2026-09-01", "--end-date", "2026-09-01", "--result-file",
                                       str(tmp_path / "result.json"), "--input-file", str(input_file)])
            assert code == airflow_steps.EXIT_OK
        with dwh_engine.connect() as conn:
            rows = conn.execute(text("SELECT mode, status, failed_task, start_date FROM audit.pipeline_run "
                                     "WHERE run_id = :id"), {"id": run_id}).all()
        assert [tuple(row) for row in rows] == [("airflow", "succeeded", None, date(2026, 9, 1))]
    finally:
        with dwh_engine.begin() as conn:
            conn.execute(text("DELETE FROM audit.pipeline_run WHERE run_id = :id"), {"id": run_id})
