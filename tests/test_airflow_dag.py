"""The Airflow DAG: data-interval windows, the step subprocess, retry semantics, and the task graph.

Window and subprocess tests need no Airflow. Graph and exception tests run where apache-airflow is installed
(the Airflow image, or a separate virtualenv; see docs/airflow.md) and are skipped otherwise.
"""

from __future__ import annotations

import os
import sys
import textwrap
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace

import pytest

DAGS = Path(__file__).resolve().parents[1] / "dags"
sys.path.insert(0, str(DAGS))

from smart_logistics import pipeline_step  # noqa: E402
from smart_logistics.pipeline_step import StepFailed, child_environment, resolve_window, run_step  # noqa: E402

UTC = timezone.utc
PIPELINE_ORDER = ["check_source_connectivity", "validate_source_data", "prepare_warehouse", "run_etl",
                  "run_data_quality", "refresh_marts", "generate_features", "train_or_load_model",
                  "run_batch_inference", "validate_prediction_records"]


def at(*args):
    return datetime(*args, tzinfo=UTC)


@pytest.mark.parametrize("params, start, end, expected", [
    ({}, at(2026, 9, 1), at(2026, 9, 2), (date(2026, 9, 1), date(2026, 9, 1))),
    ({}, at(2026, 9, 1), at(2026, 10, 1), (date(2026, 9, 1), date(2026, 9, 30))),
    ({}, at(2026, 9, 1, 0, 0), at(2026, 9, 1, 0, 0), (date(2026, 9, 1), date(2026, 9, 1))),
    ({}, datetime(2026, 9, 1, 3, tzinfo=timezone(timedelta(hours=3))), at(2026, 9, 2),
     (date(2026, 9, 1), date(2026, 9, 1))),
    ({"start_date": "2026-08-01", "end_date": "2026-08-31"}, at(2026, 9, 1), at(2026, 9, 2),
     (date(2026, 8, 1), date(2026, 8, 31))),
    ({"start_date": "2026-08-01", "end_date": "2026-08-31"}, None, None, (date(2026, 8, 1), date(2026, 8, 31))),
])
def test_window_comes_from_params_or_the_data_interval(params, start, end, expected):
    assert resolve_window(params, start, end) == expected


@pytest.mark.parametrize("params, start, end, message", [
    ({"start_date": "2026-08-01"}, at(2026, 9, 1), at(2026, 9, 2), "given together"),
    ({"start_date": "2026-08-31", "end_date": "2026-08-01"}, None, None, "after end_date"),
    ({}, None, None, "no data interval"),
])
def test_invalid_windows_are_rejected(params, start, end, message):
    with pytest.raises(ValueError, match=message):
        resolve_window(params, start, end)


def test_child_environment_drops_airflow_and_superuser_secrets():
    env = child_environment({"AIRFLOW__DATABASE__SQL_ALCHEMY_CONN": "postgresql://a:secret@h/airflow",
                             "_AIRFLOW_WWW_USER_PASSWORD": "x", "POSTGRES_PASSWORD": "x", "AIRFLOW_DB_PASSWORD": "x",
                             "DWH_DB_PASSWORD": "kept", "PATH": "/usr/bin"})
    assert env == {"DWH_DB_PASSWORD": "kept", "PATH": "/usr/bin"}


FAKE_STEP = textwrap.dedent("""
    import argparse, json, os, sys, time
    parser = argparse.ArgumentParser()
    parser.add_argument("step")
    parser.add_argument("--run-key")
    parser.add_argument("--start-date")
    parser.add_argument("--end-date")
    parser.add_argument("--result-file")
    parser.add_argument("--input-file")
    args, _ = parser.parse_known_args()
    behaviour = os.environ["FAKE_BEHAVIOUR"]
    print("fake step", args.step, args.start_date, args.end_date, flush=True)
    if behaviour == "sleep":
        pid_file = os.environ["FAKE_PID_FILE"]
        with open(pid_file + ".tmp", "w") as handle:
            handle.write(str(os.getpid()))
        os.replace(pid_file + ".tmp", pid_file)
        print("sleeping", flush=True)
        time.sleep(60)
    code = int(behaviour)
    payload = {"result": {"step": args.step, "window": [args.start_date, args.end_date],
                          "input": json.load(open(args.input_file)) if args.input_file else None}}
    if code:
        payload = {"error": {"type": "OperationalError", "message": "boom", "transient": code == 75}}
    open(args.result_file, "w").write(json.dumps(payload))
    sys.exit(code)
""")


@pytest.fixture
def fake_pipeline(tmp_path, monkeypatch):
    package = tmp_path / "orchestration"
    package.mkdir()
    (package / "__init__.py").write_text("")
    (package / "airflow_steps.py").write_text(FAKE_STEP)
    monkeypatch.setenv("PIPELINE_PYTHON", sys.executable)
    monkeypatch.setenv("PIPELINE_HOME", str(tmp_path))
    return lambda behaviour: monkeypatch.setenv("FAKE_BEHAVIOUR", behaviour)


WINDOW = (date(2026, 9, 1), date(2026, 9, 1))


def test_run_step_returns_the_result(fake_pipeline):
    fake_pipeline("0")
    result = run_step("run_etl", run_key="d/r", window=WINDOW, payload={"tasks": []})
    assert result == {"step": "run_etl", "window": ["2026-09-01", "2026-09-01"], "input": {"tasks": []}}


@pytest.mark.parametrize("code, transient", [(75, True), (1, False), (2, False)])
def test_run_step_raises_with_the_retry_decision(fake_pipeline, code, transient):
    fake_pipeline(str(code))
    with pytest.raises(StepFailed) as raised:
        run_step("run_etl", run_key="d/r", window=WINDOW)
    assert raised.value.exit_code == code
    assert raised.value.transient is transient
    assert raised.value.error["message"] == "boom"


def test_interrupted_task_stops_the_step(fake_pipeline, tmp_path, monkeypatch):
    """An Airflow timeout raises inside run_step; the subprocess must not keep running."""
    fake_pipeline("sleep")
    pid_file = tmp_path / "pid"
    monkeypatch.setenv("FAKE_PID_FILE", str(pid_file))

    def interrupt(message, *args):
        if (message % args if args else message) == "sleeping":
            raise TimeoutError("execution_timeout")

    monkeypatch.setattr(pipeline_step.logger, "info", interrupt)
    monkeypatch.setattr(pipeline_step, "TERMINATE_GRACE_S", 5)
    with pytest.raises(TimeoutError):
        run_step("run_etl", run_key="d/r", window=WINDOW)
    pid = int(pid_file.read_text())
    with pytest.raises(ProcessLookupError):
        os.kill(pid, 0)


# Everything below needs apache-airflow.

class FakeTI:
    def __init__(self, try_number=1, xcoms=None, states=None):
        self.try_number, self.dag_id, self.run_id = try_number, "smart_logistics_data_pipeline", "manual__t"
        self.pushed, self.xcoms, self.states = {}, xcoms or {}, states or {}

    def xcom_push(self, key, value):
        self.pushed[key] = value

    def xcom_pull(self, task_ids, key="return_value"):
        return self.xcoms.get((task_ids, key))

    def get_task_states(self, dag_id, task_ids, run_ids):
        return {run_ids[0]: {task_id: self.states.get(task_id, "success") for task_id in task_ids}}


@pytest.fixture
def airflow_context(monkeypatch):
    sdk = pytest.importorskip("airflow.sdk")

    def install(ti, params=None):
        context = {"ti": ti, "params": params or {}, "run_id": ti.run_id,
                   "dag": SimpleNamespace(dag_id=ti.dag_id), "logical_date": at(2026, 9, 1),
                   "data_interval_start": at(2026, 9, 1), "data_interval_end": at(2026, 9, 2),
                   "dag_run": SimpleNamespace(start_date=at(2026, 11, 8, 12), run_type="manual")}
        monkeypatch.setattr(sdk, "get_current_context", lambda: context, raising=False)
        return context
    return install


def failing(code):
    def run(step, **kwargs):
        raise StepFailed(step, code, {"type": "OperationalError", "message": "boom"})
    return run


def test_transient_failure_is_retried_and_permanent_failure_is_not(airflow_context, monkeypatch):
    from airflow.sdk.exceptions import AirflowException, AirflowFailException

    ti = FakeTI(try_number=2)
    airflow_context(ti)
    monkeypatch.setattr(pipeline_step, "run_step", failing(75))
    with pytest.raises(AirflowException) as raised:
        pipeline_step.execute_step("run_etl")
    assert type(raised.value) is AirflowException
    assert ti.pushed["failure"]["attempt"] == 2 and ti.pushed["failure"]["transient"] is True

    monkeypatch.setattr(pipeline_step, "run_step", failing(1))
    with pytest.raises(AirflowFailException):
        pipeline_step.execute_step("run_etl")
    assert ti.pushed["failure"]["transient"] is False


def test_step_receives_the_window_params_and_model(airflow_context, monkeypatch):
    calls = []
    monkeypatch.setattr(pipeline_step, "run_step", lambda step, **kwargs: calls.append((step, kwargs)) or {"ok": 1})
    ti = FakeTI(xcoms={("train_or_load_model", "return_value"): {"output": {"model_version": "v1"}}})
    airflow_context(ti, params={"force_mart": True, "retrain": True})
    result = pipeline_step.execute_step("run_batch_inference")
    pipeline_step.execute_step("refresh_marts")
    pipeline_step.execute_step("train_or_load_model")
    assert result["window"] == ["2026-09-01", "2026-09-01"] and result["output"] == {"ok": 1}
    assert [(step, kwargs["options"]) for step, kwargs in calls] == [
        ("run_batch_inference", ("--model-version", "v1")), ("refresh_marts", ("--force-mart",)),
        ("train_or_load_model", ("--retrain",))]
    assert calls[0][1]["run_key"] == "smart_logistics_data_pipeline/manual__t"


def test_publish_records_the_run_then_fails_it_when_a_task_failed(airflow_context, monkeypatch):
    from airflow.sdk.exceptions import AirflowFailException

    sent = {}

    def publish(step, **kwargs):
        sent.update(kwargs["payload"])
        return {"run_id": "u", "status": "failed"}

    monkeypatch.setattr(pipeline_step, "run_step", publish)
    ti = FakeTI(states={"run_etl": "failed", "run_data_quality": "upstream_failed"},
                xcoms={("run_etl", "failure"): {"attempt": 4, "type": "OperationalError"}})
    airflow_context(ti)
    with pytest.raises(AirflowFailException, match="run_etl"):
        pipeline_step.publish_statistics(tuple(PIPELINE_ORDER))
    by_task = {item["task_id"]: item for item in sent["tasks"]}
    assert by_task["run_etl"]["state"] == "failed" and by_task["run_etl"]["attempts"] == 4
    assert by_task["run_data_quality"]["state"] == "upstream_failed"
    assert sent["airflow"]["run_id"] == "manual__t"

    ti.states = {}
    assert pipeline_step.publish_statistics(tuple(PIPELINE_ORDER)) == {"run_id": "u", "status": "failed"}


@pytest.fixture(scope="module")
def dag():
    pytest.importorskip("airflow")
    from airflow.dag_processing.dagbag import DagBag

    bag = DagBag(dag_folder=str(DAGS))
    assert bag.import_errors == {}
    return bag.dags["smart_logistics_data_pipeline"]


def test_dag_runs_the_pipeline_in_dependency_order(dag):
    upstream = {task.task_id: task.upstream_task_ids for task in dag.tasks}
    assert upstream == {
        "check_source_connectivity": set(),
        "validate_source_data": {"check_source_connectivity"},
        "prepare_warehouse": {"validate_source_data"},
        "run_etl": {"prepare_warehouse"},
        "run_data_quality": {"run_etl"},
        "refresh_marts": {"run_data_quality"},
        "generate_features": {"run_data_quality"},
        "train_or_load_model": {"generate_features"},
        "run_batch_inference": {"train_or_load_model"},
        "validate_prediction_records": {"refresh_marts", "run_batch_inference"},
        "publish_pipeline_statistics": {"validate_prediction_records"},
    }
    order = [task.task_id for task in dag.topological_sort()]
    for task_id, deps in upstream.items():
        assert all(order.index(dep) < order.index(task_id) for dep in deps)


def test_dag_schedule_and_run_policy(dag):
    from airflow.sdk import CronDataIntervalTimetable

    assert isinstance(dag.timetable, CronDataIntervalTimetable)
    assert dag.catchup is False and dag.max_active_runs == 1
    assert dag.dagrun_timeout == timedelta(hours=4)
    assert set(dag.params) == {"start_date", "end_date", "force_mart", "retrain"}


def test_every_task_has_retries_and_a_timeout(dag):
    for task in dag.tasks:
        assert task.retries >= 1, task.task_id
        assert task.execution_timeout is not None and task.execution_timeout <= timedelta(hours=1), task.task_id
        assert task.retry_exponential_backoff, task.task_id
    publish = dag.get_task("publish_pipeline_statistics")
    assert str(publish.trigger_rule) in ("all_done", "TriggerRule.ALL_DONE")
    assert all(str(task.trigger_rule) in ("all_success", "TriggerRule.ALL_SUCCESS")
               for task in dag.tasks if task is not publish)
