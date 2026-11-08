"""Pipeline runner without a database: plan order, failure propagation, retries, CLI, report fallback."""

from __future__ import annotations

import json
import logging
from datetime import date, datetime, timezone
from types import SimpleNamespace

import psycopg
import pytest
from sqlalchemy import exc as sa_exc

from etl.config import DateWindow
from orchestration.dag import FAILED, NOT_RUN, SKIPPED, SUCCEEDED, PlanError, Task, execute, is_transient, plan
from orchestration.report import build_report, compare, format_report
from orchestration.tasks import MODES, TASKS
from scripts import run_pipeline

FULL_ORDER = ["source_ready", "warehouse_ready", "migrate", "etl", "data_quality", "mart", "features", "train",
              "score_test", "score_future", "verify"]


def names(tasks):
    return [task.name for task in tasks]


def test_full_mode_runs_the_documented_steps_in_order():
    ordered = plan(TASKS, MODES["full"])
    assert names(ordered) == FULL_ORDER
    assert [task.step for task in ordered] == ["1-2", "3", "4", "5", "6", "7", "8", "9", "10", "11", "12"]


@pytest.mark.parametrize("mode, expected", [
    ("etl", ["source_ready", "warehouse_ready", "migrate", "etl", "data_quality", "verify"]),
    ("mart", ["warehouse_ready", "migrate", "mart", "verify"]),
    ("train", ["warehouse_ready", "migrate", "features", "train", "verify"]),
    ("score", ["warehouse_ready", "migrate", "score_test", "score_future", "verify"]),
    ("backfill", ["source_ready", "warehouse_ready", "migrate", "etl", "data_quality", "mart", "score_test",
                  "score_future", "verify"]),
])
def test_mode_plans(mode, expected):
    assert names(plan(TASKS, MODES[mode])) == expected


@pytest.mark.parametrize("mode", sorted(MODES))
def test_every_selected_dependency_runs_first(mode):
    ordered = names(plan(TASKS, MODES[mode]))
    by_name = {task.name: task for task in TASKS}
    for position, name in enumerate(ordered):
        for dep in by_name[name].depends_on:
            if dep in ordered:
                assert ordered.index(dep) < position, f"{dep} must precede {name}"


def test_task_graph_has_the_required_data_dependencies():
    deps = {task.name: set(task.depends_on) for task in TASKS}
    assert "migrate" in deps["etl"] and "source_ready" in deps["etl"]
    assert "data_quality" in deps["mart"] and "data_quality" in deps["features"]
    assert deps["train"] == {"features"}
    assert "train" in deps["score_test"] and "train" in deps["score_future"]
    assert set(FULL_ORDER) - {"verify", "source_ready", "warehouse_ready", "etl", "features"} <= deps["verify"]


def _task(name, run=lambda ctx: {"ok": True}, deps=(), **kwargs):
    return Task(name, "x", name, run, tuple(deps), **kwargs)


def test_plan_rejects_unknown_duplicate_and_cyclic_graphs():
    with pytest.raises(PlanError, match="unknown tasks"):
        plan((_task("a"),), ("b",))
    with pytest.raises(PlanError, match="duplicate"):
        plan((_task("a"), _task("a")), ("a",))
    with pytest.raises(PlanError, match="unknown tasks"):
        plan((_task("a", deps=("zzz",)),), ("a",))
    with pytest.raises(PlanError, match="cycle"):
        plan((_task("a", deps=("b",)), _task("b", deps=("a",))), ("a", "b"))


def test_plan_orders_by_dependency_not_by_selection_order():
    tasks = (_task("load"), _task("check", deps=("load",)), _task("publish", deps=("check",)))
    assert names(plan(tasks, ("publish", "check", "load"))) == ["load", "check", "publish"]


def ctx():
    return SimpleNamespace(results={}, state={})


def test_failure_stops_dependents_but_not_independent_tasks():
    def boom(_):
        raise ValueError("bad data")

    tasks = (_task("a"), _task("b", boom, ("a",)), _task("c", deps=("b",)), _task("d", deps=("a",)),
             _task("e", deps=("c", "d")))
    results = execute(plan(tasks, tuple(t.name for t in tasks)), ctx(), sleep=lambda s: None)
    assert {n: r.status for n, r in results.items()} == {
        "a": SUCCEEDED, "b": FAILED, "c": NOT_RUN, "d": SUCCEEDED, "e": NOT_RUN}
    assert results["b"].error["type"] == "ValueError" and results["b"].error["message"] == "bad data"
    assert results["b"].error["transient"] is False and results["b"].attempts == 1
    assert results["c"].reason == "upstream task b failed"
    assert results["e"].reason == "upstream task c not_run"


def test_skipped_task_satisfies_its_dependents():
    tasks = (_task("a", skip_if=lambda c: "nothing to do"), _task("b", deps=("a",)))
    results = execute(plan(tasks, ("a", "b")), ctx())
    assert results["a"].status == SKIPPED and results["a"].reason == "nothing to do"
    assert results["b"].status == SUCCEEDED


def test_transient_errors_are_retried_with_backoff():
    calls, sleeps = [], []

    def flaky(_):
        calls.append(1)
        if len(calls) < 3:
            raise sa_exc.OperationalError("SELECT 1", {}, psycopg.OperationalError("server closed the connection"))
        return {"rows": 5}

    results = execute([_task("a", flaky, retries=2)], ctx(), backoff_s=2.0, sleep=sleeps.append)
    assert results["a"].status == SUCCEEDED and results["a"].attempts == 3 and results["a"].output == {"rows": 5}
    assert sleeps == [2.0, 4.0]


def test_retries_are_bounded():
    def down(_):
        raise psycopg.OperationalError("connection refused")

    results = execute([_task("a", down, retries=1)], ctx(), backoff_s=0, sleep=lambda s: None)
    assert results["a"].status == FAILED and results["a"].attempts == 2 and results["a"].error["transient"]


def test_permanent_errors_are_not_retried():
    calls = []

    def integrity(_):
        calls.append(1)
        raise sa_exc.IntegrityError("INSERT", {}, psycopg.errors.UniqueViolation("duplicate key"))

    results = execute([_task("a", integrity, retries=3)], ctx(), sleep=lambda s: None)
    assert results["a"].status == FAILED and len(calls) == 1


def test_transient_classification():
    assert is_transient(psycopg.OperationalError("x"))
    assert is_transient(psycopg.errors.SerializationFailure("x"))
    assert is_transient(psycopg.errors.DeadlockDetected("x"))
    assert is_transient(sa_exc.OperationalError("q", {}, psycopg.OperationalError("x")))
    try:
        try:
            raise psycopg.OperationalError("lost")
        except psycopg.OperationalError as inner:
            raise RuntimeError("wrapped") from inner
    except RuntimeError as outer:
        assert is_transient(outer)
    assert not is_transient(sa_exc.OperationalError("connect", {}, psycopg.OperationalError(
        'connection failed: FATAL:  password authentication failed for user "dwh_etl"')))
    assert not is_transient(ValueError("x"))
    assert not is_transient(psycopg.errors.CheckViolation("x"))
    assert not is_transient(sa_exc.IntegrityError("q", {}, psycopg.errors.ForeignKeyViolation("x")))


def test_task_events_are_structured(caplog):
    caplog.set_level(logging.INFO, logger="pipeline")
    execute([_task("a")], ctx())
    events = [(r.getMessage(), r.fields) for r in caplog.records if r.name == "pipeline"]
    assert events[0][0] == "task_started" and events[0][1]["task"] == "a"
    assert events[1][0] == "task_succeeded" and events[1][1]["attempts"] == 1 and "duration_s" in events[1][1]


@pytest.mark.parametrize("argv, message", [
    (["--mode", "backfill"], "needs --start-date and --end-date"),
    (["--mode", "full", "--start-date", "2026-09-01", "--end-date", "2026-09-30"], "loads no window"),
    (["--mode", "score", "--start-date", "2026-09-01", "--end-date", "2026-09-30"], "loads no window"),
    (["--mode", "etl", "--start-date", "2026-09-30", "--end-date", "2026-09-01"], "is after"),
    (["--mode", "etl", "--start-date", "2026-09-30"], "given together"),
    (["--mode", "nightly"], "invalid choice"),
])
def test_cli_rejects_invalid_arguments(argv, message, capsys):
    with pytest.raises(SystemExit) as raised:
        run_pipeline.parse(argv)
    assert raised.value.code == 2
    assert message in capsys.readouterr().err


def test_cli_windows():
    args, window = run_pipeline.parse([])
    assert args.mode == "full" and window.is_full
    _, window = run_pipeline.parse(["--mode", "backfill", "--start-date", "2026-09-01", "--end-date", "2026-09-30"])
    assert (window.start_date, window.end_date) == (date(2026, 9, 1), date(2026, 9, 30))
    _, window = run_pipeline.parse(["--mode", "etl"])
    assert window.is_full


def test_dry_run_prints_the_plan_without_connecting(capsys, monkeypatch):
    for name in ("POSTGRES_HOST", "DWH_DB_PASSWORD", "SOURCE_DB_PASSWORD"):
        monkeypatch.delenv(name, raising=False)
    assert run_pipeline.main(["--mode", "full", "--dry-run"]) == 0
    lines = capsys.readouterr().out.splitlines()
    assert [line.split()[1] for line in lines] == FULL_ORDER + ["report"]


def test_compare_reports_changed_keys():
    before = {"core": {"fact_rows": 10, "logical_fact": "a"}, "mart": {"rows": 3, "logical": "m"}}
    after = {"core": {"fact_rows": 10, "logical_fact": "b"}, "mart": {"rows": 3, "logical": "m"},
             "predictions": {"rows": 1}}
    assert compare(before, after) == {"core": {"changed": ["logical_fact"]}, "mart": "unchanged",
                                      "predictions": "not comparable"}
    assert compare(None, after) == {}


def test_report_is_built_when_the_database_is_unreachable():
    def unreachable(_=None):
        raise psycopg.OperationalError("connection refused")

    boom = unreachable

    context = SimpleNamespace(
        results={}, state={}, run_id="00000000-0000-0000-0000-000000000001", mode="full",
        window=DateWindow(), config=SimpleNamespace(as_of_date=date(2026, 10, 26)),
        warehouse=unreachable, source=unreachable, model_version=lambda: None,
    )
    results = execute([_task("source_ready", boom), _task("etl", deps=("source_ready",))], context,
                      sleep=lambda s: None)
    now = datetime(2026, 11, 9, tzinfo=timezone.utc)
    report = build_report(context, results, now, now, 1.5)
    assert report["run"]["status"] == "failed"
    assert report["run"]["failed_tasks"][0]["task"] == "source_ready"
    assert report["run"]["failed_tasks"][0]["type"] == "OperationalError"
    assert report["run"]["not_run_tasks"] == [{"task": "etl", "step": "x", "reason": "upstream task source_ready failed"}]
    assert "connection refused" in report["counts"]["error"]
    assert "error" in report["fingerprints"]
    json.dumps(report, default=str)
    text = format_report(report)
    assert "FAILED source_ready" in text and "not run: etl" in text


def test_catboost_progress_becomes_log_events(caplog):
    from ml.model.train import CatBoostLog

    caplog.set_level(logging.INFO, logger="ml.train")
    sink = CatBoostLog()
    sink.write("0:\tlearn: 10.5\ttest: 11.0\n100:\tlearn")
    sink.write(": 9.1\n\n")
    sink.write("Shrink model")
    sink.flush()
    lines = [r.fields["line"] for r in caplog.records if r.getMessage() == "catboost_progress"]
    assert lines == ["0: learn: 10.5 test: 11.0", "100: learn: 9.1", "Shrink model"]


def test_no_shuffle_or_wall_clock_in_task_logic():
    """Run timestamps live in dag and report; task logic takes its dates from configuration."""
    import inspect

    from orchestration import tasks, verify

    for module in (tasks, verify):
        source = inspect.getsource(module)
        for banned in ("train_test_split", "shuffle(", ".sample(", "date.today", "datetime.now"):
            assert banned not in source, f"{banned} in {module.__name__}"
