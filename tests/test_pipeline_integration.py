"""The pipeline runner against the real warehouse: repeatable modes, failure reporting, change detection."""

from __future__ import annotations

import dataclasses
import json
from datetime import date

import pytest
from sqlalchemy import text

from etl.config import DateWindow
from etl.mart.refresh import refresh_if_changed
from orchestration import runner
from orchestration.report import MART_COLUMNS, PREDICTION_FINGERPRINT_SQL
from orchestration.tasks import TASKS, RunContext
from orchestration.verify import duplicate_keys, foreign_key_orphans

pytestmark = pytest.mark.integration

AS_OF = date(2026, 10, 26)


def make_context(mode, etl_config, root):
    ctx = RunContext.from_environment(mode, DateWindow(), etl_config)
    return dataclasses.replace(ctx, artifact_root=root / "models", feature_dir=root / "features",
                               report_dir=root / "reports", retry_backoff_s=0.0)


def run_mode(mode, etl_config, root):
    return runner.run(mode, DateWindow(), config=etl_config, context=make_context(mode, etl_config, root))


def task_output(report, name):
    return report["task_outputs"][name]


@pytest.fixture(scope="module")
def pipeline_root(tmp_path_factory):
    return tmp_path_factory.mktemp("pipeline")


@pytest.fixture(scope="module")
def trained_run(loaded_warehouse, etl_config, pipeline_root):
    code, report = run_mode("train", etl_config, pipeline_root)
    assert code == 0, report["run"]
    return report


@pytest.fixture(scope="module")
def score_runs(trained_run, etl_config, pipeline_root, dwh_engine):
    first = run_mode("score", etl_config, pipeline_root)
    with dwh_engine.connect() as conn:
        between = dict(conn.execute(text(PREDICTION_FINGERPRINT_SQL)).mappings().one())
    second = run_mode("score", etl_config, pipeline_root)
    return first, between, second


def test_train_mode_registers_the_model_and_writes_artifacts(trained_run, pipeline_root):
    train = task_output(trained_run, "train")
    assert train["rows"]["train"] == 24_226 and train["rows"]["test"] == 12_808
    assert (pipeline_root / "models" / train["model_version"] / "model.cbm").is_file()
    assert (pipeline_root / "features" / "X_train.parquet").is_file()
    assert trained_run["counts"]["ml_training_rows"] == 24_226
    assert trained_run["model"]["metrics"]["test"]["n"] == 12_808


def test_score_mode_twice_changes_nothing(score_runs, dwh_engine):
    (code_1, first), between, (code_2, second) = score_runs
    assert code_1 == 0 and code_2 == 0
    for name in ("score_test", "score_future"):
        out = task_output(second, name)
        assert (out["rows_inserted"], out["rows_updated"], out["rows_deleted"]) == (0, 0, 0)
    assert task_output(second, "score_test")["rows_unchanged"] == 12_808
    assert task_output(second, "score_future")["rows_unchanged"] == 4_891
    assert task_output(second, "score_test")["split_roles"] == ["test"]
    with dwh_engine.connect() as conn:
        after = dict(conn.execute(text(PREDICTION_FINGERPRINT_SQL)).mappings().one())
    assert after == between
    assert second["previous_run"]["comparison"]["predictions"] == "unchanged"
    assert second["counts"]["ml_test_rows"] == 12_808 and second["counts"]["ml_future_rows"] == 4_891


def test_separate_role_batches_keep_the_other_role(score_runs, dwh_engine):
    _, _, (_, second) = score_runs
    batch_ids = [task_output(second, name)["scoring_batch_id"] for name in ("score_test", "score_future")]
    with dwh_engine.connect() as conn:
        rows = conn.execute(text(
            "SELECT scoring_batch_id, split_roles, rows_test, rows_future, status FROM audit.ml_scoring_batch "
            "WHERE scoring_batch_id = ANY (:ids) ORDER BY scoring_batch_id"), {"ids": batch_ids}).all()
    assert [(r.split_roles, r.rows_test, r.rows_future, r.status) for r in rows] == [
        (["test"], 12_808, 0, "succeeded"), (["future"], 0, 4_891, "succeeded")]


def test_verify_reports_every_check_passing(score_runs):
    _, _, (_, second) = score_runs
    checks = {check["name"]: check for check in second["integrity"]}
    assert all(check["ok"] for check in checks.values())
    assert checks["future_actuals_null"]["detail"]["with_actuals"] == 0
    assert checks["future_actuals_null"]["detail"]["future_rows"] >= 4_891
    assert checks["foreign_keys_hold"]["detail"]["checked"] >= 25
    assert checks["unique_keys_hold"]["detail"]["checked"] >= 25


def test_run_is_persisted_with_report_file(score_runs, dwh_engine, pipeline_root):
    _, _, (_, second) = score_runs
    run_id = second["run"]["run_id"]
    with dwh_engine.connect() as conn:
        row = conn.execute(text("SELECT mode, status, failed_task, jsonb_array_length(tasks) AS tasks, "
                                "report -> 'counts' ->> 'core_dwh_deals' AS core FROM audit.pipeline_run "
                                "WHERE run_id = :id"), {"id": run_id}).mappings().one()
    assert dict(row) == {"mode": "score", "status": "succeeded", "failed_task": None, "tasks": 5, "core": "299797"}
    latest = json.loads((pipeline_root / "reports" / "latest.json").read_text())
    assert latest["run"]["run_id"] == run_id
    assert latest["persisted"]["audit.pipeline_run"] == run_id
    assert set(latest["durations_s"]) >= {"warehouse_ready", "score_test", "verify", "tasks", "report", "total"}


def test_failed_task_is_reported_and_blocks_dependents(trained_run, etl_config, pipeline_root, dwh_engine,
                                                       monkeypatch):
    def broken(ctx):
        raise RuntimeError("injected scoring failure")

    tasks = tuple(dataclasses.replace(task, run=broken) if task.name == "score_future" else task for task in TASKS)
    monkeypatch.setattr(runner, "TASKS", tasks)
    code, report = run_mode("score", etl_config, pipeline_root)
    assert code == 1
    status = {task["task"]: task["status"] for task in report["tasks"]}
    assert status == {"warehouse_ready": "succeeded", "migrate": "succeeded", "score_test": "succeeded",
                      "score_future": "failed", "verify": "not_run"}
    failure = report["run"]["failed_tasks"][0]
    assert (failure["task"], failure["type"], failure["message"], failure["attempts"]) == (
        "score_future", "RuntimeError", "injected scoring failure", 1)
    assert any("injected scoring failure" in line for line in failure["traceback_tail"])
    with dwh_engine.connect() as conn:
        row = conn.execute(text("SELECT status, failed_task, error_message FROM audit.pipeline_run WHERE run_id = :id"),
                           {"id": report["run"]["run_id"]}).one()
    assert tuple(row) == ("failed", "score_future", "injected scoring failure")


def test_mart_refresh_is_skipped_when_inputs_are_unchanged(loaded_warehouse, dwh_engine):
    refresh_if_changed(dwh_engine, AS_OF)
    again = refresh_if_changed(dwh_engine, AS_OF)
    assert again["status"] == "unchanged" and again["rows_inserted"] == 0 and again["cells"] == 203_644


def test_mart_input_fingerprint_sees_core_changes(loaded_warehouse, dwh_tx):
    before = dwh_tx.execute(text("SELECT mart.market_corridor_input_fingerprint()")).scalar_one()
    dwh_tx.execute(text("UPDATE core.fact_deal SET price_rub = price_rub + 1 "
                        "WHERE price_id = (SELECT min(price_id) FROM core.fact_deal)"))
    after = dwh_tx.execute(text("SELECT mart.market_corridor_input_fingerprint()")).scalar_one()
    assert before != after


def test_mart_fingerprint_columns_are_the_table_minus_lineage(dwh_tx):
    columns = dwh_tx.execute(text(
        "SELECT string_agg(attname, ', ' ORDER BY attnum) FROM pg_attribute "
        "WHERE attrelid = 'mart.mart_market_corridor'::regclass AND attnum > 0 AND NOT attisdropped "
        "AND attname <> 'refresh_id'")).scalar_one()
    assert columns == MART_COLUMNS


def test_integrity_helpers_detect_orphans_and_duplicates(dwh_tx):
    dwh_tx.execute(text("CREATE TEMPORARY TABLE t_parent (id int, code text) ON COMMIT DROP"))
    dwh_tx.execute(text("CREATE TEMPORARY TABLE t_child (parent_id int, code text) ON COMMIT DROP"))
    dwh_tx.execute(text("INSERT INTO t_parent VALUES (1, 'a'), (2, 'b'), (2, 'b'), (3, NULL), (3, NULL)"))
    dwh_tx.execute(text("INSERT INTO t_child VALUES (1, 'a'), (2, 'b'), (9, 'z'), (NULL, 'a'), (1, 'b')"))
    fk = {"child": "t_child", "parent": "t_parent", "columns": ["parent_id", "code"], "ref_columns": ["id", "code"]}
    assert foreign_key_orphans(dwh_tx, fk) == 2
    assert duplicate_keys(dwh_tx, {"child": "t_parent", "columns": ["id", "code"]}) == 1
    assert duplicate_keys(dwh_tx, {"child": "t_parent", "columns": ["id"]}) == 2
