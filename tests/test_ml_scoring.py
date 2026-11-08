"""Training, artifacts, and batch scoring against the real warehouse."""

from __future__ import annotations

from datetime import date

import pytest
from sqlalchemy import text
from sqlalchemy.exc import IntegrityError

from ml.features.dataset import build_dataset
from ml.features.extract import FUTURE_PREDICATE
from ml.features.spec import FEATURE_COLUMNS
from ml.model.artifacts import load_model, publish_latest, save_artifacts
from ml.model.train import register_model, train_model
from ml.scoring import score as scoring

pytestmark = pytest.mark.integration

ELIGIBLE = "is_fact AND NOT complex AND NOT outlier_mark AND price_rub > 0 AND distance_km > 0"
PREDICTIONS_HASH = """
SELECT count(*) AS n,
       md5(string_agg(md5(ROW(price_id, split_role, loading_date, loading_city_gcid, unloading_city_gcid,
           body_group, actual_price_rub, predicted_price_rub, abs_error_rub, ape_pct)::text), ''
           ORDER BY price_id)) AS content
FROM ml.ml_price_predictions WHERE model_name = :name AND model_version = :version
"""


@pytest.fixture(scope="module")
def trained(loaded_warehouse, dwh_engine, tmp_path_factory):
    with dwh_engine.connect() as conn:
        dataset = build_dataset(conn)
    result = train_model(dataset)
    root = tmp_path_factory.mktemp("ml_artifacts")
    card = save_artifacts(result, root)
    with dwh_engine.begin() as conn:
        register_model(conn, card)
    publish_latest(root, card)
    return dataset, result, card, root


@pytest.fixture(scope="module")
def scored(trained, dwh_engine):
    _, _, card, root = trained
    first = scoring.score(dwh_engine, root)
    second = scoring.score(dwh_engine, root)
    return first, second


def _params(card):
    return {"name": card["model_name"], "version": card["model_version"]}


def test_artifact_is_reproducible(trained, tmp_path):
    dataset, result, card, _ = trained
    again = train_model(dataset)
    again_card = save_artifacts(again, tmp_path)
    assert again_card["model_version"] == card["model_version"]
    assert again_card["model_sha256"] == card["model_sha256"]
    assert again_card["metrics"] == card["metrics"]


def test_artifact_contents(trained):
    _, result, card, root = trained
    model, loaded, _ = load_model(root, card["model_version"])
    assert list(model.feature_names_) == list(FEATURE_COLUMNS) == loaded["feature_columns"]
    assert loaded["random_seed"] == 42
    assert loaded["train_period"] == {"start": "2026-07-26", "end_inclusive": "2026-10-04"}
    assert loaded["test_period"] == {"start": "2026-10-05", "end_inclusive": "2026-10-26"}
    assert loaded["rows"]["train"] == result.dataset.X["train"].height
    assert loaded["metrics"]["test"]["n"] == result.dataset.X["test"].height


def test_registry_row(trained, dwh_tx):
    _, _, card, _ = trained
    row = dwh_tx.execute(text(
        "SELECT * FROM ml.ml_model_registry WHERE model_name = :name AND model_version = :version"
    ), _params(card)).mappings().one()
    assert row["train_start"] == date(2026, 7, 26) and row["train_end"] == date(2026, 10, 4)
    assert row["test_start"] == date(2026, 10, 5) and row["test_end"] == date(2026, 10, 26)
    assert list(row["feature_columns"]) == list(FEATURE_COLUMNS)
    assert row["random_seed"] == 42 and row["model_sha256"] == card["model_sha256"]


def test_scored_rows_respect_time_boundaries(trained, scored, dwh_tx):
    _, _, card, _ = trained
    stats = dwh_tx.execute(text("""
        SELECT split_role, count(*) AS n, min(loading_date) AS first, max(loading_date) AS last
        FROM ml.ml_price_predictions WHERE model_name = :name AND model_version = :version
        GROUP BY split_role
    """), _params(card)).mappings().all()
    by_role = {row["split_role"]: row for row in stats}
    assert set(by_role) == {"test", "future"}
    assert by_role["test"]["first"] >= date(2026, 10, 5) and by_role["test"]["last"] <= date(2026, 10, 26)
    assert by_role["future"]["first"] > date(2026, 10, 26)
    expected_test = dwh_tx.execute(text(
        f"SELECT count(*) FROM core.fact_deal WHERE {ELIGIBLE} AND loading_date BETWEEN '2026-10-05' AND '2026-10-26'"
    )).scalar_one()
    expected_future = dwh_tx.execute(text(f"SELECT count(*) FROM core.fact_deal AS f WHERE {FUTURE_PREDICATE}"),
                                     {"as_of": date(2026, 10, 26)}).scalar_one()
    assert by_role["test"]["n"] == expected_test
    assert by_role["future"]["n"] == expected_future
    assert scored[0]["rows_test"] == expected_test and scored[0]["rows_future"] == expected_future
    train_overlap = dwh_tx.execute(text(
        "SELECT count(*) FROM ml.ml_price_predictions WHERE model_version = :version AND loading_date < '2026-10-05'"
    ), _params(card)).scalar_one()
    assert train_overlap == 0


def test_future_actuals_stay_null_although_the_source_has_a_price(trained, scored, dwh_tx):
    _, _, card, _ = trained
    row = dwh_tx.execute(text("""
        SELECT count(*) AS n,
               count(p.actual_price_rub) AS actual, count(p.abs_error_rub) AS abs_error, count(p.ape_pct) AS ape,
               count(f.price_src) AS source_price, count(*) FILTER (WHERE f.is_fact) AS flagged_fact
        FROM ml.ml_price_predictions AS p JOIN core.fact_deal AS f USING (price_id)
        WHERE p.model_name = :name AND p.model_version = :version AND p.split_role = 'future'
    """), _params(card)).mappings().one()
    assert row["n"] > 0
    assert row["actual"] == row["abs_error"] == row["ape"] == 0
    assert row["source_price"] == row["n"], "the snapshot gives every planned deal a source price"
    assert row["flagged_fact"] > 0


def test_test_actuals_and_errors(trained, scored, dwh_tx):
    _, _, card, _ = trained
    bad = dwh_tx.execute(text("""
        SELECT count(*) FROM ml.ml_price_predictions AS p JOIN core.fact_deal AS f USING (price_id)
        WHERE p.model_name = :name AND p.model_version = :version AND p.split_role = 'test'
          AND (p.actual_price_rub IS DISTINCT FROM f.price_rub
               OR p.abs_error_rub <> abs(p.actual_price_rub - p.predicted_price_rub)
               OR abs(p.ape_pct - p.abs_error_rub / p.actual_price_rub * 100) > 0.0001
               OR p.loading_date <> f.loading_date OR p.body_group <> f.body_group_code
               OR p.loading_city_gcid <> f.loading_city_gcid OR p.unloading_city_gcid <> f.unloading_city_gcid)
    """), _params(card)).scalar_one()
    assert bad == 0


def test_database_metrics_equal_training_metrics(trained, scored, dwh_tx):
    _, _, card, _ = trained
    summary = dwh_tx.execute(text(
        "SELECT * FROM ml.v_prediction_error_summary WHERE model_name = :name AND model_version = :version "
        "AND record_type = 'historical'"), _params(card)).mappings().one()
    expected = card["metrics"]["test"]
    assert summary["deals"] == expected["n"]
    assert float(summary["r2"]) == pytest.approx(expected["r2"], abs=1e-5)
    assert float(summary["mae_rub"]) == pytest.approx(expected["mae_rub"], abs=0.01)
    assert float(summary["rmse_rub"]) == pytest.approx(expected["rmse_rub"], abs=0.01)
    assert float(summary["mape_pct"]) == pytest.approx(expected["mape_pct"], abs=1e-3)


def test_prediction_mart_has_the_brief_columns(trained, scored, dwh_tx):
    _, _, card, _ = trained
    columns = dwh_tx.execute(text("""
        SELECT column_name FROM information_schema.columns
        WHERE table_schema = 'mart' AND table_name = 'ml_price_predictions' ORDER BY ordinal_position
    """)).scalars().all()
    assert columns[:11] == ["price_id", "loading_date", "loading_city_gcid", "unloading_city_gcid", "body_group",
                            "actual_price_rub", "predicted_price_rub", "abs_error_rub", "ape_pct", "model_name",
                            "scored_at"]
    row = dwh_tx.execute(text("""
        SELECT count(*) AS n,
               count(*) FILTER (WHERE split_role = 'future'
                                AND (actual_price_rub IS NOT NULL OR abs_error_rub IS NOT NULL
                                     OR ape_pct IS NOT NULL)) AS future_with_actual
        FROM mart.ml_price_predictions WHERE model_name = :name AND model_version = :version
    """), _params(card)).mappings().one()
    assert row["n"] == scored[1]["rows_test"] + scored[1]["rows_future"]
    assert row["future_with_actual"] == 0


def test_predictions_are_unique(trained, scored, dwh_tx):
    _, _, card, _ = trained
    duplicates = dwh_tx.execute(text("""
        SELECT count(*) FROM (SELECT price_id FROM ml.ml_price_predictions
                              WHERE model_name = :name AND model_version = :version
                              GROUP BY price_id HAVING count(*) > 1) AS d
    """), _params(card)).scalar_one()
    assert duplicates == 0
    key = dwh_tx.execute(text("""
        SELECT array_agg(a.attname ORDER BY k.ord) FROM pg_constraint AS c
        CROSS JOIN LATERAL unnest(c.conkey) WITH ORDINALITY AS k(attnum, ord)
        JOIN pg_attribute AS a ON a.attrelid = c.conrelid AND a.attnum = k.attnum
        WHERE c.conrelid = 'ml.ml_price_predictions'::regclass AND c.contype = 'p'
    """)).scalar_one()
    assert list(key) == ["price_id", "model_name", "model_version"]
    with pytest.raises(IntegrityError):
        with dwh_tx.begin_nested():
            dwh_tx.execute(text("""
                INSERT INTO ml.ml_price_predictions
                SELECT * FROM ml.ml_price_predictions WHERE model_name = :name AND model_version = :version LIMIT 1
            """), _params(card))


def test_future_row_with_an_actual_is_rejected(trained, scored, dwh_tx):
    _, _, card, _ = trained
    with pytest.raises(IntegrityError):
        with dwh_tx.begin_nested():
            dwh_tx.execute(text("""
                UPDATE ml.ml_price_predictions SET actual_price_rub = 1000
                WHERE model_name = :name AND model_version = :version AND split_role = 'future'
            """), _params(card))


def test_repeated_scoring_changes_nothing(trained, scored, dwh_engine):
    _, _, card, root = trained
    first, second = scored
    assert first["rows_inserted"] + first["rows_updated"] + first["rows_unchanged"] == \
        first["rows_test"] + first["rows_future"]
    assert (second["rows_inserted"], second["rows_updated"], second["rows_deleted"]) == (0, 0, 0)
    assert second["rows_unchanged"] == second["rows_test"] + second["rows_future"]
    with dwh_engine.connect() as conn:
        before = dict(conn.execute(text(PREDICTIONS_HASH), _params(card)).mappings().one())
    third = scoring.score(dwh_engine, root)
    with dwh_engine.connect() as conn:
        after = dict(conn.execute(text(PREDICTIONS_HASH), _params(card)).mappings().one())
        batches = conn.execute(text(
            "SELECT status, rows_test, rows_future, test_metrics FROM audit.ml_scoring_batch "
            "WHERE scoring_batch_id IN (:a, :b, :c)"), {"a": first["scoring_batch_id"],
                                                         "b": second["scoring_batch_id"],
                                                         "c": third["scoring_batch_id"]}).mappings().all()
    assert after == before and after["n"] == third["rows_test"] + third["rows_future"]
    assert len(batches) == 3 and all(b["status"] == "succeeded" and b["test_metrics"] for b in batches)


def test_failed_scoring_rolls_back(trained, scored, dwh_engine, monkeypatch):
    _, _, card, root = trained
    with dwh_engine.connect() as conn:
        before = dict(conn.execute(text(PREDICTIONS_HASH), _params(card)).mappings().one())
    real_write = scoring.write_predictions

    def write_then_fail(conn, frame, card_, batch_id, roles):
        real_write(conn, frame.with_columns(frame["predicted_price_rub"] * 2), card_, batch_id, roles)
        raise RuntimeError("injected failure after upsert")

    monkeypatch.setattr(scoring, "write_predictions", write_then_fail)
    with pytest.raises(RuntimeError, match="injected"):
        scoring.score(dwh_engine, root)
    with dwh_engine.connect() as conn:
        after = dict(conn.execute(text(PREDICTIONS_HASH), _params(card)).mappings().one())
        status, message = conn.execute(text(
            "SELECT status, error_message FROM audit.ml_scoring_batch ORDER BY scoring_batch_id DESC LIMIT 1"
        )).one()
    assert after == before
    assert status == "failed" and "injected" in message


def test_ml_objects_do_not_read_raw_or_group_stats(trained, dwh_tx):
    definitions = dwh_tx.execute(text("""
        SELECT pg_get_viewdef(c.oid) FROM pg_class AS c JOIN pg_namespace AS n ON n.oid = c.relnamespace
        WHERE n.nspname = 'ml' AND c.relkind = 'v'
        UNION ALL
        SELECT pg_get_functiondef(p.oid) FROM pg_proc AS p JOIN pg_namespace AS n ON n.oid = p.pronamespace
        WHERE n.nspname = 'ml'
    """)).scalars().all()
    assert len(definitions) >= 4
    for definition in definitions:
        assert "price_group_stats" not in definition and "raw." not in definition
        assert "price_src" not in definition
