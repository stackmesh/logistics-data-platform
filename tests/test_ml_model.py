"""Model training rules, metrics, artifacts, and feature order. No database."""

from __future__ import annotations

import json
import math
import re
from dataclasses import replace
from datetime import date, timedelta
from pathlib import Path

import numpy as np
import polars as pl
import pytest
from sklearn.metrics import mean_absolute_error, mean_absolute_percentage_error, mean_squared_error, r2_score

from ml.features import extract
from ml.features.dataset import FeatureDataset, assemble
from ml.features.spec import CATEGORICAL_FEATURES, DEFAULT_SPLIT, FEATURE_COLUMNS, FORBIDDEN_COLUMNS, TARGET
from ml.model.artifacts import load_model, publish_latest, save_artifacts, sha256_file
from ml.model.config import TrainingConfig
from ml.model.metrics import regression_metrics
from ml.model.train import chronological_validation_mask, make_pool, model_version, train_model
from ml.scoring.score import STAGE_COLUMNS, predict

ROOT = Path(__file__).resolve().parents[1]
FAST = TrainingConfig(iterations=80, learning_rate=0.2, depth=4, early_stopping_rounds=20, verbose=0,
                      thread_count=2)
CITIES = [("Москва, г. Москва", 55.75, 37.61), ("Казань, Республика Татарстан", 55.79, 49.10),
          ("Самара, Самарская обл.", 53.19, 50.10), ("Пермь, Пермский край", 58.01, 56.25)]
BODY_RATE = {"TENT": 90.0, "REF": 120.0, "BORT": 85.0}


# --- Metrics -----------------------------------------------------------------------------


def test_metrics_hand_computed():
    m = regression_metrics([100.0, 200.0, 300.0, 400.0], [110.0, 190.0, 330.0, 360.0])
    errors = np.array([10.0, -10.0, 30.0, -40.0])
    assert m["mae_rub"] == pytest.approx(22.5)
    assert m["rmse_rub"] == pytest.approx(math.sqrt(np.mean(errors ** 2)))
    assert m["mape_pct"] == pytest.approx((0.1 + 0.05 + 0.1 + 0.1) / 4 * 100)
    assert m["r2"] == pytest.approx(1 - np.sum(errors ** 2) / 50_000)
    assert m["bias_rub"] == pytest.approx(-2.5)
    assert m["n"] == m["mape_rows"] == 4


def test_metrics_match_sklearn():
    rng = np.random.default_rng(7)
    y = rng.lognormal(11.7, 0.9, 500)
    p = y * rng.normal(1, 0.15, 500)
    m = regression_metrics(y, p)
    assert m["r2"] == pytest.approx(r2_score(y, p))
    assert m["mae_rub"] == pytest.approx(mean_absolute_error(y, p))
    assert m["rmse_rub"] == pytest.approx(math.sqrt(mean_squared_error(y, p)))
    assert m["mape_pct"] == pytest.approx(mean_absolute_percentage_error(y, p) * 100)


def test_mape_skips_non_positive_targets():
    m = regression_metrics([0.0, 100.0, -5.0, 200.0], [10.0, 110.0, 1.0, 180.0])
    assert m["mape_pct"] == pytest.approx(10.0)
    assert (m["mape_rows"], m["mape_skipped_non_positive"]) == (2, 2)
    assert m["mae_rub"] == pytest.approx((10 + 10 + 6 + 20) / 4)
    assert regression_metrics([0.0, 0.0], [1.0, 2.0])["mape_pct"] is None
    assert regression_metrics([5.0, 5.0], [4.0, 6.0])["r2"] is None


@pytest.mark.parametrize("actual, predicted", [([1.0, 2.0], [1.0]), ([], []), ([1.0, np.nan], [1.0, 2.0]),
                                               ([1.0, 2.0], [np.inf, 2.0])])
def test_metrics_reject_bad_input(actual, predicted):
    with pytest.raises(ValueError):
        regression_metrics(actual, predicted)


# --- Configuration and time boundaries ---------------------------------------------------


def test_training_config_is_chronological_and_seeded():
    config = TrainingConfig()
    params = config.catboost_params(list(CATEGORICAL_FEATURES))
    assert params["random_seed"] == 42 and params["loss_function"] == "RMSE" and params["verbose"] == 100
    assert params["has_time"] is True
    assert params["cat_features"] == list(CATEGORICAL_FEATURES)
    assert config.validation_start() == date(2026, 9, 21)
    with pytest.raises(ValueError):
        replace(config, validation_days=80).validation_start()


def test_validation_mask_takes_the_last_days_and_requires_order():
    days = [DEFAULT_SPLIT.train_start + timedelta(days=i) for i in range(71)]
    ids = pl.DataFrame({"price_id": range(71), "loading_date": days})
    mask = chronological_validation_mask(ids, date(2026, 9, 21))
    assert ids.filter(~mask)["loading_date"].max() == date(2026, 9, 20)
    assert ids.filter(mask)["loading_date"].min() == date(2026, 9, 21)
    assert ids.filter(mask)["loading_date"].max() == date(2026, 10, 4)
    with pytest.raises(ValueError, match="order"):
        chronological_validation_mask(ids.reverse(), date(2026, 9, 21))


# --- Synthetic training ------------------------------------------------------------------


def synthetic_dataset(n_days: int = 71, per_day: int = 8, test_days: int = 22) -> FeatureDataset:
    rng = np.random.default_rng(20261026)
    rows = []
    price_id = 0
    for day_index in range(n_days + test_days):
        day = DEFAULT_SPLIT.train_start + timedelta(days=day_index)
        for _ in range(per_day):
            price_id += 1
            o, d = rng.choice(len(CITIES), 2, replace=False)
            body = ("TENT", "REF", "BORT")[price_id % 3]
            distance = float(rng.integers(100, 3000))
            rows.append({
                "price_id": price_id, "loading_date": day,
                "origin_full_name": CITIES[o][0], "destination_full_name": CITIES[d][0],
                "origin_latitude": CITIES[o][1], "origin_longitude": CITIES[o][2],
                "destination_latitude": CITIES[d][1], "destination_longitude": CITIES[d][2],
                "distance": distance, "total_weight": float(rng.uniform(1, 20)), "total_volume": 82.0,
                "body_group": body, "weight_category": "FTL", "volume_category": "FTL",
                TARGET: distance * BODY_RATE[body] * float(rng.normal(1, 0.08)) + 5_000,
            })
    base = pl.DataFrame(rows, schema_overrides={"loading_date": pl.Date})
    pipeline, X, y, ids = assemble(base, DEFAULT_SPLIT)
    return FeatureDataset(DEFAULT_SPLIT, pipeline, X, y, ids, funnel={}, core_batch_id=None)


@pytest.fixture(scope="module")
def trained(tmp_path_factory):
    dataset = synthetic_dataset()
    root = tmp_path_factory.mktemp("artifacts")
    result = train_model(dataset, FAST)
    card = save_artifacts(result, root)
    publish_latest(root, card)
    return dataset, result, card, root


def test_training_is_reproducible(trained, tmp_path):
    dataset, result, card, _ = trained
    again = train_model(dataset, FAST)
    assert again.model_version == result.model_version == model_version(dataset, FAST)
    assert np.array_equal(again.test_predictions, result.test_predictions)
    assert again.best_iteration == result.best_iteration
    second = save_artifacts(again, tmp_path)
    assert second["model_sha256"] == card["model_sha256"]
    assert {k: v for k, v in second.items()} == card
    assert not (tmp_path / "latest.json").exists()


def test_model_version_changes_with_data_or_config(trained):
    dataset, result, _, _ = trained
    assert model_version(dataset, replace(FAST, depth=5)) != result.model_version
    shifted = synthetic_dataset(per_day=7)
    assert model_version(shifted, FAST) != result.model_version


def test_model_learns_and_beats_nothing_trivial(trained):
    _, result, _, _ = trained
    assert result.metrics["test"]["r2"] > 0.8
    assert result.metrics["test"]["n"] == 22 * 8
    assert result.fit_rows + result.validation_rows == 71 * 8
    assert result.validation_rows == 14 * 8
    assert set(result.baselines) == {"ridge", "decision_tree", "median_rate_per_km"}


def test_model_card_records_everything(trained):
    _, result, card, root = trained
    assert card["model_version"] == result.model_version
    assert card["random_seed"] == 42
    assert card["train_period"] == {"start": "2026-07-26", "end_inclusive": "2026-10-04"}
    assert card["validation_period"]["start"] == "2026-09-21"
    assert card["test_period"] == {"start": "2026-10-05", "end_inclusive": "2026-10-26"}
    assert card["feature_columns"] == list(FEATURE_COLUMNS)
    assert card["categorical_features"] == list(CATEGORICAL_FEATURES)
    assert card["training_config"] == FAST.to_dict()
    assert card["catboost_params"]["has_time"] is True and card["catboost_params"]["random_seed"] == 42
    assert card["final_iterations"] == card["best_iteration"] + 1
    assert set(card["metrics"]) == {"validation_probe", "train", "test"}
    assert set(card["metrics"]["test"]) >= {"r2", "mape_pct", "rmse_rub", "mae_rub"}
    assert set(card["baseline_metrics"]) == {"ridge", "decision_tree", "median_rate_per_km"}
    assert card["categorical_encoding"]["vocabulary"]["origin_city"]
    directory = root / card["model_version"]
    assert sha256_file(directory / "model.cbm") == card["model_sha256"]
    assert json.loads((root / "latest.json").read_text())["model_version"] == card["model_version"]


def test_saved_model_round_trips(trained):
    dataset, result, card, root = trained
    model, loaded_card, pipeline = load_model(root)
    assert loaded_card == json.loads(json.dumps(card, default=str))
    assert np.array_equal(predict(model, dataset.X["test"]), result.test_predictions)
    assert pipeline.vocabulary == dataset.pipeline.vocabulary


def test_tampered_model_file_is_rejected(trained, tmp_path):
    _, result, _, _ = trained
    card = save_artifacts(result, tmp_path)
    publish_latest(tmp_path, card)
    path = tmp_path / card["model_version"] / "model.cbm"
    path.write_bytes(path.read_bytes() + b"x")
    with pytest.raises(ValueError, match="sha256"):
        load_model(tmp_path)


# --- Feature order and leakage ------------------------------------------------------------


def test_feature_order_is_enforced(trained):
    dataset, result, _, _ = trained
    assert list(result.model.feature_names_) == list(FEATURE_COLUMNS)
    X = dataset.X["test"]
    with pytest.raises(ValueError):
        predict(result.model, X.select(list(reversed(X.columns))))
    with pytest.raises(ValueError):
        make_pool(X.select(list(reversed(X.columns))))
    with pytest.raises(ValueError):
        make_pool(X.with_columns(pl.lit(1.0).alias("price_rub")))


def test_model_is_not_given_the_target(trained):
    dataset, result, _, _ = trained
    importances = dict(zip(result.model.feature_names_, result.model.get_feature_importance(), strict=True))
    assert set(importances) == set(FEATURE_COLUMNS)
    assert not set(importances) & FORBIDDEN_COLUMNS
    assert importances["distance"] == max(importances.values())


def test_future_query_reads_no_price_or_price_derived_flag():
    select_list = extract.FUTURE_SQL.split("FROM core.fact_deal")[0]
    for name in (*FORBIDDEN_COLUMNS, "is_fact", "price_type"):
        assert not re.search(rf"\b{name}\b", select_list), name
    for name in ("price_rub", "price_src", "outlier_mark", "is_fact", "fx_rate", "price_group"):
        assert name not in extract.FUTURE_SQL, name
    assert "loading_date > :as_of" in extract.FUTURE_PREDICATE
    assert STAGE_COLUMNS == ("price_id", "split_role", "predicted_price_rub")


def test_model_code_does_not_shuffle_or_read_the_clock():
    sources = [*(ROOT / "ml" / "model").glob("*.py"), *(ROOT / "ml" / "scoring").glob("*.py")]
    text = "\n".join(path.read_text() for path in sources)
    for pattern in (r"train_test_split", r"shuffle", r"\.sample\(", r"KFold", r"date\.today", r"datetime\.now",
                    r"CURRENT_DATE"):
        assert not re.search(pattern, text), pattern
