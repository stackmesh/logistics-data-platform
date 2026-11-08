"""Train the CatBoost price model and its baselines, evaluate on the test window, save artifacts.

python -m ml.model.train
"""

from __future__ import annotations

import argparse
import hashlib
import io
import json
import logging
import time
from dataclasses import dataclass, field
from datetime import timedelta

import catboost
import numpy as np
import polars as pl
import sklearn
from catboost import CatBoostRegressor, Pool
from sqlalchemy import text
from sqlalchemy.engine import Connection

from etl.config import load_config, warehouse_engine
from etl.log import configure_logging, log_event
from ml.features.dataset import FeatureDataset, assert_no_leakage, build_dataset, metadata, seed_everything
from ml.features.spec import CATEGORICAL_FEATURES, FEATURE_COLUMNS, NUMERIC_FEATURES, SPEC_VERSION
from ml.model import baselines
from ml.model.artifacts import publish_latest, save_artifacts
from ml.model.config import TrainingConfig, artifact_root
from ml.model.metrics import regression_metrics

logger = logging.getLogger("ml.train")
CAT_INDICES = [FEATURE_COLUMNS.index(name) for name in CATEGORICAL_FEATURES]


@dataclass
class TrainingResult:
    config: TrainingConfig
    dataset: FeatureDataset
    model: CatBoostRegressor
    model_version: str
    best_iteration: int
    validation_rows: int
    fit_rows: int
    metrics: dict
    baselines: dict = field(default_factory=dict)
    test_predictions: np.ndarray | None = None


def make_pool(X: pl.DataFrame, y: pl.Series | None = None) -> Pool:
    assert_no_leakage(X.columns)
    return Pool(baselines.to_matrix(X), label=None if y is None else y.to_numpy(),
                cat_features=CAT_INDICES, feature_names=list(FEATURE_COLUMNS))


def chronological_validation_mask(ids: pl.DataFrame, validation_start) -> pl.Series:
    """True for the last days of the train window. Requires rows already in loading_date order."""
    dates = ids["loading_date"]
    if not dates.is_sorted():
        raise ValueError("train rows must be in loading_date order")
    return dates >= validation_start


def model_version(dataset: FeatureDataset, config: TrainingConfig) -> str:
    digests = metadata(dataset)["digests"]
    payload = json.dumps({
        "config": config.to_dict(), "feature_spec": SPEC_VERSION, "features": list(FEATURE_COLUMNS),
        "split": dataset.split.to_dict(), "train": digests["train"], "catboost": catboost.__version__,
    }, sort_keys=True)
    return f"{dataset.split.as_of:%Y%m%d}-{hashlib.sha256(payload.encode()).hexdigest()[:12]}"


class CatBoostLog(io.TextIOBase):
    """File-like sink that turns CatBoost's verbose text into JSON log events, one per line."""

    def __init__(self, level: int = logging.INFO) -> None:
        super().__init__()
        self._level = level
        self._buffer = ""

    def writable(self) -> bool:
        return True

    def write(self, chunk: str) -> int:
        self._buffer += chunk
        *lines, self._buffer = self._buffer.split("\n")
        for line in lines:
            self._emit(line)
        return len(chunk)

    def flush(self) -> None:
        if self._buffer:
            self._emit(self._buffer)
            self._buffer = ""

    def _emit(self, line: str) -> None:
        if line.strip():
            log_event(logger, "catboost_progress", self._level, line=" ".join(line.split()))


def _fit_catboost(config: TrainingConfig, train: Pool, eval_set: Pool | None, iterations: int | None):
    model = CatBoostRegressor(**config.catboost_params(list(CATEGORICAL_FEATURES), iterations))
    out, err = CatBoostLog(), CatBoostLog(logging.WARNING)
    if eval_set is None:
        model.fit(train, log_cout=out, log_cerr=err)
    else:
        model.fit(train, eval_set=eval_set, early_stopping_rounds=config.early_stopping_rounds, use_best_model=True,
                  log_cout=out, log_cerr=err)
    out.flush()
    err.flush()
    return model


def train_model(dataset: FeatureDataset, config: TrainingConfig = TrainingConfig()) -> TrainingResult:
    seed_everything(config.random_seed)
    X, y, ids = dataset.X["train"], dataset.y["train"], dataset.ids["train"]
    validation = chronological_validation_mask(ids, config.validation_start(dataset.split))
    fit_X, fit_y = X.filter(~validation), y.filter(~validation)
    val_X, val_y = X.filter(validation), y.filter(validation)

    # Early stopping on the chronological hold-out picks the tree count.
    probe = _fit_catboost(config, make_pool(fit_X, fit_y), make_pool(val_X, val_y), None)
    best_iteration = int(probe.get_best_iteration())
    validation_metrics = regression_metrics(val_y.to_numpy(), probe.predict(make_pool(val_X)))

    # The published model uses that tree count on the whole train window, so the last weeks count.
    model = _fit_catboost(config, make_pool(X, y), None, best_iteration + 1)
    test_X, test_y = dataset.X["test"], dataset.y["test"]
    test_predictions = model.predict(make_pool(test_X))
    result = TrainingResult(
        config=config, dataset=dataset, model=model, model_version=model_version(dataset, config),
        best_iteration=best_iteration, validation_rows=val_X.height, fit_rows=fit_X.height,
        metrics={"validation_probe": validation_metrics,
                 "train": regression_metrics(y.to_numpy(), model.predict(make_pool(X))),
                 "test": regression_metrics(test_y.to_numpy(), test_predictions)},
        test_predictions=test_predictions,
    )
    result.baselines = fit_baselines(dataset, config)
    return result


def fit_baselines(dataset: FeatureDataset, config: TrainingConfig) -> dict:
    X, y = dataset.X["train"], dataset.y["train"]
    test_X, test_y = dataset.X["test"], dataset.y["test"]
    results = {}
    for name, factory in (("ridge", baselines.ridge), ("decision_tree", baselines.decision_tree)):
        model = factory(config.random_seed).fit(baselines.to_matrix(X), y.to_numpy())
        results[name] = {"model": model,
                         "test": regression_metrics(test_y.to_numpy(), model.predict(baselines.to_matrix(test_X)))}
    rate = baselines.RatePerKmBaseline().fit(X, y)
    results["median_rate_per_km"] = {"model": rate, "test": regression_metrics(test_y.to_numpy(), rate.predict(test_X))}
    return results


def model_card(result: TrainingResult, model_sha256: str) -> dict:
    split = result.dataset.split
    feature_meta = metadata(result.dataset)
    importances = result.model.get_feature_importance(type="PredictionValuesChange")
    return {
        "model_name": result.config.model_name,
        "model_version": result.model_version,
        "algorithm": "catboost.CatBoostRegressor",
        "library_versions": {"catboost": catboost.__version__, "scikit-learn": sklearn.__version__,
                             "polars": pl.__version__, "numpy": np.__version__},
        "random_seed": result.config.random_seed,
        "as_of": split.as_of.isoformat(),
        "train_period": {"start": split.train_start.isoformat(),
                         "end_inclusive": (split.test_start - timedelta(days=1)).isoformat()},
        "validation_period": {"start": result.config.validation_start(split).isoformat(),
                              "end_inclusive": (split.test_start - timedelta(days=1)).isoformat(),
                              "use": "early stopping only; the final model is refit on the whole train period"},
        "test_period": {"start": split.test_start.isoformat(), "end_inclusive": split.as_of.isoformat()},
        "training_config": result.config.to_dict(),
        "catboost_params": {k: v for k, v in result.model.get_params().items()},
        "best_iteration": result.best_iteration,
        "final_iterations": result.model.tree_count_,
        "feature_columns": list(FEATURE_COLUMNS),
        "categorical_features": list(CATEGORICAL_FEATURES),
        "numeric_features": list(NUMERIC_FEATURES),
        "feature_definitions": feature_meta["features"],
        "categorical_encoding": result.dataset.pipeline.to_dict(),
        "target": feature_meta["target"],
        "rows": {"fit": result.fit_rows, "validation": result.validation_rows,
                 "train": result.dataset.X["train"].height, "test": result.dataset.X["test"].height},
        "data_digests": feature_meta["digests"],
        "core_batch_id": result.dataset.core_batch_id,
        "metrics": result.metrics,
        "baseline_metrics": {name: entry["test"] for name, entry in result.baselines.items()},
        "feature_importance": dict(sorted(
            ((name, round(float(value), 4)) for name, value in zip(FEATURE_COLUMNS, importances, strict=True)),
            key=lambda item: -item[1])),
        "model_file": "model.cbm",
        "model_sha256": model_sha256,
    }


REGISTER_SQL = """
INSERT INTO ml.ml_model_registry (
    model_name, model_version, algorithm, as_of_date, train_start, train_end, test_start, test_end,
    random_seed, feature_columns, categorical_features, training_config, metrics, baseline_metrics,
    model_sha256, training_rows, validation_rows, best_iteration, core_batch_id
) VALUES (
    :model_name, :model_version, :algorithm, :as_of, :train_start, :train_end, :test_start, :test_end,
    :random_seed, :feature_columns, :categorical_features, CAST(:training_config AS jsonb),
    CAST(:metrics AS jsonb), CAST(:baseline_metrics AS jsonb), :model_sha256, :training_rows,
    :validation_rows, :best_iteration, :core_batch_id
)
ON CONFLICT (model_name, model_version) DO UPDATE SET
    metrics = EXCLUDED.metrics,
    baseline_metrics = EXCLUDED.baseline_metrics,
    model_sha256 = EXCLUDED.model_sha256,
    core_batch_id = EXCLUDED.core_batch_id,
    registered_at = clock_timestamp()
"""


def register_model(conn: Connection, card: dict) -> None:
    conn.execute(text(REGISTER_SQL), {
        "model_name": card["model_name"], "model_version": card["model_version"], "algorithm": card["algorithm"],
        "as_of": card["as_of"], "train_start": card["train_period"]["start"],
        "train_end": card["train_period"]["end_inclusive"], "test_start": card["test_period"]["start"],
        "test_end": card["test_period"]["end_inclusive"], "random_seed": card["random_seed"],
        "feature_columns": card["feature_columns"], "categorical_features": card["categorical_features"],
        "training_config": json.dumps({"config": card["training_config"], "catboost_params": card["catboost_params"]},
                                      default=str),
        "metrics": json.dumps(card["metrics"]), "baseline_metrics": json.dumps(card["baseline_metrics"]),
        "model_sha256": card["model_sha256"], "training_rows": card["rows"]["train"],
        "validation_rows": card["rows"]["validation"], "best_iteration": card["best_iteration"],
        "core_batch_id": card["core_batch_id"],
    })


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="python -m ml.model.train")
    parser.parse_args(argv)
    configure_logging()
    started = time.monotonic()
    engine = warehouse_engine(load_config())
    try:
        with engine.connect() as conn, conn.begin():
            conn.execute(text("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY"))
            dataset = build_dataset(conn)
        result = train_model(dataset)
        card = save_artifacts(result, artifact_root())
        with engine.begin() as conn:
            register_model(conn, card)
        publish_latest(artifact_root(), card)
    finally:
        engine.dispose()
    log_event(logger, "model_trained", model_name=card["model_name"], model_version=card["model_version"],
              best_iteration=card["best_iteration"], test=card["metrics"]["test"],
              baselines=card["baseline_metrics"], duration_s=round(time.monotonic() - started, 3))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
