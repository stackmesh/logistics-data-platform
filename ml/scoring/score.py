"""Score held-out test deals and planned future deals with a saved model.

python -m ml.scoring.score [--model-version VERSION]
"""

from __future__ import annotations

import argparse
import json
import logging
import time
from datetime import date
from pathlib import Path

import numpy as np
import polars as pl
from catboost import CatBoostRegressor
from sqlalchemy import text
from sqlalchemy.engine import Connection, Engine

from etl.config import load_config, warehouse_engine
from etl.load.copy import copy_frame
from etl.log import configure_logging, log_event
from ml.features.dataset import assert_no_leakage, split_roles
from ml.features.extract import read_future_population, read_population
from ml.features.spec import FEATURE_COLUMNS, SplitWindows
from ml.features.transform import FeaturePipeline
from ml.model.artifacts import load_model
from ml.model.config import artifact_root
from ml.model.train import make_pool

logger = logging.getLogger("ml.scoring")
LOCK_KEY = "logistics_dwh.core_etl"
MAX_ERROR_LENGTH = 4000
STAGE_COLUMNS = ("price_id", "split_role", "predicted_price_rub")

STATS_SQL = """
SELECT record_type, deals, mae_rub, rmse_rub, mape_pct, r2, bias_rub, median_ape_pct, p90_ape_pct,
       within_10_pct, within_20_pct, mean_predicted_rub, first_loading_date, last_loading_date
FROM ml.v_prediction_error_summary
WHERE model_name = :model_name AND model_version = :model_version
"""


def split_from_card(card: dict) -> SplitWindows:
    return SplitWindows(as_of=date.fromisoformat(card["as_of"]),
                        train_start=date.fromisoformat(card["train_period"]["start"]),
                        test_start=date.fromisoformat(card["test_period"]["start"]))


def features_for(base: pl.DataFrame, pipeline: FeaturePipeline) -> pl.DataFrame:
    X = pipeline.transform(base)
    assert_no_leakage(X.columns)
    return X


def predict(model: CatBoostRegressor, X: pl.DataFrame) -> np.ndarray:
    if list(model.feature_names_) != list(X.columns) or tuple(X.columns) != FEATURE_COLUMNS:
        raise ValueError("feature order differs from the model")
    return model.predict(make_pool(X))


def score_frame(conn: Connection, model: CatBoostRegressor, card: dict, pipeline: FeaturePipeline) -> pl.DataFrame:
    """Predictions for the test window and for planned deals. Reads no target for future rows."""
    split = split_from_card(card)
    window = read_population(conn, split)
    test_base = window.filter(split_roles(window, split) == "test")
    future_base = read_future_population(conn, split)
    parts = []
    for role, base in (("test", test_base), ("future", future_base)):
        if base.height == 0:
            continue
        predictions = predict(model, features_for(base, pipeline))
        parts.append(pl.DataFrame({"price_id": base["price_id"], "split_role": [role] * base.height,
                                   "predicted_price_rub": predictions}))
    return pl.concat(parts) if parts else pl.DataFrame(schema={"price_id": pl.Int64, "split_role": pl.String,
                                                               "predicted_price_rub": pl.Float64})


def write_predictions(conn: Connection, frame: pl.DataFrame, card: dict, batch_id: int) -> dict:
    """Stage with COPY and upsert in the caller's transaction."""
    if frame.get_column("price_id").is_duplicated().any():
        raise ValueError("duplicate price_id in scoring frame")
    if not np.isfinite(frame["predicted_price_rub"].to_numpy()).all():
        raise ValueError("non-finite prediction")
    conn.execute(text(
        "CREATE TEMPORARY TABLE stage_price_predictions ("
        "price_id bigint PRIMARY KEY, split_role text NOT NULL, predicted_price_rub double precision NOT NULL"
        ") ON COMMIT DROP"
    ))
    copy_frame(conn, "stage_price_predictions", STAGE_COLUMNS, frame)
    conn.execute(text("ANALYZE stage_price_predictions"))
    inserted, updated, deleted = conn.execute(
        text("SELECT * FROM ml.upsert_price_predictions(:name, :version, :as_of, :batch_id)"),
        {"name": card["model_name"], "version": card["model_version"], "as_of": card["as_of"], "batch_id": batch_id},
    ).one()
    return {"rows_inserted": int(inserted), "rows_updated": int(updated),
            "rows_unchanged": frame.height - int(inserted) - int(updated), "rows_deleted": int(deleted)}


def score(engine: Engine, root: Path, model_version: str | None = None) -> dict:
    model, card, pipeline = load_model(root, model_version)
    started = time.perf_counter()
    with engine.begin() as conn:
        batch_id = conn.execute(text(
            "INSERT INTO audit.ml_scoring_batch (model_name, model_version, as_of_date, status) "
            "VALUES (:name, :version, :as_of, 'running') RETURNING scoring_batch_id"
        ), {"name": card["model_name"], "version": card["model_version"], "as_of": card["as_of"]}).scalar_one()
    log_event(logger, "scoring_started", scoring_batch_id=batch_id, model_version=card["model_version"])
    try:
        with engine.begin() as conn:
            conn.execute(text("SELECT pg_advisory_xact_lock(hashtext(:key))"), {"key": LOCK_KEY})
            frame = score_frame(conn, model, card, pipeline)
            counts = write_predictions(conn, frame, card, batch_id)
            stats = {row.record_type: dict(row._mapping) for row in conn.execute(
                text(STATS_SQL), {"model_name": card["model_name"], "model_version": card["model_version"]})}
            roles = frame.group_by("split_role").len()
            rows = {role: n for role, n in roles.iter_rows()}
            conn.execute(text(
                "UPDATE audit.ml_scoring_batch SET status = 'succeeded', completed_at = clock_timestamp(), "
                "rows_test = :test, rows_future = :future, rows_inserted = :inserted, rows_updated = :updated, "
                "rows_unchanged = :unchanged, rows_deleted = :deleted, test_metrics = CAST(:test_metrics AS jsonb), "
                "future_summary = CAST(:future_summary AS jsonb) WHERE scoring_batch_id = :batch_id"
            ), {"test": rows.get("test", 0), "future": rows.get("future", 0), **{
                k.removeprefix("rows_"): v for k, v in counts.items()},
                "test_metrics": json.dumps(stats.get("historical"), default=str),
                "future_summary": json.dumps(stats.get("future"), default=str), "batch_id": batch_id})
    except Exception as exc:
        with engine.begin() as conn:
            conn.execute(text(
                "UPDATE audit.ml_scoring_batch SET status = 'failed', completed_at = clock_timestamp(), "
                "error_message = :message WHERE scoring_batch_id = :batch_id AND status = 'running'"
            ), {"message": f"{type(exc).__name__}: {exc}"[:MAX_ERROR_LENGTH], "batch_id": batch_id})
        log_event(logger, "scoring_failed", logging.ERROR, scoring_batch_id=batch_id, error=f"{type(exc).__name__}: {exc}")
        raise
    result = {"scoring_batch_id": batch_id, "model_name": card["model_name"], "model_version": card["model_version"],
              "rows_test": rows.get("test", 0), "rows_future": rows.get("future", 0), **counts,
              "duration_s": round(time.perf_counter() - started, 3), "summary": stats}
    log_event(logger, "scoring_completed", **result)
    return result


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="python -m ml.scoring.score")
    parser.add_argument("--model-version", help="default: ml/artifacts/latest.json")
    args = parser.parse_args(argv)
    configure_logging()
    engine = warehouse_engine(load_config())
    try:
        score(engine, artifact_root(), args.model_version)
    except Exception:
        return 1
    finally:
        engine.dispose()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
