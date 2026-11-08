"""Score held-out test deals and planned future deals with a saved model.

python -m ml.scoring.score [--model-version VERSION] [--role test|future]
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
SPLIT_ROLES = ("test", "future")
RECORD_TYPES = {"test": "historical", "future": "future"}

STATS_SQL = """
SELECT record_type, deals, mae_rub, rmse_rub, mape_pct, r2, bias_rub, median_ape_pct, p90_ape_pct,
       within_10_pct, within_20_pct, mean_predicted_rub, first_loading_date, last_loading_date
FROM ml.v_prediction_error_summary
WHERE model_name = :model_name AND model_version = :model_version AND record_type = ANY (:record_types)
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


def _role_base(conn: Connection, role: str, split: SplitWindows) -> pl.DataFrame:
    if role == "test":
        window = read_population(conn, split)
        return window.filter(split_roles(window, split) == "test")
    if role == "future":
        return read_future_population(conn, split)
    raise ValueError(f"unknown split role {role!r}")


def score_frame(conn: Connection, model: CatBoostRegressor, card: dict, pipeline: FeaturePipeline,
                roles: tuple[str, ...] = SPLIT_ROLES) -> pl.DataFrame:
    """Predictions for the test window and/or planned deals. Reads no target for future rows."""
    split = split_from_card(card)
    parts = []
    for role in roles:
        base = _role_base(conn, role, split)
        if base.height == 0:
            continue
        predictions = predict(model, features_for(base, pipeline))
        parts.append(pl.DataFrame({"price_id": base["price_id"], "split_role": [role] * base.height,
                                   "predicted_price_rub": predictions}))
    return pl.concat(parts) if parts else pl.DataFrame(schema={"price_id": pl.Int64, "split_role": pl.String,
                                                               "predicted_price_rub": pl.Float64})


def write_predictions(conn: Connection, frame: pl.DataFrame, card: dict, batch_id: int,
                      roles: tuple[str, ...] = SPLIT_ROLES) -> dict:
    """Stage with COPY and upsert in the caller's transaction. Reconciles only the given roles."""
    if frame.get_column("price_id").is_duplicated().any():
        raise ValueError("duplicate price_id in scoring frame")
    if not set(frame["split_role"].unique().to_list()) <= set(roles):
        raise ValueError(f"scoring frame holds roles outside {roles}")
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
        text("SELECT * FROM ml.upsert_price_predictions(:name, :version, :as_of, :batch_id, :roles)"),
        {"name": card["model_name"], "version": card["model_version"], "as_of": card["as_of"], "batch_id": batch_id,
         "roles": list(roles)},
    ).one()
    return {"rows_inserted": int(inserted), "rows_updated": int(updated),
            "rows_unchanged": frame.height - int(inserted) - int(updated), "rows_deleted": int(deleted)}


def score(engine: Engine, root: Path, model_version: str | None = None,
          roles: tuple[str, ...] = SPLIT_ROLES) -> dict:
    roles = tuple(roles)
    if not roles or not set(roles) <= set(SPLIT_ROLES) or len(set(roles)) != len(roles):
        raise ValueError(f"roles must be a non-empty subset of {SPLIT_ROLES}, got {roles}")
    model, card, pipeline = load_model(root, model_version)
    started = time.perf_counter()
    with engine.begin() as conn:
        batch_id = conn.execute(text(
            "INSERT INTO audit.ml_scoring_batch (model_name, model_version, as_of_date, status, split_roles) "
            "VALUES (:name, :version, :as_of, 'running', :roles) RETURNING scoring_batch_id"
        ), {"name": card["model_name"], "version": card["model_version"], "as_of": card["as_of"],
            "roles": list(roles)}).scalar_one()
    log_event(logger, "scoring_started", scoring_batch_id=batch_id, model_version=card["model_version"],
              split_roles=list(roles))
    try:
        with engine.begin() as conn:
            conn.execute(text("SELECT pg_advisory_xact_lock(hashtext(:key))"), {"key": LOCK_KEY})
            frame = score_frame(conn, model, card, pipeline, roles)
            counts = write_predictions(conn, frame, card, batch_id, roles)
            stats = {row.record_type: dict(row._mapping) for row in conn.execute(
                text(STATS_SQL), {"model_name": card["model_name"], "model_version": card["model_version"],
                                  "record_types": [RECORD_TYPES[role] for role in roles]})}
            rows = {role: n for role, n in frame.group_by("split_role").len().iter_rows()}
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
              "split_roles": list(roles), "rows_test": rows.get("test", 0), "rows_future": rows.get("future", 0), **counts,
              "duration_s": round(time.perf_counter() - started, 3), "summary": stats}
    log_event(logger, "scoring_completed", **result)
    return result


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="python -m ml.scoring.score")
    parser.add_argument("--model-version", help="default: ml/artifacts/latest.json")
    parser.add_argument("--role", choices=SPLIT_ROLES, action="append",
                        help="score only this split role; repeatable (default: test and future)")
    args = parser.parse_args(argv)
    configure_logging()
    engine = warehouse_engine(load_config())
    try:
        score(engine, artifact_root(), args.model_version, tuple(args.role or SPLIT_ROLES))
    except Exception:
        return 1
    finally:
        engine.dispose()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
