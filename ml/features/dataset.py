"""Builds the train and test feature matrices and writes them with their metadata.

python -m ml.features.dataset [--output DIR]
"""

from __future__ import annotations

import argparse
import hashlib
import json
import logging
import os
import random
import time
from dataclasses import dataclass
from pathlib import Path

import numpy as np
import polars as pl
from sqlalchemy import text
from sqlalchemy.engine import Connection

from etl.config import load_config, warehouse_engine
from etl.log import configure_logging, log_event
from ml.features.extract import BASE_COLUMNS, POPULATION_PREDICATE, read_funnel, read_population
from ml.features.spec import (
    CATEGORICAL_FEATURES,
    DEFAULT_SPLIT,
    FEATURE_COLUMNS,
    FEATURES,
    FORBIDDEN_COLUMNS,
    FORBIDDEN_NAME_PARTS,
    NUMERIC_FEATURES,
    RANDOM_SEED,
    SPEC_VERSION,
    TARGET,
    SplitWindows,
)
from ml.features.transform import FeaturePipeline, split_target

logger = logging.getLogger("ml.features")

DEFAULT_OUTPUT = Path("artifacts") / "features"
ID_COLUMNS = ("price_id", "loading_date")
SPLITS = ("train", "test")


@dataclass
class FeatureDataset:
    split: SplitWindows
    pipeline: FeaturePipeline
    X: dict[str, pl.DataFrame]
    y: dict[str, pl.Series]
    ids: dict[str, pl.DataFrame]
    funnel: dict
    core_batch_id: int | None


def seed_everything(seed: int = RANDOM_SEED) -> None:
    random.seed(seed)
    np.random.seed(seed)


def assert_no_leakage(columns) -> None:
    columns = list(columns)
    banned = sorted(set(columns) & FORBIDDEN_COLUMNS)
    suspicious = sorted(c for c in columns for part in FORBIDDEN_NAME_PARTS if part in c.lower())
    if banned or suspicious or TARGET in columns:
        raise ValueError(f"feature matrix contains banned columns: {banned or suspicious or [TARGET]}")
    if tuple(columns) != FEATURE_COLUMNS:
        raise ValueError(f"feature matrix columns differ from the allow-list: {columns}")


def split_roles(base: pl.DataFrame, split: SplitWindows) -> pl.Series:
    date = pl.col("loading_date")
    return base.select(
        pl.when((date >= split.train_start) & (date < split.test_start)).then(pl.lit("train"))
        .when((date >= split.test_start) & (date < split.test_end)).then(pl.lit("test"))
        .otherwise(None)
        .alias("split_role")
    ).to_series()


def assemble(base: pl.DataFrame, split: SplitWindows) -> tuple[FeaturePipeline, dict, dict, dict]:
    """Splits a population frame by loading_date, fits on train, and transforms both parts."""
    roles = split_roles(base, split)
    if roles.null_count():
        raise ValueError(f"{roles.null_count()} rows fall outside the train and test windows")
    if base.get_column("price_id").is_duplicated().any():
        raise ValueError("price_id is not unique in the population")
    parts = {
        name: base.filter(roles == name).sort(["loading_date", "price_id"])
        for name in SPLITS
    }
    pipeline = FeaturePipeline().fit(parts["train"])
    X = {name: pipeline.transform(part) for name, part in parts.items()}
    y = {name: split_target(part) for name, part in parts.items()}
    ids = {name: part.select(ID_COLUMNS) for name, part in parts.items()}
    for frame in X.values():
        assert_no_leakage(frame.columns)
    return pipeline, X, y, ids


def build_dataset(conn: Connection, split: SplitWindows = DEFAULT_SPLIT) -> FeatureDataset:
    seed_everything()
    base = read_population(conn, split)
    pipeline, X, y, ids = assemble(base, split)
    batch_id = conn.execute(text(
        "SELECT max(batch_id) FROM audit.etl_batch WHERE status = 'succeeded'"
    )).scalar_one()
    return FeatureDataset(split, pipeline, X, y, ids, read_funnel(conn, split), batch_id)


def frame_digest(frame: pl.DataFrame | pl.Series) -> str:
    if isinstance(frame, pl.Series):
        frame = frame.to_frame()
    return hashlib.sha256(frame.write_csv(float_precision=10).encode()).hexdigest()


def metadata(dataset: FeatureDataset) -> dict:
    return {
        "spec_version": SPEC_VERSION,
        "random_seed": RANDOM_SEED,
        "target": {"name": TARGET, "source": "core.fact_deal.price_rub", "unit": "RUB without VAT",
                   "stored_in": "y_<split>.parquet"},
        "split": dataset.split.to_dict(),
        "row_order": "loading_date, price_id",
        "population": {
            "source": "core.fact_deal JOIN core.dim_city (origin, destination)",
            "predicate": " ".join(POPULATION_PREDICATE.split()),
            "extract_columns": BASE_COLUMNS,
            "core_batch_id": dataset.core_batch_id,
            "funnel": dataset.funnel,
        },
        "features": [
            {"name": f.name, "group": f.group, "kind": f.kind, "dtype": f.dtype, "source": f.source,
             "definition": f.definition}
            for f in FEATURES
        ],
        "feature_columns": list(FEATURE_COLUMNS),
        "categorical_features": list(CATEGORICAL_FEATURES),
        "numeric_features": list(NUMERIC_FEATURES),
        "categorical_encoding": dataset.pipeline.to_dict(),
        "numeric_missing_policy": "kept as null; no imputation is fitted",
        "forbidden_columns": sorted(FORBIDDEN_COLUMNS),
        "forbidden_name_parts": list(FORBIDDEN_NAME_PARTS),
        "rows": {name: dataset.X[name].height for name in SPLITS},
        "digests": {
            name: {"X": frame_digest(dataset.X[name]), "y": frame_digest(dataset.y[name]),
                   "ids": frame_digest(dataset.ids[name])}
            for name in SPLITS
        },
    }


def write_dataset(dataset: FeatureDataset, output: Path) -> dict:
    from ml.features.profile import profile_dataset

    output.mkdir(parents=True, exist_ok=True)
    for name in SPLITS:
        dataset.X[name].write_parquet(output / f"X_{name}.parquet")
        dataset.y[name].to_frame().write_parquet(output / f"y_{name}.parquet")
        dataset.ids[name].write_parquet(output / f"ids_{name}.parquet")
    meta = metadata(dataset)
    (output / "metadata.json").write_text(json.dumps(meta, ensure_ascii=False, indent=2, default=str) + "\n")
    profile = profile_dataset(dataset)
    (output / "profile.json").write_text(json.dumps(profile, ensure_ascii=False, indent=2, default=str) + "\n")
    return meta


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path,
                        default=Path(os.environ.get("ML_FEATURE_DIR", "").strip() or DEFAULT_OUTPUT))
    args = parser.parse_args(argv)
    configure_logging()
    started = time.monotonic()
    engine = warehouse_engine(load_config())
    try:
        with engine.connect() as conn, conn.begin():
            conn.execute(text("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY"))
            dataset = build_dataset(conn)
    finally:
        engine.dispose()
    meta = write_dataset(dataset, args.output)
    log_event(logger, "feature_dataset_written", output=str(args.output), rows=meta["rows"],
              features=len(FEATURE_COLUMNS), core_batch_id=meta["population"]["core_batch_id"],
              duration_s=round(time.monotonic() - started, 3))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
