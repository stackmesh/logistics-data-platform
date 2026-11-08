"""Statistics behind docs/feature_engineering_report.md."""

from __future__ import annotations

import math

import numpy as np
import polars as pl

from ml.features.spec import (
    CATEGORICAL_FEATURES,
    FEATURE_COLUMNS,
    FORBIDDEN_COLUMNS,
    FORBIDDEN_NAME_PARTS,
    MISSING,
    NUMERIC_FEATURES,
    TARGET,
    UNKNOWN,
)

PSI_BINS = 10
PSI_EPS = 1e-4
QUANTILES = (0.05, 0.25, 0.5, 0.75, 0.95)


def _round(value, digits=4):
    if value is None or (isinstance(value, float) and math.isnan(value)):
        return None
    return round(float(value), digits)


def numeric_summary(series: pl.Series) -> dict:
    values = series.drop_nulls()
    summary = {"count": values.len(), "nulls": series.null_count(),
               "null_pct": _round(100 * series.null_count() / max(series.len(), 1), 3)}
    if values.len():
        summary |= {"mean": _round(values.mean()), "std": _round(values.std()), "min": _round(values.min()),
                    "max": _round(values.max()), "distinct": values.n_unique()}
        summary |= {f"p{int(q * 100):02d}": _round(values.quantile(q, "linear")) for q in QUANTILES}
    return summary


def psi(expected: np.ndarray, actual: np.ndarray) -> float:
    expected = np.clip(expected, PSI_EPS, None)
    actual = np.clip(actual, PSI_EPS, None)
    return float(np.sum((actual - expected) * np.log(actual / expected)))


def numeric_psi(train: pl.Series, test: pl.Series) -> float | None:
    train_values = train.drop_nulls().to_numpy()
    test_values = test.drop_nulls().to_numpy()
    if not len(train_values) or not len(test_values):
        return None
    edges = np.unique(np.quantile(train_values, np.linspace(0, 1, PSI_BINS + 1)))
    if len(edges) < 2:
        return 0.0
    edges[0], edges[-1] = -np.inf, np.inf
    expected = np.histogram(train_values, edges)[0] / len(train_values)
    actual = np.histogram(test_values, edges)[0] / len(test_values)
    return _round(psi(expected, actual))


def shares(series: pl.Series) -> dict[str, float]:
    counts = series.value_counts().sort(["count", series.name], descending=[True, False])
    total = series.len()
    return {str(row[0]): row[1] / total for row in counts.iter_rows()}


def categorical_psi(train: pl.Series, test: pl.Series) -> float:
    train_shares, test_shares = shares(train), shares(test)
    levels = sorted(set(train_shares) | set(test_shares))
    return _round(psi(np.array([train_shares.get(k, 0.0) for k in levels]),
                      np.array([test_shares.get(k, 0.0) for k in levels])))


def categorical_summary(series: pl.Series, vocabulary: list[str]) -> dict:
    top = shares(series)
    return {
        "distinct": series.n_unique(),
        "missing": int((series == MISSING).sum()),
        "unknown": int((series == UNKNOWN).sum()),
        "missing_pct": _round(100 * (series == MISSING).sum() / max(series.len(), 1), 3),
        "levels_absent": sorted(set(vocabulary) - set(series.unique().to_list())),
        "top": {k: _round(100 * v, 2) for k, v in list(top.items())[:8]},
    }


def leakage_audit(dataset) -> list[dict]:
    X, y, ids, split = dataset.X, dataset.y, dataset.ids, dataset.split
    checks = []

    def check(name: str, passed: bool, detail) -> None:
        checks.append({"check": name, "passed": bool(passed), "detail": detail})

    columns = set(X["train"].columns) | set(X["test"].columns)
    check("Feature columns equal the allow-list, in order",
          all(tuple(frame.columns) == FEATURE_COLUMNS for frame in X.values()), f"{len(FEATURE_COLUMNS)} columns")
    check("No banned column in the matrix", not columns & FORBIDDEN_COLUMNS, sorted(columns & FORBIDDEN_COLUMNS))
    check("No banned name fragment in the matrix",
          not [c for c in columns for p in FORBIDDEN_NAME_PARTS if p in c], list(FORBIDDEN_NAME_PARTS))
    check("Target is not in the matrix", TARGET not in columns, "y_<split>.parquet holds price_rub only")
    check("Identifiers are not in the matrix", not columns & {"price_id", "loading_date"}, "ids_<split>.parquet")
    check("X, y, and ids are row-aligned",
          all(X[s].height == y[s].len() == ids[s].height for s in X), {s: X[s].height for s in X})
    train_dates, test_dates = ids["train"]["loading_date"], ids["test"]["loading_date"]
    check("Train loading_date in [train_start, test_start)",
          train_dates.min() >= split.train_start and train_dates.max() < split.test_start,
          [str(train_dates.min()), str(train_dates.max())])
    check("Test loading_date in [test_start, as_of]",
          test_dates.min() >= split.test_start and test_dates.max() <= split.as_of,
          [str(test_dates.min()), str(test_dates.max())])
    overlap = set(ids["train"]["price_id"].to_list()) & set(ids["test"]["price_id"].to_list())
    check("No deal in both train and test", not overlap, len(overlap))
    check("Rows are in chronological loading_date, price_id order",
          all(frame.equals(frame.sort(["loading_date", "price_id"])) for frame in ids.values()), "ids order")
    vocab = dataset.pipeline.vocabulary
    learned = {name: levels for name, levels in vocab.items() if name in ("origin_city", "destination_city",
                                                                         "origin_region", "destination_region")}
    check("City and region vocabularies come from train only",
          all(set(levels) <= set(X["train"][name].unique().to_list()) for name, levels in learned.items()),
          {name: len(levels) for name, levels in learned.items()})
    correlations = {}
    for name in NUMERIC_FEATURES:
        value = _round(pl.DataFrame({"x": X["train"][name], "y": y["train"]}).drop_nulls()
                       .select(pl.corr("x", "y", method="spearman")).item())
        correlations[name] = abs(value) if value is not None else 0.0  # None: constant on train
    worst = max(correlations, key=correlations.get)
    check("No numeric feature is a near copy of the target (|Spearman| < 0.99 on train)",
          correlations[worst] < 0.99, {"max": worst, "value": correlations[worst]})
    return checks


def profile_dataset(dataset) -> dict:
    X, y = dataset.X, dataset.y
    numeric = {
        name: {"train": numeric_summary(X["train"][name]), "test": numeric_summary(X["test"][name]),
               "psi": numeric_psi(X["train"][name], X["test"][name])}
        for name in NUMERIC_FEATURES
    }
    categorical = {
        name: {"vocabulary_size": len(dataset.pipeline.vocabulary[name]),
               "train": categorical_summary(X["train"][name], dataset.pipeline.vocabulary[name]),
               "test": categorical_summary(X["test"][name], dataset.pipeline.vocabulary[name]),
               "psi": categorical_psi(X["train"][name], X["test"][name])}
        for name in CATEGORICAL_FEATURES
    }
    target = {s: numeric_summary(y[s]) for s in y}
    for s in y:
        logs = y[s].log()
        target[s] |= {"log_mean": _round(logs.mean()), "log_std": _round(logs.std()),
                      "skew": _round(y[s].skew()), "log_skew": _round(logs.skew())}
    weekly = {
        s: dataset.ids[s].group_by(pl.col("loading_date").dt.truncate("1w").alias("week")).len()
        .sort("week").with_columns(pl.col("week").cast(pl.String)).rows()
        for s in dataset.ids
    }
    return {
        "rows": {s: X[s].height for s in X},
        "target": target,
        "numeric": numeric,
        "categorical": categorical,
        "weekly_rows": weekly,
        "funnel": dataset.funnel,
        "leakage_audit": leakage_audit(dataset),
    }
