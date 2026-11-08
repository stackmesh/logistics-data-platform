"""Comparison models fitted on the same train rows as CatBoost."""

from __future__ import annotations

import numpy as np
import polars as pl
from sklearn.compose import ColumnTransformer
from sklearn.linear_model import Ridge
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import OneHotEncoder, OrdinalEncoder, StandardScaler
from sklearn.tree import DecisionTreeRegressor

from ml.features.spec import CATEGORICAL_FEATURES, FEATURE_COLUMNS, NUMERIC_FEATURES


def to_matrix(X: pl.DataFrame) -> np.ndarray:
    """Object matrix in allow-list order. Categoricals stay strings, numerics stay floats."""
    if tuple(X.columns) != FEATURE_COLUMNS:
        raise ValueError("feature columns are not in allow-list order")
    return np.column_stack([
        X[name].to_numpy().astype(object) if name in CATEGORICAL_FEATURES else X[name].cast(pl.Float64).to_numpy()
        for name in FEATURE_COLUMNS
    ]).astype(object)


_CAT_IDX = [FEATURE_COLUMNS.index(name) for name in CATEGORICAL_FEATURES]
_NUM_IDX = [FEATURE_COLUMNS.index(name) for name in NUMERIC_FEATURES]


def ridge(seed: int) -> Pipeline:
    """One-hot categoricals and standardized numerics; both encoders are fitted on train only."""
    del seed  # Ridge with the default solver is deterministic
    return Pipeline([
        ("encode", ColumnTransformer([
            ("cat", OneHotEncoder(handle_unknown="ignore"), _CAT_IDX),
            ("num", StandardScaler(), _NUM_IDX),
        ])),
        ("model", Ridge(alpha=1.0)),
    ])


def decision_tree(seed: int) -> Pipeline:
    return Pipeline([
        ("encode", ColumnTransformer([
            ("cat", OrdinalEncoder(handle_unknown="use_encoded_value", unknown_value=-1), _CAT_IDX),
            ("num", "passthrough", _NUM_IDX),
        ])),
        ("model", DecisionTreeRegressor(max_depth=12, min_samples_leaf=20, random_state=seed)),
    ])


class RatePerKmBaseline:
    """Train median of price_rub / distance per body group, times the deal's distance."""

    def fit(self, X: pl.DataFrame, y: pl.Series) -> RatePerKmBaseline:
        rates = (pl.DataFrame({"body_group": X["body_group"], "rate": y / X["distance"]})
                 .group_by("body_group").agg(pl.col("rate").median()))
        self.rates = dict(rates.iter_rows())
        self.default = float((y / X["distance"]).median())
        return self

    def predict(self, X: pl.DataFrame) -> np.ndarray:
        rate = X["body_group"].replace_strict(self.rates, default=self.default, return_dtype=pl.Float64)
        return (rate * X["distance"]).to_numpy()
