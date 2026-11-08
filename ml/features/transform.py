"""Row-wise feature construction and a categorical vocabulary fitted on train only."""

from __future__ import annotations

import math
from dataclasses import dataclass, field

import polars as pl

from ml.features.spec import (
    CATEGORICAL_FEATURES,
    DAY_OF_YEAR_PERIOD,
    FEATURE_COLUMNS,
    FEATURES,
    FIXED_DOMAINS,
    MISSING,
    MONTH_PERIOD,
    TARGET,
    UNKNOWN,
    WEEKDAY_PERIOD,
)

REQUIRED_INPUT = (
    "loading_date", "origin_full_name", "destination_full_name",
    "origin_latitude", "origin_longitude", "destination_latitude", "destination_longitude",
    "distance", "total_weight", "total_volume", "body_group", "weight_category", "volume_category",
)

_DTYPES = {"String": pl.String, "Float64": pl.Float64, "Int32": pl.Int32}


def parse_full_name(full_name: str | None) -> tuple[str | None, str | None]:
    """'Казань, Республика Татарстан' -> ('Казань', 'Республика Татарстан')."""
    if full_name is None:
        return None, None
    city, _, region = full_name.partition(",")
    return (city.strip() or None), (region.strip() or None)


def _blank_to_null(expr: pl.Expr) -> pl.Expr:
    stripped = expr.str.strip_chars()
    return pl.when(stripped.str.len_chars() > 0).then(stripped).otherwise(None)


def _city(column: str) -> pl.Expr:
    return _blank_to_null(pl.col(column).cast(pl.String).str.splitn(",", 2).struct.field("field_0"))


def _region(column: str) -> pl.Expr:
    return _blank_to_null(pl.col(column).cast(pl.String).str.splitn(",", 2).struct.field("field_1"))


def _cyclic(column: str, period: float) -> tuple[pl.Expr, pl.Expr]:
    angle = pl.col(column).cast(pl.Float64) * (2 * math.pi / period)
    return angle.sin(), angle.cos()


def raw_features(base: pl.DataFrame) -> pl.DataFrame:
    """Feature values before categorical encoding. Each output row depends only on its input row."""
    missing = [name for name in REQUIRED_INPUT if name not in base.columns]
    if missing:
        raise ValueError(f"input frame lacks columns: {missing}")
    date = pl.col("loading_date")
    frame = base.select(
        _city("origin_full_name").alias("origin_city"),
        _city("destination_full_name").alias("destination_city"),
        _region("origin_full_name").alias("origin_region"),
        _region("destination_full_name").alias("destination_region"),
        pl.col("origin_latitude"), pl.col("origin_longitude"),
        pl.col("destination_latitude"), pl.col("destination_longitude"),
        pl.col("distance"), pl.col("total_weight"), pl.col("total_volume"),
        _blank_to_null(pl.col("body_group").cast(pl.String)).alias("body_group"),
        _blank_to_null(pl.col("weight_category").cast(pl.String)).alias("weight_category"),
        _blank_to_null(pl.col("volume_category").cast(pl.String)).alias("volume_category"),
        date.dt.year().alias("year"),
        date.dt.month().alias("month"),
        date.dt.weekday().alias("day_of_week"),  # ISO: Monday = 1 .. Sunday = 7
        date.dt.ordinal_day().alias("day_of_year"),
    )
    month_sin, month_cos = _cyclic("month", MONTH_PERIOD)
    weekday_sin, weekday_cos = _cyclic("day_of_week", WEEKDAY_PERIOD)
    doy_sin, doy_cos = _cyclic("day_of_year", DAY_OF_YEAR_PERIOD)
    frame = frame.with_columns(
        month_sin.alias("month_sin"), month_cos.alias("month_cos"),
        weekday_sin.alias("weekday_sin"), weekday_cos.alias("weekday_cos"),
        doy_sin.alias("day_of_year_sin"), doy_cos.alias("day_of_year_cos"),
    )
    return frame.select(
        pl.col(f.name).cast(_DTYPES[f.dtype]) for f in FEATURES
    )


def split_target(base: pl.DataFrame) -> pl.Series:
    return base.get_column(TARGET).cast(pl.Float64)


@dataclass
class FeaturePipeline:
    """Maps categoricals onto a frozen vocabulary. Numeric values pass through unchanged.

    City and region levels come from the train frame given to fit. Body group, weight
    category, and volume category use their fixed core domains. A null or blank value
    becomes MISSING; a value outside the vocabulary becomes UNKNOWN.
    """

    vocabulary: dict[str, list[str]] = field(default_factory=dict)

    @property
    def is_fitted(self) -> bool:
        return bool(self.vocabulary)

    def fit(self, train_base: pl.DataFrame) -> FeaturePipeline:
        raw = raw_features(train_base)
        vocabulary = {}
        for name in CATEGORICAL_FEATURES:
            if name in FIXED_DOMAINS:
                vocabulary[name] = list(FIXED_DOMAINS[name])
            else:
                vocabulary[name] = sorted(raw.get_column(name).drop_nulls().unique().to_list())
        self.vocabulary = vocabulary
        return self

    def transform(self, base: pl.DataFrame) -> pl.DataFrame:
        if not self.is_fitted:
            raise RuntimeError("FeaturePipeline.transform called before fit")
        raw = raw_features(base)
        encoded = raw.with_columns(
            pl.when(pl.col(name).is_null()).then(pl.lit(MISSING))
            .when(pl.col(name).is_in(self.vocabulary[name])).then(pl.col(name))
            .otherwise(pl.lit(UNKNOWN))
            .alias(name)
            for name in CATEGORICAL_FEATURES
        )
        return encoded.select(FEATURE_COLUMNS)

    def levels(self, name: str) -> list[str]:
        """Every value transform can emit for a categorical, in a fixed order."""
        return [*self.vocabulary[name], MISSING, UNKNOWN]

    def to_dict(self) -> dict:
        return {"vocabulary": self.vocabulary, "missing_token": MISSING, "unknown_token": UNKNOWN}

    @classmethod
    def from_dict(cls, payload: dict) -> FeaturePipeline:
        if payload.get("missing_token") != MISSING or payload.get("unknown_token") != UNKNOWN:
            raise ValueError("feature pipeline tokens differ from this code version")
        return cls(vocabulary={name: list(levels) for name, levels in payload["vocabulary"].items()})
