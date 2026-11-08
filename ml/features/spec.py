"""Feature definitions, the target, the ban list, and the fixed temporal split."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, timedelta

SPEC_VERSION = "1"
RANDOM_SEED = 20261026
TARGET = "price_rub"

MISSING = "__MISSING__"
UNKNOWN = "__UNKNOWN__"

MONTH_PERIOD = 12.0
WEEKDAY_PERIOD = 7.0
DAY_OF_YEAR_PERIOD = 365.25


@dataclass(frozen=True)
class FeatureDef:
    name: str
    group: str
    kind: str  # "categorical" or "numeric"
    dtype: str
    source: str
    definition: str


FEATURES: tuple[FeatureDef, ...] = (
    FeatureDef("origin_city", "geography", "categorical", "String", "core.dim_city.full_name_local (loading gc_id)",
               "Text before the first comma"),
    FeatureDef("destination_city", "geography", "categorical", "String",
               "core.dim_city.full_name_local (unloading gc_id)", "Text before the first comma"),
    FeatureDef("origin_region", "geography", "categorical", "String", "core.dim_city.full_name_local (loading gc_id)",
               "Text after the first comma, trimmed; missing when there is no comma"),
    FeatureDef("destination_region", "geography", "categorical", "String",
               "core.dim_city.full_name_local (unloading gc_id)",
               "Text after the first comma, trimmed; missing when there is no comma"),
    FeatureDef("origin_latitude", "geography", "numeric", "Float64", "core.dim_city.latitude", "As stored"),
    FeatureDef("origin_longitude", "geography", "numeric", "Float64", "core.dim_city.longitude", "As stored"),
    FeatureDef("destination_latitude", "geography", "numeric", "Float64", "core.dim_city.latitude", "As stored"),
    FeatureDef("destination_longitude", "geography", "numeric", "Float64", "core.dim_city.longitude", "As stored"),
    FeatureDef("distance", "transport", "numeric", "Float64", "core.fact_deal.distance_km", "Kilometres, as stored"),
    FeatureDef("total_weight", "transport", "numeric", "Float64", "core.fact_deal.total_weight_t", "Tonnes"),
    FeatureDef("total_volume", "transport", "numeric", "Float64", "core.fact_deal.total_volume_m3", "Cubic metres"),
    FeatureDef("body_group", "transport", "categorical", "String", "core.fact_deal.body_group_code",
               "Group of prices.body_type: TENT, REF, ISO, BORT, OTHER, UNSPECIFIED"),
    FeatureDef("weight_category", "transport", "categorical", "String", "core.fact_deal.weight_category",
               "G20T, FTL, 10T, 5T, L5T, OTH"),
    FeatureDef("volume_category", "transport", "categorical", "String", "core.fact_deal.volume_category", "FTL, OTH"),
    FeatureDef("year", "temporal", "numeric", "Int32", "core.fact_deal.loading_date", "Calendar year"),
    FeatureDef("month", "temporal", "numeric", "Int32", "core.fact_deal.loading_date", "1..12"),
    FeatureDef("day_of_week", "temporal", "numeric", "Int32", "core.fact_deal.loading_date",
               "ISO weekday, Monday = 1 .. Sunday = 7"),
    FeatureDef("day_of_year", "temporal", "numeric", "Int32", "core.fact_deal.loading_date", "1..366"),
    FeatureDef("month_sin", "temporal", "numeric", "Float64", "month", "sin(2*pi*month/12)"),
    FeatureDef("month_cos", "temporal", "numeric", "Float64", "month", "cos(2*pi*month/12)"),
    FeatureDef("weekday_sin", "temporal", "numeric", "Float64", "day_of_week", "sin(2*pi*day_of_week/7)"),
    FeatureDef("weekday_cos", "temporal", "numeric", "Float64", "day_of_week", "cos(2*pi*day_of_week/7)"),
    FeatureDef("day_of_year_sin", "temporal", "numeric", "Float64", "day_of_year", "sin(2*pi*day_of_year/365.25)"),
    FeatureDef("day_of_year_cos", "temporal", "numeric", "Float64", "day_of_year", "cos(2*pi*day_of_year/365.25)"),
)

FEATURE_COLUMNS: tuple[str, ...] = tuple(f.name for f in FEATURES)
CATEGORICAL_FEATURES: tuple[str, ...] = tuple(f.name for f in FEATURES if f.kind == "categorical")
NUMERIC_FEATURES: tuple[str, ...] = tuple(f.name for f in FEATURES if f.kind == "numeric")

# Levels fixed by core check constraints. City and region levels are learned from train.
FIXED_DOMAINS: dict[str, tuple[str, ...]] = {
    "body_group": ("TENT", "REF", "ISO", "BORT", "OTHER", "UNSPECIFIED"),
    "weight_category": ("G20T", "FTL", "10T", "5T", "L5T", "OTH"),
    "volume_category": ("FTL", "OTH"),
}

FORBIDDEN_COLUMNS: frozenset[str] = frozenset({
    "price", "price_src", "price_rub", "price_with_vat", "price_per_km", "price_per_ton",
    "price_km", "km_price", "customer_cost", "transportation_cost", "profit", "carrier_cost",
    "price_group_id", "avg_price", "min_km_price", "max_km_price", "outlier_mark",
    "fx_rate", "currency_code", "nearest_rate_date",
})

# Substrings that never belong in a feature name: target, VAT, unit prices, margin, group stats.
FORBIDDEN_NAME_PARTS: tuple[str, ...] = ("price", "cost", "profit", "vat", "outlier", "margin", "fx_")


@dataclass(frozen=True)
class SplitWindows:
    """Half-open loading_date windows. test_start belongs only to test."""

    as_of: date = date(2026, 10, 26)
    train_start: date = date(2026, 7, 26)
    test_start: date = date(2026, 10, 5)

    def __post_init__(self) -> None:
        if not self.train_start < self.test_start <= self.as_of:
            raise ValueError("expected train_start < test_start <= as_of")

    @property
    def test_end(self) -> date:
        """Exclusive upper bound: the day after as_of."""
        return self.as_of + timedelta(days=1)

    def role(self, loading_date: date) -> str | None:
        if self.train_start <= loading_date < self.test_start:
            return "train"
        if self.test_start <= loading_date < self.test_end:
            return "test"
        return None

    def to_dict(self) -> dict:
        return {
            "as_of": self.as_of.isoformat(),
            "train": [self.train_start.isoformat(), self.test_start.isoformat()],
            "test": [self.test_start.isoformat(), self.test_end.isoformat()],
            "interval": "half-open [start, end) on loading_date",
        }


DEFAULT_SPLIT = SplitWindows()
