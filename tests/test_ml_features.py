"""Feature pipeline rules. No database."""

from __future__ import annotations

import math
import re
from datetime import date, timedelta
from pathlib import Path

import polars as pl
import pytest

from ml.features import extract
from ml.features.dataset import assemble, assert_no_leakage, frame_digest
from ml.features.spec import (
    CATEGORICAL_FEATURES,
    DEFAULT_SPLIT,
    FEATURE_COLUMNS,
    FIXED_DOMAINS,
    FORBIDDEN_COLUMNS,
    FORBIDDEN_NAME_PARTS,
    MISSING,
    NUMERIC_FEATURES,
    TARGET,
    UNKNOWN,
    SplitWindows,
)
from ml.features.transform import FeaturePipeline, parse_full_name, raw_features

FEATURE_PACKAGE = Path(__file__).resolve().parents[1] / "ml" / "features"

# Every input the brief bans, plus the precomputed group statistics by their source names.
BRIEF_BANNED = (
    "price", "price_rub", "price_with_vat", "customer_cost", "transportation_cost", "profit",
    "carrier_cost", "price_group_id", "avg_price", "min_km_price", "max_km_price", "outlier_mark",
)
DERIVED_BANNED = ("price_per_km", "price_per_ton", "price_km", "km_price")

CITIES = {
    1001: ("Москва, г. Москва", 55.7558, 37.6173),
    1003: ("Казань, Республика Татарстан", 55.7961, 49.1064),
    1004: ("Екатеринбург, Свердловская обл.", 56.8389, 60.6057),
    1099: ("Тестоград, Тестовая обл.", 50.0, 50.0),
}


def base_row(price_id: int, loading_date: date, origin: int = 1001, destination: int = 1003, **overrides) -> dict:
    o, d = CITIES[origin], CITIES[destination]
    row = {
        "price_id": price_id, "loading_date": loading_date,
        "origin_full_name": o[0], "destination_full_name": d[0],
        "origin_latitude": o[1], "origin_longitude": o[2],
        "destination_latitude": d[1], "destination_longitude": d[2],
        "distance": 820.0, "total_weight": 20.0, "total_volume": 82.0,
        "body_group": "TENT", "weight_category": "FTL", "volume_category": "FTL",
        TARGET: 90_000.0 + price_id,
    }
    row.update(overrides)
    return row


def frame(rows: list[dict]) -> pl.DataFrame:
    return pl.DataFrame(rows, schema_overrides={"loading_date": pl.Date}, infer_schema_length=None)


def sample_base() -> pl.DataFrame:
    rows = []
    start = DEFAULT_SPLIT.train_start
    for i in range(40):
        rows.append(base_row(i + 1, start + timedelta(days=i), origin=(1001, 1003, 1004)[i % 3],
                             destination=(1003, 1004, 1001)[i % 3]))
    for i in range(10):
        rows.append(base_row(100 + i, DEFAULT_SPLIT.test_start + timedelta(days=i * 2)))
    return frame(rows)


# --- Geography ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("full_name", "city", "region"),
    [
        ("Казань, Республика Татарстан", "Казань", "Республика Татарстан"),
        ("Москва, г. Москва", "Москва", "г. Москва"),
        ("  Самара ,  Самарская обл.  ", "Самара", "Самарская обл."),
        ("Город, Область, Район", "Город", "Область, Район"),
        ("Безрегионск", "Безрегионск", None),
        ("Город,", "Город", None),
        ("", None, None),
        (None, None, None),
    ],
)
def test_parse_full_name(full_name, city, region):
    assert parse_full_name(full_name) == (city, region)
    base = frame([base_row(1, date(2026, 8, 1), origin_full_name=full_name)])
    raw = raw_features(base)
    assert (raw["origin_city"][0], raw["origin_region"][0]) == (city, region)


def test_origin_and_destination_are_separate_features():
    assert {"origin_city", "destination_city", "origin_region", "destination_region"} <= set(CATEGORICAL_FEATURES)
    assert not [c for c in FEATURE_COLUMNS if "route" in c or "pair" in c]
    raw = raw_features(frame([base_row(1, date(2026, 8, 1), origin=1003, destination=1001)]))
    assert raw.select("origin_city", "destination_city", "origin_region", "destination_region").row(0) == (
        "Казань", "Москва", "Республика Татарстан", "г. Москва")
    assert raw.select("origin_latitude", "destination_longitude").row(0) == (55.7961, 37.6173)


# --- Temporal ----------------------------------------------------------------------------


def temporal(day: date) -> dict:
    return raw_features(frame([base_row(1, day)])).row(0, named=True)


def test_calendar_parts_use_iso_weekday_and_ordinal_day():
    sunday, monday = temporal(date(2026, 10, 25)), temporal(date(2026, 10, 26))
    assert (sunday["day_of_week"], monday["day_of_week"]) == (7, 1)
    assert temporal(date(2024, 12, 31))["day_of_year"] == 366
    assert temporal(date(2026, 12, 31))["day_of_year"] == 365
    row = temporal(date(2026, 3, 15))
    assert (row["year"], row["month"], row["day_of_year"]) == (2026, 3, 74)


def test_cyclic_encodings_follow_the_formula():
    day = date(2026, 1, 1)
    while day.year == 2026:
        row = temporal(day)
        iso = day.isoweekday()
        doy = day.timetuple().tm_yday
        assert row["month_sin"] == pytest.approx(math.sin(2 * math.pi * day.month / 12), abs=1e-12)
        assert row["month_cos"] == pytest.approx(math.cos(2 * math.pi * day.month / 12), abs=1e-12)
        assert row["weekday_sin"] == pytest.approx(math.sin(2 * math.pi * iso / 7), abs=1e-12)
        assert row["weekday_cos"] == pytest.approx(math.cos(2 * math.pi * iso / 7), abs=1e-12)
        assert row["day_of_year_sin"] == pytest.approx(math.sin(2 * math.pi * doy / 365.25), abs=1e-12)
        assert row["day_of_year_cos"] == pytest.approx(math.cos(2 * math.pi * doy / 365.25), abs=1e-12)
        day += timedelta(days=17)


def _gap(a: dict, b: dict, prefix: str) -> float:
    return math.hypot(a[f"{prefix}_sin"] - b[f"{prefix}_sin"], a[f"{prefix}_cos"] - b[f"{prefix}_cos"])


def test_cyclic_encodings_keep_period_ends_adjacent():
    dec31, jan1 = temporal(date(2025, 12, 31)), temporal(date(2026, 1, 1))
    assert _gap(dec31, jan1, "day_of_year") < 2 * math.sin(math.pi * 1.5 / 365.25)
    assert _gap(dec31, jan1, "month") == pytest.approx(2 * math.sin(math.pi / 12))
    sunday, monday = temporal(date(2026, 10, 25)), temporal(date(2026, 10, 26))
    assert _gap(sunday, monday, "weekday") == pytest.approx(2 * math.sin(math.pi / 7))
    assert _gap(sunday, monday, "weekday") < _gap(temporal(date(2026, 10, 22)), sunday, "weekday")


# --- Leakage -----------------------------------------------------------------------------


def test_allow_list_has_no_banned_column():
    assert set(BRIEF_BANNED) <= FORBIDDEN_COLUMNS
    assert set(DERIVED_BANNED) <= FORBIDDEN_COLUMNS
    assert not set(FEATURE_COLUMNS) & FORBIDDEN_COLUMNS
    assert not [c for c in FEATURE_COLUMNS for part in FORBIDDEN_NAME_PARTS if part in c]
    assert TARGET not in FEATURE_COLUMNS
    assert set(FEATURE_COLUMNS) == set(CATEGORICAL_FEATURES) | set(NUMERIC_FEATURES)
    assert len(FEATURE_COLUMNS) == 24


@pytest.mark.parametrize("column", [*BRIEF_BANNED, *DERIVED_BANNED, "fx_rate", "price_src", "id_price_vat"])
def test_assert_no_leakage_rejects_banned_columns(column):
    with pytest.raises(ValueError):
        assert_no_leakage([*FEATURE_COLUMNS, column])


def test_assert_no_leakage_rejects_a_changed_allow_list():
    assert_no_leakage(FEATURE_COLUMNS)
    with pytest.raises(ValueError):
        assert_no_leakage(FEATURE_COLUMNS[:-1])
    with pytest.raises(ValueError):
        assert_no_leakage([*FEATURE_COLUMNS, "organization_id"])


def test_banned_inputs_never_reach_the_matrix():
    base = sample_base()
    polluted = base.with_columns(
        pl.col(TARGET).alias("price"),
        (pl.col(TARGET) * 1.2).alias("price_with_vat"),
        (pl.col(TARGET) / pl.col("distance")).alias("price_per_km"),
        (pl.col(TARGET) / pl.col("total_weight")).alias("price_per_ton"),
        (pl.col(TARGET) * 1.3).alias("customer_cost"),
        (pl.col(TARGET) * 1.3).alias("transportation_cost"),
        (pl.col(TARGET) * 0.3).alias("profit"),
        pl.col(TARGET).alias("carrier_cost"),
        pl.lit(7).alias("price_group_id"),
        pl.col(TARGET).alias("avg_price"),
        pl.lit(80.0).alias("min_km_price"),
        pl.lit(120.0).alias("max_km_price"),
        pl.lit(False).alias("outlier_mark"),
    )
    pipeline = FeaturePipeline().fit(base)
    clean, dirty = pipeline.transform(base), pipeline.transform(polluted)
    assert tuple(dirty.columns) == FEATURE_COLUMNS
    assert dirty.equals(clean)


def test_features_do_not_depend_on_the_target():
    base = sample_base()
    scrambled = base.with_columns(pl.col(TARGET).reverse() * 3)
    pipeline = FeaturePipeline().fit(base)
    assert pipeline.transform(scrambled).equals(pipeline.transform(base))
    assert FeaturePipeline().fit(scrambled).vocabulary == pipeline.vocabulary


def test_features_are_row_wise_with_no_cross_row_aggregates():
    base = sample_base()
    pipeline = FeaturePipeline().fit(base)
    whole = pipeline.transform(base)
    one_by_one = pl.concat([pipeline.transform(base.slice(i, 1)) for i in range(base.height)])
    assert whole.equals(one_by_one)
    assert pipeline.transform(base.reverse()).equals(whole.reverse())


def test_extract_reads_no_banned_source_column():
    select_list = extract.POPULATION_SQL.split("FROM core.fact_deal")[0]
    for column in FORBIDDEN_COLUMNS - {TARGET}:
        assert not re.search(rf"\b{column}\b", select_list), column
    assert select_list.count("price_rub") == 2  # f.price_rub::float8 AS price_rub, the target only
    for name in ("price_group_stats", "raw.", "price_src", "fx_rate", "organization_id", "subscriber_id"):
        assert name not in extract.POPULATION_SQL
    assert "outlier_mark" in extract.POPULATION_PREDICATE
    assert set(extract.BASE_COLUMNS) & FORBIDDEN_COLUMNS == set()


def test_feature_code_does_not_read_the_clock_or_shuffle():
    source = "\n".join(path.read_text() for path in FEATURE_PACKAGE.glob("*.py"))
    for pattern in (r"date\.today", r"datetime\.now", r"datetime\.utcnow", r"now\(\)", r"CURRENT_DATE",
                    r"shuffle", r"\.sample\(", r"permutation"):
        assert not re.search(pattern, source), pattern


# --- Categoricals ------------------------------------------------------------------------


def test_vocabulary_is_fitted_on_train_only():
    train = frame([base_row(1, date(2026, 8, 1)), base_row(2, date(2026, 8, 2), origin=1004)])
    test = frame([
        base_row(3, date(2026, 10, 6), origin=1099),
        base_row(4, date(2026, 10, 7), origin_full_name=None, body_group=None),
        base_row(5, date(2026, 10, 8), origin_full_name="  ", weight_category="XXL"),
    ])
    pipeline = FeaturePipeline().fit(train)
    assert pipeline.vocabulary["origin_city"] == ["Екатеринбург", "Москва"]
    assert "Тестоград" not in pipeline.vocabulary["origin_city"]
    X = pipeline.transform(test)
    assert X["origin_city"].to_list() == [UNKNOWN, MISSING, MISSING]
    assert X["origin_region"].to_list() == [UNKNOWN, MISSING, MISSING]
    assert X["body_group"].to_list() == ["TENT", MISSING, "TENT"]
    assert X["weight_category"].to_list() == ["FTL", "FTL", UNKNOWN]
    for name in CATEGORICAL_FEATURES:
        assert set(X[name].to_list()) <= set(pipeline.levels(name))


def test_fixed_domains_do_not_depend_on_train():
    pipeline = FeaturePipeline().fit(frame([base_row(1, date(2026, 8, 1))]))
    for name, levels in FIXED_DOMAINS.items():
        assert pipeline.vocabulary[name] == list(levels)
    X = pipeline.transform(frame([base_row(2, date(2026, 10, 6), body_group="REF", weight_category="L5T")]))
    assert X.select("body_group", "weight_category").row(0) == ("REF", "L5T")


def test_pipeline_round_trips_and_refuses_use_before_fit():
    with pytest.raises(RuntimeError):
        FeaturePipeline().transform(sample_base())
    base = sample_base()
    pipeline = FeaturePipeline().fit(base)
    restored = FeaturePipeline.from_dict(pipeline.to_dict())
    assert restored.transform(base).equals(pipeline.transform(base))
    with pytest.raises(ValueError):
        FeaturePipeline.from_dict({**pipeline.to_dict(), "unknown_token": "?"})


def test_missing_input_column_is_an_error():
    with pytest.raises(ValueError, match="distance"):
        raw_features(sample_base().drop("distance"))


# --- Temporal split ----------------------------------------------------------------------


@pytest.mark.parametrize(
    ("day", "role"),
    [(date(2026, 7, 25), None), (date(2026, 7, 26), "train"), (date(2026, 10, 4), "train"),
     (date(2026, 10, 5), "test"), (date(2026, 10, 26), "test"), (date(2026, 10, 27), None)],
)
def test_split_boundaries(day, role):
    assert DEFAULT_SPLIT.role(day) == role


def test_split_windows_are_fixed_literals():
    assert DEFAULT_SPLIT.to_dict() == {
        "as_of": "2026-10-26", "train": ["2026-07-26", "2026-10-05"], "test": ["2026-10-05", "2026-10-27"],
        "interval": "half-open [start, end) on loading_date",
    }
    with pytest.raises(ValueError):
        SplitWindows(train_start=date(2026, 10, 5), test_start=date(2026, 10, 5))


def test_assemble_splits_fits_on_train_and_keeps_chronological_order():
    base = sample_base().with_columns(
        pl.when(pl.col("price_id") >= 100).then(pl.lit(CITIES[1099][0])).otherwise(pl.col("origin_full_name"))
        .alias("origin_full_name")
    )
    shuffled = base.sample(fraction=1.0, shuffle=True, seed=1)
    pipeline, X, y, ids = assemble(shuffled, DEFAULT_SPLIT)
    assert ids["train"]["loading_date"].max() < DEFAULT_SPLIT.test_start <= ids["test"]["loading_date"].min()
    for part in ("train", "test"):
        assert ids[part].equals(ids[part].sort(["loading_date", "price_id"]))
        assert tuple(X[part].columns) == FEATURE_COLUMNS
        assert y[part].name == TARGET and y[part].len() == X[part].height
    assert "Тестоград" not in pipeline.vocabulary["origin_city"]
    assert set(X["test"]["origin_city"].to_list()) == {UNKNOWN}
    expected = base.sort(["loading_date", "price_id"])
    assert y["train"].to_list() + y["test"].to_list() == expected[TARGET].to_list()
    again = assemble(base, DEFAULT_SPLIT)
    assert frame_digest(again[1]["train"]) == frame_digest(X["train"])
    assert frame_digest(again[2]["test"]) == frame_digest(y["test"])


def test_assemble_rejects_rows_outside_the_windows_and_duplicates():
    with pytest.raises(ValueError, match="outside"):
        assemble(pl.concat([sample_base(), frame([base_row(999, date(2026, 10, 27))])]), DEFAULT_SPLIT)
    with pytest.raises(ValueError, match="unique"):
        assemble(pl.concat([sample_base(), frame([base_row(1, date(2026, 8, 1))])]), DEFAULT_SPLIT)
