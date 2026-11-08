"""mart.mart_market_corridor: reference recomputation, fixture semantics, and refresh behaviour."""

from __future__ import annotations

import random
import statistics
from datetime import date, timedelta

import numpy as np
import polars as pl
import pytest
from sqlalchemy import text

from etl.config import DateWindow
from etl.load.warehouse import load_snapshot
from etl.mart.refresh import refresh_market_corridor
from etl.transform.normalize import transform_snapshot
from tests.etl_factories import AS_OF, default_rates, make_snapshot

pytestmark = pytest.mark.integration

SNAPSHOT_AS_OF = date(2026, 10, 26)
KEY = ["price_month", "loading_city_gcid", "unloading_city_gcid", "body_group_code", "weight_category"]


# --- Full-mart reference -------------------------------------------------------------------


@pytest.fixture(scope="module")
def refreshed(loaded_warehouse, dwh_engine):
    return refresh_market_corridor(dwh_engine, SNAPSHOT_AS_OF)


def _frame(conn, sql: str) -> pl.DataFrame:
    result = conn.execute(text(sql))
    return pl.DataFrame([tuple(row) for row in result], schema=list(result.keys()), orient="row",
                        infer_schema_length=None)


def _reference_cells(conn, as_of: date) -> pl.DataFrame:
    """Recompute every cell in Polars from core, without the mart SQL."""
    deals = _frame(conn, """
        SELECT price_id, loading_city_gcid, unloading_city_gcid, weight_category,
               price_rub::float8 AS price_rub, distance_km::float8 AS distance_km,
               organization_id, price_type, is_fact, complex, outlier_mark, currency_code::text AS currency_code
        FROM core.fact_deal
    """)
    dates = _frame(conn, "SELECT price_id, price_date FROM core.fact_deal_date")
    bridge = _frame(conn, "SELECT price_id, body_group_code FROM core.bridge_deal_body_group")
    eligible = deals.filter(
        pl.col("is_fact") & pl.col("price_type").is_in([1, 2, 3, 4, 5, 9]) & ~pl.col("complex")
        & ~pl.col("outlier_mark") & (pl.col("price_rub") > 0) & (pl.col("distance_km") > 0)
        & pl.col("currency_code").is_in(["643", "840", "978"])
    )
    observations = (
        dates.filter(pl.col("price_date") <= as_of)
        .with_columns(pl.col("price_date").dt.truncate("1mo").alias("price_month"))
        .join(bridge, on="price_id")
        .group_by(["price_id", "price_month", "body_group_code"])
        .agg(pl.col("price_date").max())
        .join(eligible, on="price_id")
        .with_columns((pl.col("price_rub") / pl.col("distance_km")).alias("price_km"))
    )
    cells = observations.group_by(KEY).agg(
        pl.col("price_rub").quantile(0.25, "linear").alias("p25_trip"),
        pl.col("price_rub").quantile(0.50, "linear").alias("median_trip"),
        pl.col("price_rub").quantile(0.75, "linear").alias("p75_trip"),
        pl.col("price_rub").mean().alias("avg_trip"),
        pl.col("price_km").quantile(0.25, "linear").alias("p25_km"),
        pl.col("price_km").quantile(0.50, "linear").alias("median_km"),
        pl.col("price_km").quantile(0.75, "linear").alias("p75_km"),
        pl.col("price_id").n_unique().alias("trips_count"),
        pl.col("organization_id").filter(pl.col("price_type").is_in([3, 9])).drop_nulls().n_unique()
        .alias("carriers_count"),
        pl.col("price_rub").std(ddof=1).alias("std"),
        pl.len().alias("n"),
        pl.col("price_date").max().alias("max_price_date"),
    )
    medians = {
        tuple(row[:5]): row[5]
        for row in cells.select([*KEY, "median_trip"]).iter_rows()
    }

    def shifted(month: date, months: int) -> date:
        index = month.year * 12 + month.month - 1 - months
        return date(index // 12, index % 12 + 1, 1)

    def change(row: dict, months: int):
        base = medians.get((shifted(row["price_month"], months), *[row[k] for k in KEY[1:]]))
        if base is None or base == 0:
            return None
        return (row["median_trip"] / base - 1) * 100

    rows = []
    for row in cells.iter_rows(named=True):
        freshness = (as_of - row["max_price_date"]).days
        if row["trips_count"] >= 30 and row["carriers_count"] >= 5 and freshness <= 30:
            level = "HIGH"
        elif row["trips_count"] >= 10:
            level = "MEDIUM"
        else:
            level = "LOW_DATA"
        rows.append({
            **row,
            "cv_rate": None if row["n"] < 2 or row["avg_trip"] == 0 else row["std"] / row["avg_trip"],
            "mom_pct": change(row, 1),
            "yoy_pct": change(row, 12),
            "freshness_days": freshness,
            "reliability_level": level,
        })
    return pl.DataFrame(rows, infer_schema_length=None)


def _mart(conn) -> pl.DataFrame:
    return _frame(conn, """
        SELECT price_month, loading_city_gcid, unloading_city_gcid, body_group_code, weight_category,
               p25_trip::float8, median_trip::float8, p75_trip::float8, avg_trip::float8,
               p25_km::float8, median_km::float8, p75_km::float8, trips_count, carriers_count,
               cv_rate::float8, mom_pct::float8, yoy_pct::float8, max_price_date, freshness_days,
               reliability_level, as_of_date
        FROM mart.mart_market_corridor
    """)


def test_every_cell_matches_independent_reference(refreshed, dwh_tx):
    reference = _reference_cells(dwh_tx, SNAPSHOT_AS_OF)
    mart = _mart(dwh_tx)
    assert mart.height == reference.height == refreshed["cells"]
    joined = mart.join(reference, on=KEY, how="full", suffix="_ref", coalesce=True)
    assert joined.height == mart.height, "cell keys differ between mart and reference"

    for column in ("trips_count", "carriers_count", "max_price_date", "freshness_days", "reliability_level"):
        bad = joined.filter(pl.col(column).ne_missing(pl.col(f"{column}_ref")))
        assert bad.height == 0, (column, bad.head(3).to_dicts())

    tolerances = {"p25_trip": 0.011, "median_trip": 0.011, "p75_trip": 0.011, "avg_trip": 0.011,
                  "p25_km": 0.00011, "median_km": 0.00011, "p75_km": 0.00011, "cv_rate": 0.0000011,
                  "mom_pct": 0.00011, "yoy_pct": 0.00011}
    for column, tolerance in tolerances.items():
        null_mismatch = joined.filter(pl.col(column).is_null() != pl.col(f"{column}_ref").is_null())
        assert null_mismatch.height == 0, (column, null_mismatch.head(3).to_dicts())
        worst = joined.select((pl.col(column) - pl.col(f"{column}_ref")).abs().max()).item()
        assert worst is None or worst <= tolerance, (column, worst)


def test_snapshot_shape(refreshed, dwh_tx):
    stats = dwh_tx.execute(text("""
        SELECT max(price_month) AS last_month, max(max_price_date) AS last_date,
               count(*) - count(DISTINCT (price_month, loading_city_gcid, unloading_city_gcid,
                                          body_group_code, weight_category)) AS duplicate_cells,
               bool_and(as_of_date = DATE '2026-10-26') AS as_of_ok
        FROM mart.mart_market_corridor
    """)).one()
    assert stats.last_month == date(2026, 10, 1)
    assert stats.last_date <= SNAPSHOT_AS_OF
    assert stats.duplicate_cells == 0 and stats.as_of_ok
    definition = dwh_tx.execute(text(
        "SELECT pg_get_functiondef('mart.build_market_corridor(date)'::regprocedure)"
    )).scalar_one()
    for forbidden in ("price_group_stats", "raw.", "deals_bigdata"):
        assert forbidden not in definition


# --- Repeated and failed refresh ------------------------------------------------------------

MART_HASH = """
SELECT count(*) AS cells,
       md5(string_agg(md5(ROW(price_month, loading_city_gcid, unloading_city_gcid, body_group_code,
           weight_category, origin_city_name, destination_city_name, body_group_name, p25_trip,
           median_trip, p75_trip, avg_trip, p25_km, median_km, p75_km, trips_count, carriers_count,
           cv_rate, mom_pct, yoy_pct, max_price_date, freshness_days, reliability_level,
           as_of_date)::text), '' ORDER BY price_month, loading_city_gcid, unloading_city_gcid,
           body_group_code, weight_category)) AS content_hash,
       sum(trips_count) AS observations
FROM mart.mart_market_corridor
"""


def test_repeated_refresh_gives_identical_mart(refreshed, dwh_engine):
    with dwh_engine.connect() as conn:
        before = dict(conn.execute(text(MART_HASH)).mappings().one())
    second = refresh_market_corridor(dwh_engine, SNAPSHOT_AS_OF)
    third = refresh_market_corridor(dwh_engine, SNAPSHOT_AS_OF)
    with dwh_engine.connect() as conn:
        after = dict(conn.execute(text(MART_HASH)).mappings().one())
        refresh_ids = conn.execute(text("SELECT DISTINCT refresh_id FROM mart.mart_market_corridor")).scalars().all()
        statuses = conn.execute(text(
            "SELECT status, rows_deleted, rows_inserted FROM audit.mart_refresh WHERE refresh_id IN (:a, :b)"
        ), {"a": second["refresh_id"], "b": third["refresh_id"]}).all()
    assert after == before
    assert second["rows_inserted"] == third["rows_inserted"] == third["rows_deleted"] == before["cells"]
    assert refresh_ids == [third["refresh_id"]]
    assert all(row.status == "succeeded" for row in statuses)


def test_failed_refresh_keeps_previous_mart(refreshed, dwh_engine):
    with dwh_engine.connect() as conn:
        before = dict(conn.execute(text(MART_HASH)).mappings().one())
    connection = dwh_engine.connect()
    transaction = connection.begin()
    with pytest.raises(Exception):
        connection.execute(text("CALL mart.refresh_market_corridor(DATE '2026-10-26', -1, NULL, NULL)"))
    transaction.rollback()
    connection.close()
    with dwh_engine.connect() as conn:
        assert dict(conn.execute(text(MART_HASH)).mappings().one()) == before


# --- Fixture semantics -----------------------------------------------------------------------

BASE = 960_000_000
FIXTURE_AS_OF = date(2099, 6, 15)
WINDOW = DateWindow(date(2098, 1, 1), date(2099, 12, 31))
CITY_COUNT = 24
CITIES = [(960000 + i, 960000 + i, f"Фикстура-{i}", f"Фикстура-{i}, Тестовая область") for i in range(1, CITY_COUNT + 1)]
CITIES.append((960099, 960001, "Дубль", "Дубль, Тестовая область"))


def city(n: int) -> int:
    return 960000 + n


class Builder:
    def __init__(self):
        self.prices: list[dict] = []
        self.dates: list[tuple] = []
        self.links: list[tuple] = []

    def deal(self, origin: int, destination: int, price: float, price_dates: list[date], *,
             body_types=(56,), weight: str = "FTL", price_type: int = 1, organization: int = 1,
             distance: float = 1000.0, **extra) -> int:
        deal_id = BASE + len(self.prices) + 1
        self.prices.append({
            "id": deal_id, "item_id": deal_id, "loading_city_gcid": city(origin),
            "unloading_city_gcid": city(destination), "price": price, "distance": distance,
            "loading_date": price_dates[0], "end_loading_date": price_dates[-1],
            "nearest_rate_date": None, "body_type": body_types[0] if body_types else None,
            "weight_category": weight, "price_type": price_type, "organization_id": organization,
            "is_fact": price_type != 6, **extra,
        })
        for offset, price_date in enumerate(price_dates):
            self.dates.append((deal_id * 10 + offset, deal_id, price_date))
        for offset, body_type in enumerate(body_types):
            self.links.append((deal_id * 10 + offset, deal_id, body_type))
        return deal_id


REFERENCE_SAMPLE = [round(random.Random(20991).uniform(20_000, 300_000), 2) for _ in range(7)]
MAY = [date(2099, 5, 10)]


def _build() -> Builder:
    b = Builder()
    # Hand-computed percentiles on 1 -> 2. Origin gc 960001 also has a duplicate source row.
    for price in (100_000, 120_000, 150_000, 210_000):
        b.deal(1, 2, price, MAY)
    # numpy reference on 1 -> 3
    for price in REFERENCE_SAMPLE:
        b.deal(1, 3, price, MAY, distance=850.0)
    # Child body types on 4 -> 5: 56+57+61 is one TENT deal; 56+15 is TENT and BORT.
    b.deal(4, 5, 50_000, MAY, body_types=(56, 57, 61))
    b.deal(4, 5, 70_000, MAY, body_types=(56, 15))
    # MoM / YoY series on 6 -> 7, FTL: 2098-02, 2098-03, 2099-01, 2099-02, 2099-04 (no 2099-03).
    for month, price in ((date(2098, 2, 10), 88_000), (date(2098, 3, 10), 95_000), (date(2099, 1, 10), 100_000),
                         (date(2099, 2, 10), 110_000), (date(2099, 4, 10), 121_000)):
        b.deal(6, 7, price, [month])
    b.deal(6, 7, 999_000, [date(2099, 3, 10)], weight="10T")
    # Reliability boundaries, each on its own destination, month 2099-05.
    fresh = FIXTURE_AS_OF - timedelta(days=30)
    stale = FIXTURE_AS_OF - timedelta(days=31)

    def population(destination, trips, carriers, last_date):
        for index in range(trips):
            carrier = index < carriers
            b.deal(8, destination, 60_000 + index * 100, [date(2099, 5, 1) if index else last_date],
                   price_type=3 if carrier else 1, organization=100 + index if carrier else 500)

    population(9, 30, 5, fresh)      # HIGH
    population(10, 30, 5, stale)     # MEDIUM: 31 days old
    population(11, 29, 5, fresh)     # MEDIUM: 29 trips
    population(12, 30, 4, fresh)     # MEDIUM: 4 carriers
    population(13, 10, 0, fresh)     # MEDIUM: 10 trips
    population(14, 9, 0, fresh)      # LOW_DATA: 9 trips
    # Carrier counting on 15 -> 16: type 3 org 700 twice, type 9 org 701, type 1 org 702.
    b.deal(15, 16, 40_000, MAY, price_type=3, organization=700)
    b.deal(15, 16, 41_000, MAY, price_type=3, organization=700)
    b.deal(15, 16, 42_000, MAY, price_type=9, organization=701)
    b.deal(15, 16, 43_000, MAY, price_type=1, organization=702)
    # Future dates on 17 -> 18: as-of day counts, later days and months do not.
    b.deal(17, 18, 30_000, [FIXTURE_AS_OF])
    b.deal(17, 18, 31_000, [date(2099, 6, 10), date(2099, 7, 5)])
    b.deal(17, 18, 32_000, [FIXTURE_AS_OF + timedelta(days=1)])
    b.deal(17, 18, 33_000, [date(2099, 8, 1)])
    # Ineligible rows on 19 -> 20 next to one eligible deal.
    b.deal(19, 20, 25_000, MAY)
    b.deal(19, 20, 26_000, MAY, outlier_mark=True)
    b.deal(19, 20, 27_000, MAY, complex=True)
    b.deal(19, 20, 28_000, MAY, price_type=6)
    # Two dates of one deal in the same month count once on 21 -> 22.
    b.deal(21, 22, 35_000, [date(2099, 5, 3), date(2099, 5, 20)])
    return b


@pytest.fixture(scope="module")
def fixture_cells(loaded_warehouse, dwh_engine):
    builder = _build()
    snapshot = make_snapshot(
        builder.prices, cities=CITIES, rates=default_rates([date(2099, 1, 5)], start_id=BASE),
        price_body_types=builder.links, price_dates=builder.dates, window=WINDOW,
    )
    result = transform_snapshot(snapshot, AS_OF)
    assert result.reasons.height == 0, result.reasons.to_dicts()
    connection = dwh_engine.connect()
    transaction = connection.begin()
    try:
        batch_id = connection.execute(text(
            "INSERT INTO audit.etl_batch (source_system, mode, status, start_date, end_date) "
            "VALUES ('test_mart', 'window', 'running', '2098-01-01', '2099-12-31') RETURNING batch_id"
        )).scalar_one()
        load_snapshot(connection, batch_id=batch_id, window=WINDOW, snapshot=snapshot, result=result,
                      source_system="test_mart")
        rows = connection.execute(text(
            "SELECT * FROM mart.build_market_corridor(:as_of) WHERE loading_city_gcid BETWEEN 960001 AND 960099"
        ), {"as_of": FIXTURE_AS_OF}).mappings().all()
        city_rows = connection.execute(text(
            "SELECT gc_id, source_city_id, name, duplicate_count FROM core.dim_city WHERE gc_id = 960001"
        )).all()
    finally:
        transaction.rollback()
        connection.close()
    return {"cells": [dict(row) for row in rows], "city": city_rows}


def cells(fixture, origin, destination, **filters):
    found = [
        row for row in fixture["cells"]
        if row["loading_city_gcid"] == city(origin) and row["unloading_city_gcid"] == city(destination)
        and all(row[key] == value for key, value in filters.items())
    ]
    return found


def one(fixture, origin, destination, **filters):
    found = cells(fixture, origin, destination, **filters)
    assert len(found) == 1, found
    return found[0]


def test_percentiles_match_hand_computed_values(fixture_cells):
    row = one(fixture_cells, 1, 2)
    assert (row["p25_trip"], row["median_trip"], row["p75_trip"]) == (115_000, 135_000, 165_000)
    assert float(row["avg_trip"]) == 145_000
    assert (row["p25_km"], row["median_km"], row["p75_km"]) == pytest.approx((115, 135, 165))
    expected_cv = statistics.stdev([100_000, 120_000, 150_000, 210_000]) / 145_000
    assert float(row["cv_rate"]) == pytest.approx(expected_cv, rel=1e-12)


def test_percentiles_match_numpy_reference(fixture_cells):
    row = one(fixture_cells, 1, 3)
    sample = np.array(REFERENCE_SAMPLE)
    for column, q in (("p25_trip", 25), ("median_trip", 50), ("p75_trip", 75)):
        assert row[column] == pytest.approx(np.percentile(sample, q, method="linear"), abs=1e-6)
    for column, q in (("p25_km", 25), ("median_km", 50), ("p75_km", 75)):
        assert row[column] == pytest.approx(np.percentile(sample / 850.0, q, method="linear"), abs=1e-9)
    assert row["trips_count"] == 7


def test_duplicate_gc_id_does_not_multiply_deals(fixture_cells):
    assert [tuple(r) for r in fixture_cells["city"]] == [(960001, 960001, "Фикстура-1", 2)]
    row = one(fixture_cells, 1, 2)
    assert row["trips_count"] == 4
    assert row["origin_city_name"] == "Фикстура-1"


def test_child_body_types_are_deduplicated_per_group(fixture_cells):
    tent = one(fixture_cells, 4, 5, body_group_code="TENT")
    bort = one(fixture_cells, 4, 5, body_group_code="BORT")
    assert tent["trips_count"] == 2 and tent["body_group_name"] == "Тент"
    assert bort["trips_count"] == 1 and bort["body_group_name"] == "Борт"
    assert bort["median_trip"] == 70_000
    assert len(cells(fixture_cells, 4, 5)) == 2


def test_mom_and_yoy_use_exact_calendar_months(fixture_cells):
    def at(month):
        return one(fixture_cells, 6, 7, weight_category="FTL", price_month=month)

    feb = at(date(2099, 2, 1))
    assert feb["mom_pct"] == pytest.approx(10.0)
    assert feb["yoy_pct"] == pytest.approx(25.0)
    jan = at(date(2099, 1, 1))
    assert jan["mom_pct"] is None and jan["yoy_pct"] is None
    apr = at(date(2099, 4, 1))
    assert apr["mom_pct"] is None, "March is absent; February must not be substituted"
    assert apr["yoy_pct"] is None, "2098-04 is absent; 2098-03 must not be substituted"
    assert at(date(2098, 3, 1))["mom_pct"] == pytest.approx((95_000 / 88_000 - 1) * 100)
    assert one(fixture_cells, 6, 7, weight_category="10T")["mom_pct"] is None


@pytest.mark.parametrize(
    ("destination", "level", "trips", "carriers", "freshness"),
    [(9, "HIGH", 30, 5, 30), (10, "MEDIUM", 30, 5, 31), (11, "MEDIUM", 29, 5, 30),
     (12, "MEDIUM", 30, 4, 30), (13, "MEDIUM", 10, 0, 30), (14, "LOW_DATA", 9, 0, 30)],
)
def test_reliability_boundaries(fixture_cells, destination, level, trips, carriers, freshness):
    row = one(fixture_cells, 8, destination)
    assert (row["trips_count"], row["carriers_count"], row["freshness_days"]) == (trips, carriers, freshness)
    assert row["reliability_level"] == level


def test_carriers_count_only_types_3_and_9_and_distinct(fixture_cells):
    row = one(fixture_cells, 15, 16)
    assert row["trips_count"] == 4 and row["carriers_count"] == 2


def test_future_dates_are_excluded(fixture_cells):
    rows = cells(fixture_cells, 17, 18)
    assert [row["price_month"] for row in rows] == [date(2099, 6, 1)]
    june = rows[0]
    assert june["trips_count"] == 2
    assert june["max_price_date"] == FIXTURE_AS_OF and june["freshness_days"] == 0
    assert all(row["price_month"] <= date(2099, 6, 1) for row in fixture_cells["cells"])


def test_ineligible_rows_are_filtered_and_single_cv_is_null(fixture_cells):
    row = one(fixture_cells, 19, 20)
    assert row["trips_count"] == 1 and row["median_trip"] == 25_000
    assert row["cv_rate"] is None and row["reliability_level"] == "LOW_DATA"


def test_two_dates_in_one_month_count_once(fixture_cells):
    row = one(fixture_cells, 21, 22)
    assert row["trips_count"] == 1 and row["max_price_date"] == date(2099, 5, 20)
