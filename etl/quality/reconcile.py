"""Source-to-core reconciliation.

Expected outcomes are recomputed in SQL on deals_bigdata, independently of the Polars
transform, then compared row by row with raw, core, and quarantine in logistics_dwh.

    python -m etl.quality.reconcile --start-date 2026-07-26 --end-date 2026-10-26
    python -m etl.quality.reconcile --full --by-month
"""

from __future__ import annotations

import argparse
import json
import sys
from datetime import date
from decimal import Decimal

import polars as pl
from sqlalchemy.engine import Connection

from etl.config import DateWindow, load_config, source_engine, warehouse_engine

SOURCE_EXPECTED_SQL = """
WITH city AS (
    SELECT DISTINCT ON (gc_id) gc_id, id
    FROM cities
    ORDER BY gc_id, id
),
rate AS (
    SELECT DISTINCT ON (num_code, setting_date) num_code, setting_date, value
    FROM exchange_rates
    ORDER BY num_code, setting_date, id DESC
)
SELECT
    p.id AS price_id,
    p.loading_date,
    ARRAY_REMOVE(ARRAY[
        CASE WHEN p.price IS NULL OR p.price <= 0 THEN 'PRICE_NON_POSITIVE' END,
        CASE WHEN p.currency_code = '643' AND p.price > 0 AND p.price <= 1000 THEN 'RUB_PRICE_LE_1000' END,
        CASE WHEN p.distance IS NULL OR p.distance <= 0 THEN 'DISTANCE_NON_POSITIVE' END,
        CASE WHEN p.total_weight IS NULL OR p.total_weight < 0 THEN 'WEIGHT_NEGATIVE' END,
        CASE WHEN p.total_volume IS NULL OR p.total_volume < 0 THEN 'VOLUME_NEGATIVE' END,
        CASE WHEN lc.gc_id IS NULL THEN 'ORIGIN_CITY_NOT_FOUND' END,
        CASE WHEN uc.gc_id IS NULL THEN 'DESTINATION_CITY_NOT_FOUND' END,
        CASE WHEN p.currency_code IS NULL OR p.currency_code NOT IN ('643', '840', '978')
             THEN 'CURRENCY_UNSUPPORTED' END,
        CASE WHEN p.currency_code IN ('840', '978') AND r.value IS NULL THEN 'FX_RATE_MISSING' END
    ], NULL) AS expected_reasons,
    CASE WHEN p.currency_code = '643' THEN 1.000000::numeric(18, 6)
         ELSE round(r.value::numeric, 6)::numeric(18, 6) END::text AS expected_fx_rate,
    CASE WHEN p.currency_code IN ('840', '978') THEN p.nearest_rate_date END AS expected_fx_rate_date,
    p.nearest_rate_date AS source_nearest_rate_date,
    round(
        round(p.price::numeric, 2)
        * CASE WHEN p.currency_code = '643' THEN 1 ELSE round(r.value::numeric, 6) END,
        2
    )::numeric(18, 2)::text AS expected_price_rub,
    round(p.distance::numeric, 3)::numeric(12, 3)::text AS expected_distance,
    p.loading_city_gcid AS source_origin_gcid,
    p.unloading_city_gcid AS source_destination_gcid,
    lc.id AS expected_origin_city_id,
    uc.id AS expected_destination_city_id,
    CASE WHEN p.weight_category IN ('G20T', 'FTL', '10T', '5T', 'L5T', 'OTH') THEN p.weight_category
         WHEN round(p.total_weight::numeric, 1) >= 30 THEN 'G20T'
         WHEN round(p.total_weight::numeric, 1) >= 10 THEN 'FTL'
         WHEN round(p.total_weight::numeric, 1) >= 5 THEN '10T'
         WHEN round(p.total_weight::numeric, 1) >= 2.5 THEN '5T'
         WHEN round(p.total_weight::numeric, 1) >= 0.5 THEN 'L5T'
         ELSE 'OTH' END AS expected_weight_category,
    CASE WHEN p.volume_category IN ('FTL', 'OTH') THEN p.volume_category
         WHEN p.total_volume > 0 AND p.total_volume <= 120 THEN 'FTL'
         ELSE 'OTH' END AS expected_volume_category,
    p.outlier_mark AS source_outlier_mark,
    p.is_fact AS source_is_fact
FROM prices AS p
LEFT JOIN city AS lc ON lc.gc_id = p.loading_city_gcid
LEFT JOIN city AS uc ON uc.gc_id = p.unloading_city_gcid
LEFT JOIN rate AS r
       ON r.num_code = p.currency_code
      AND r.setting_date = p.nearest_rate_date
      AND p.currency_code IN ('840', '978')
WHERE {predicate}
ORDER BY p.id
"""

SOURCE_SCHEMA = {
    "price_id": pl.Int64,
    "loading_date": pl.Date,
    "expected_reasons": pl.List(pl.Utf8),
    "expected_fx_rate": pl.Utf8,
    "expected_fx_rate_date": pl.Date,
    "source_nearest_rate_date": pl.Date,
    "expected_price_rub": pl.Utf8,
    "expected_distance": pl.Utf8,
    "source_origin_gcid": pl.Int64,
    "source_destination_gcid": pl.Int64,
    "expected_origin_city_id": pl.Int64,
    "expected_destination_city_id": pl.Int64,
    "expected_weight_category": pl.Utf8,
    "expected_volume_category": pl.Utf8,
    "source_outlier_mark": pl.Boolean,
    "source_is_fact": pl.Boolean,
}

FACT_SQL = """
SELECT
    f.price_id,
    f.loading_date AS fact_loading_date,
    f.fx_rate::text AS fx_rate,
    f.fx_rate_date,
    f.nearest_rate_date,
    f.price_rub::text AS price_rub,
    f.distance_km::text AS distance_km,
    f.loading_city_gcid,
    f.unloading_city_gcid,
    lc.source_city_id AS origin_city_id,
    uc.source_city_id AS destination_city_id,
    f.weight_category,
    f.volume_category,
    f.outlier_mark,
    f.is_fact
FROM core.fact_deal AS f
JOIN core.dim_city AS lc ON lc.gc_id = f.loading_city_gcid
JOIN core.dim_city AS uc ON uc.gc_id = f.unloading_city_gcid
WHERE {predicate}
ORDER BY f.price_id
"""

FACT_SCHEMA = {
    "price_id": pl.Int64,
    "fact_loading_date": pl.Date,
    "fx_rate": pl.Utf8,
    "fx_rate_date": pl.Date,
    "nearest_rate_date": pl.Date,
    "price_rub": pl.Utf8,
    "distance_km": pl.Utf8,
    "loading_city_gcid": pl.Int64,
    "unloading_city_gcid": pl.Int64,
    "origin_city_id": pl.Int64,
    "destination_city_id": pl.Int64,
    "weight_category": pl.Utf8,
    "volume_category": pl.Utf8,
    "outlier_mark": pl.Boolean,
    "is_fact": pl.Boolean,
}

QUARANTINE_SQL = """
SELECT price_id, min(loading_date) AS quarantine_loading_date,
       array_agg(reason_code ORDER BY reason_code) AS actual_reasons
FROM audit.quarantine_price AS q
WHERE {predicate}
GROUP BY price_id
ORDER BY price_id
"""

QUARANTINE_SCHEMA = {
    "price_id": pl.Int64,
    "quarantine_loading_date": pl.Date,
    "actual_reasons": pl.List(pl.Utf8),
}

RAW_SQL = "SELECT id AS price_id FROM raw.prices AS r WHERE {predicate} ORDER BY id"

MISMATCH_CHECKS = {
    "fx_rate": ("fx_rate", "expected_fx_rate"),
    "fx_rate_date": ("fx_rate_date", "expected_fx_rate_date"),
    "nearest_rate_date": ("nearest_rate_date", "source_nearest_rate_date"),
    "price_rub": ("price_rub", "expected_price_rub"),
    "distance": ("distance_km", "expected_distance"),
    "origin_gcid": ("loading_city_gcid", "source_origin_gcid"),
    "destination_gcid": ("unloading_city_gcid", "source_destination_gcid"),
    "origin_city_resolution": ("origin_city_id", "expected_origin_city_id"),
    "destination_city_resolution": ("destination_city_id", "expected_destination_city_id"),
    "weight_category": ("weight_category", "expected_weight_category"),
    "volume_category": ("volume_category", "expected_volume_category"),
    "outlier_mark": ("outlier_mark", "source_outlier_mark"),
    "is_fact": ("is_fact", "source_is_fact"),
}


def _predicate(window: DateWindow, alias: str) -> str:
    if window.is_full:
        return "TRUE"
    return f"{alias}.loading_date BETWEEN %(start_date)s AND %(end_date)s"


def _fetch(conn: Connection, sql: str, params: dict, schema: dict) -> pl.DataFrame:
    driver = conn.connection.driver_connection
    with driver.cursor() as cursor:
        cursor.execute(sql, params)
        rows = cursor.fetchall()
    return pl.DataFrame(rows, schema=schema, orient="row")


def build_outcomes(source: Connection, warehouse: Connection, window: DateWindow) -> pl.DataFrame:
    """One row per price_id seen in the source window or in the warehouse for that window."""
    params = window.sql_params()
    expected = _fetch(source, SOURCE_EXPECTED_SQL.format(predicate=_predicate(window, "p")), params, SOURCE_SCHEMA)
    fact = _fetch(warehouse, FACT_SQL.format(predicate=_predicate(window, "f")), params, FACT_SCHEMA)
    quarantine = _fetch(warehouse, QUARANTINE_SQL.format(predicate=_predicate(window, "q")), params,
                        QUARANTINE_SCHEMA)
    raw = _fetch(warehouse, RAW_SQL.format(predicate=_predicate(window, "r")), params, {"price_id": pl.Int64})

    frame = (
        expected.with_columns(pl.lit(True).alias("in_source"))
        .join(fact.with_columns(pl.lit(True).alias("in_fact")), on="price_id", how="full", coalesce=True)
        .join(quarantine.with_columns(pl.lit(True).alias("in_quarantine")), on="price_id", how="full",
              coalesce=True)
        .join(raw.with_columns(pl.lit(True).alias("in_raw")), on="price_id", how="full", coalesce=True)
        .with_columns(
            pl.col("in_source", "in_fact", "in_quarantine", "in_raw").fill_null(False),
            pl.coalesce("loading_date", "fact_loading_date", "quarantine_loading_date").alias("loading_date"),
            pl.col("expected_reasons").list.sort(),
            pl.col("actual_reasons").fill_null(pl.lit([], dtype=pl.List(pl.Utf8))),
        )
    )

    accepted = pl.col("in_fact")
    mismatch_columns = [
        (accepted & pl.col(actual).ne_missing(pl.col(expected_column))).alias(f"bad_{name}")
        for name, (actual, expected_column) in MISMATCH_CHECKS.items()
    ]
    frame = frame.with_columns(
        *mismatch_columns,
        (pl.col("in_source") & (pl.col("expected_reasons").list.len() == 0)).alias("expect_accept"),
        (pl.col("in_source") & (pl.col("expected_reasons").list.len() > 0)).alias("expect_reject"),
    )
    frame = frame.with_columns(
        (pl.col("in_source") & pl.col("in_fact") & pl.col("in_quarantine")).alias("bad_in_both"),
        (pl.col("in_source") & ~pl.col("in_fact") & ~pl.col("in_quarantine")).alias("bad_missing"),
        (~pl.col("in_source")).alias("bad_not_in_source"),
        (pl.col("in_source") & ~pl.col("in_raw")).alias("bad_not_in_raw"),
        (pl.col("expect_accept") & ~pl.col("in_fact")).alias("bad_should_be_accepted"),
        (pl.col("expect_reject") & ~pl.col("in_quarantine")).alias("bad_should_be_quarantined"),
        (pl.col("in_quarantine") & pl.col("in_source")
         & (pl.col("actual_reasons") != pl.col("expected_reasons"))).alias("bad_reason_codes"),
    )
    bad = [column for column in frame.columns if column.startswith("bad_")]
    outcome = (
        pl.when(pl.any_horizontal(bad)).then(pl.lit("unexplained"))
        .when(pl.col("in_fact")).then(pl.lit("accepted"))
        .otherwise(pl.lit("quarantined"))
    )
    return frame.with_columns(outcome.alias("outcome")).sort("price_id")


def summarize(frame: pl.DataFrame) -> dict:
    bad = sorted(column for column in frame.columns if column.startswith("bad_"))
    mismatches = {column.removeprefix("bad_"): int(frame[column].sum()) for column in bad}
    examples = {
        column.removeprefix("bad_"): frame.filter(pl.col(column))["price_id"].head(5).to_list()
        for column in bad
        if mismatches[column.removeprefix("bad_")]
    }
    quarantined = frame.filter(pl.col("in_quarantine"))
    reasons = [reason for codes in quarantined["actual_reasons"].to_list() for reason in codes]
    reason_counts = {code: reasons.count(code) for code in sorted(set(reasons))}
    accepted = frame.filter(pl.col("outcome") == "accepted")
    return {
        "source_count": int(frame["in_source"].sum()),
        "raw_count": int(frame["in_raw"].sum()),
        "fact_count": int(frame["in_fact"].sum()),
        "quarantined_count": int(frame["in_quarantine"].sum()),
        "accepted_explained": accepted.height,
        "quarantined_explained": int((frame["outcome"] == "quarantined").sum()),
        "unexplained": int((frame["outcome"] == "unexplained").sum()),
        "duplicate_price_ids": frame.height - frame["price_id"].n_unique(),
        "sum_price_rub": str(sum((Decimal(value) for value in accepted["price_rub"].drop_nulls()), Decimal(0))),
        "reason_counts": reason_counts,
        "mismatches": mismatches,
        "mismatch_examples": examples,
    }


def by_month(frame: pl.DataFrame) -> list[dict]:
    monthly = (
        frame.with_columns(pl.col("loading_date").dt.truncate("1mo").alias("month"))
        .group_by("month")
        .agg(
            pl.col("in_source").sum().alias("source"),
            pl.col("in_raw").sum().alias("raw"),
            (pl.col("outcome") == "accepted").sum().alias("accepted"),
            (pl.col("outcome") == "quarantined").sum().alias("quarantined"),
            (pl.col("outcome") == "unexplained").sum().alias("unexplained"),
        )
        .sort("month")
    )
    return [{**row, "month": row["month"].isoformat()[:7]} for row in monthly.to_dicts()]


def reconcile_window(window: DateWindow, monthly: bool = False) -> dict:
    config = load_config()
    source = source_engine(config)
    warehouse = warehouse_engine(config)
    try:
        with source.connect() as source_conn, warehouse.connect() as warehouse_conn:
            source_conn.exec_driver_sql("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY")
            warehouse_conn.exec_driver_sql("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY")
            frame = build_outcomes(source_conn, warehouse_conn, window)
    finally:
        source.dispose()
        warehouse.dispose()
    report = {"window": window.describe(), **summarize(frame)}
    if monthly:
        report["by_month"] = by_month(frame)
    return report


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="python -m etl.quality.reconcile")
    parser.add_argument("--start-date", type=date.fromisoformat)
    parser.add_argument("--end-date", type=date.fromisoformat)
    parser.add_argument("--full", action="store_true")
    parser.add_argument("--by-month", action="store_true")
    args = parser.parse_args(argv)
    if args.full == bool(args.start_date or args.end_date):
        parser.error("give --full, or --start-date with --end-date")
    window = DateWindow() if args.full else DateWindow(args.start_date, args.end_date)
    report = reconcile_window(window, monthly=args.by_month)
    print(json.dumps(report, indent=2, default=str))
    return 0 if report["unexplained"] == 0 and report["duplicate_price_ids"] == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
