"""Data-quality rules. Every failed rule produces one reason row."""

from __future__ import annotations

import polars as pl

from etl.transform.rules import (
    FOREIGN_CURRENCIES,
    RUB,
    RUB_PRICE_THRESHOLD,
    SUPPORTED_CURRENCIES,
    Reason,
)

REASON_SCHEMA = {"price_id": pl.Int64, "reason_code": pl.Utf8, "reason_detail": pl.Utf8}


def _value(name: str) -> pl.Expr:
    column = pl.col(name)
    return pl.when(column.is_null()).then(pl.lit(f"{name} is NULL")).otherwise(
        pl.format(f"{name}={{}}", column)
    )


def find_reasons(priced: pl.DataFrame, city_ids: pl.Series) -> pl.DataFrame:
    """priced is prices with rate_micro attached for exact USD and EUR matches."""

    price = pl.col("price")
    currency = pl.col("currency_code")
    distance = pl.col("distance")
    weight = pl.col("total_weight")
    volume = pl.col("total_volume")
    origin = pl.col("loading_city_gcid")
    destination = pl.col("unloading_city_gcid")
    nearest = pl.col("nearest_rate_date")
    known_cities = city_ids.drop_nulls().to_list()

    rules = [
        (
            Reason.PRICE_NON_POSITIVE,
            price.is_null() | (price <= 0),
            _value("price"),
        ),
        (
            Reason.RUB_PRICE_LE_1000,
            (currency == RUB) & (price > 0) & (price <= RUB_PRICE_THRESHOLD),
            pl.format("RUB price={} is at or below {}", price, pl.lit(RUB_PRICE_THRESHOLD)),
        ),
        (
            Reason.DISTANCE_NON_POSITIVE,
            distance.is_null() | (distance <= 0),
            _value("distance"),
        ),
        (
            Reason.WEIGHT_NEGATIVE,
            weight.is_null() | (weight < 0),
            _value("total_weight"),
        ),
        (
            Reason.VOLUME_NEGATIVE,
            volume.is_null() | (volume < 0),
            _value("total_volume"),
        ),
        (
            Reason.ORIGIN_CITY_NOT_FOUND,
            origin.is_null() | ~origin.is_in(known_cities),
            pl.format("loading_city_gcid={} is not in source cities", origin),
        ),
        (
            Reason.DESTINATION_CITY_NOT_FOUND,
            destination.is_null() | ~destination.is_in(known_cities),
            pl.format("unloading_city_gcid={} is not in source cities", destination),
        ),
        (
            Reason.CURRENCY_UNSUPPORTED,
            currency.is_null() | ~currency.is_in(list(SUPPORTED_CURRENCIES)),
            _value("currency_code"),
        ),
        (
            Reason.FX_RATE_MISSING,
            currency.is_in(list(FOREIGN_CURRENCIES)) & pl.col("rate_micro").is_null(),
            pl.when(nearest.is_null())
            .then(pl.format("nearest_rate_date is NULL for currency {}", currency))
            .otherwise(pl.format("no exchange rate for currency {} on {}", currency, nearest)),
        ),
    ]

    frames = [
        priced.filter(condition.fill_null(False)).select(
            pl.col("id").alias("price_id"),
            pl.lit(code).alias("reason_code"),
            detail.fill_null("value is NULL").alias("reason_detail"),
        )
        for code, condition, detail in rules
    ]
    return pl.concat(frames, how="vertical").cast(REASON_SCHEMA).sort(["price_id", "reason_code"])
