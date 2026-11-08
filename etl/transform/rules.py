"""Business rules shared by validation, normalization, and tests."""

from __future__ import annotations

import polars as pl

RUB = "643"
USD = "840"
EUR = "978"
SUPPORTED_CURRENCIES = (RUB, USD, EUR)
FOREIGN_CURRENCIES = (USD, EUR)
RUB_PRICE_THRESHOLD = 1000

WEIGHT_CATEGORIES = ("G20T", "FTL", "10T", "5T", "L5T", "OTH")
VOLUME_CATEGORIES = ("FTL", "OTH")
BODY_GROUPS = ("TENT", "REF", "ISO", "BORT", "OTHER", "UNSPECIFIED")


class Reason:
    PRICE_NON_POSITIVE = "PRICE_NON_POSITIVE"
    RUB_PRICE_LE_1000 = "RUB_PRICE_LE_1000"
    DISTANCE_NON_POSITIVE = "DISTANCE_NON_POSITIVE"
    WEIGHT_NEGATIVE = "WEIGHT_NEGATIVE"
    VOLUME_NEGATIVE = "VOLUME_NEGATIVE"
    ORIGIN_CITY_NOT_FOUND = "ORIGIN_CITY_NOT_FOUND"
    DESTINATION_CITY_NOT_FOUND = "DESTINATION_CITY_NOT_FOUND"
    CURRENCY_UNSUPPORTED = "CURRENCY_UNSUPPORTED"
    FX_RATE_MISSING = "FX_RATE_MISSING"


REASON_CODES = (
    Reason.PRICE_NON_POSITIVE,
    Reason.RUB_PRICE_LE_1000,
    Reason.DISTANCE_NON_POSITIVE,
    Reason.WEIGHT_NEGATIVE,
    Reason.VOLUME_NEGATIVE,
    Reason.ORIGIN_CITY_NOT_FOUND,
    Reason.DESTINATION_CITY_NOT_FOUND,
    Reason.CURRENCY_UNSUPPORTED,
    Reason.FX_RATE_MISSING,
)


def half_up(expr: pl.Expr, scale: int) -> pl.Expr:
    """Round a non-negative float to an integer count of 1/scale units, halves away from zero.

    The intermediate round(6) removes binary noise such as 29.95 * 10 = 299.49999999999997,
    so the result matches PostgreSQL ROUND(numeric) on the same decimal value.
    """
    return ((expr * scale).round(6) + 0.5).floor().cast(pl.Int64)


def weight_category(weight: pl.Expr) -> pl.Expr:
    tenths = half_up(weight, 10)
    return (
        pl.when(tenths >= 300).then(pl.lit("G20T"))
        .when(tenths >= 100).then(pl.lit("FTL"))
        .when(tenths >= 50).then(pl.lit("10T"))
        .when(tenths >= 25).then(pl.lit("5T"))
        .when(tenths >= 5).then(pl.lit("L5T"))
        .otherwise(pl.lit("OTH"))
    )


def volume_category(volume: pl.Expr) -> pl.Expr:
    return pl.when((volume > 0) & (volume <= 120)).then(pl.lit("FTL")).otherwise(pl.lit("OTH"))


def body_group(body_type_id: pl.Expr) -> pl.Expr:
    """Same mapping as core.body_group_for_type."""
    return (
        pl.when(body_type_id.is_null()).then(pl.lit("UNSPECIFIED"))
        .when(body_type_id.is_between(56, 61)).then(pl.lit("TENT"))
        .when(body_type_id.is_in([4, 46, 47, 48])).then(pl.lit("REF"))
        .when(body_type_id == 2).then(pl.lit("ISO"))
        .when(body_type_id.is_in([1, 15])).then(pl.lit("BORT"))
        .otherwise(pl.lit("OTHER"))
    )
