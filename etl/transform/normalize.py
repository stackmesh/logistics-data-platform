"""Turn a source snapshot into conformed core rows and quarantine rows."""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from datetime import date, datetime

import numpy as np
import polars as pl

from etl.extract.source import SourceSnapshot
from etl.transform.rules import (
    FOREIGN_CURRENCIES,
    REASON_CODES,
    RUB,
    SUPPORTED_CURRENCIES,
    VOLUME_CATEGORIES,
    WEIGHT_CATEGORIES,
    body_group,
    half_up,
    volume_category,
    weight_category,
)
from etl.transform.validate import find_reasons

RATE_SCALE = 1_000_000
CENT_SCALE = 100

DEAL_COLUMNS = (
    "price_id",
    "loading_date",
    "end_loading_date",
    "loading_city_gcid",
    "unloading_city_gcid",
    "distance_km",
    "total_weight_t",
    "total_volume_m3",
    "body_type_id",
    "body_group_code",
    "weight_category",
    "volume_category",
    "weight_category_imputed",
    "volume_category_imputed",
    "price_src_cents",
    "currency_code",
    "fx_rate_micro",
    "nearest_rate_date",
    "fx_rate_date",
    "price_rub_cents",
    "price_type",
    "is_fact",
    "complex",
    "outlier_mark",
    "organization_id",
    "subscriber_id",
    "created_at",
    "updated_at",
    "source_name",
    "item_id",
    "meta_item_id",
    "external_item_uuid",
)

QUARANTINE_COLUMNS = (
    "price_id",
    "reason_code",
    "reason_detail",
    "created_at",
    "loading_date",
    "loading_city_gcid",
    "unloading_city_gcid",
    "distance",
    "total_weight",
    "total_volume",
    "price",
    "currency_code",
    "nearest_rate_date",
    "price_type",
    "is_fact",
    "source_record",
)

CITY_COLUMNS = (
    "gc_id",
    "source_city_id",
    "name",
    "full_name_local",
    "region_name",
    "latitude",
    "longitude",
    "duplicate_count",
)

BODY_TYPE_COLUMNS = (
    "body_type_id",
    "name",
    "id_from_1c",
    "source_std_id",
    "body_group_code",
    "source_system",
)

RATE_COLUMNS = ("currency_code", "rate_date", "rate_micro", "source_rate_id")

INFERRED_SOURCE_SYSTEM = "etl_inferred"


@dataclass(frozen=True)
class TransformResult:
    deals: pl.DataFrame
    reasons: pl.DataFrame
    quarantine: pl.DataFrame
    deal_dates: pl.DataFrame
    bridge: pl.DataFrame
    dim_city: pl.DataFrame
    dim_body_type: pl.DataFrame
    dim_exchange_rate: pl.DataFrame
    stats: dict = field(default_factory=dict)


def build_exchange_rates(rates: pl.DataFrame) -> pl.DataFrame:
    """One rate per currency and date. Duplicate source rows keep the greatest id."""
    return (
        rates.filter(pl.col("num_code").is_in(list(SUPPORTED_CURRENCIES)) & pl.col("value").is_not_null())
        .group_by(["num_code", "setting_date"])
        .agg(
            pl.col("id").max().alias("source_rate_id"),
            pl.col("value").sort_by("id").last().alias("rate_value"),
        )
        .rename({"num_code": "currency_code", "setting_date": "rate_date"})
        .with_columns(half_up(pl.col("rate_value"), RATE_SCALE).alias("rate_micro"))
        .select(RATE_COLUMNS)
        .sort(["currency_code", "rate_date"])
    )


def build_cities(cities: pl.DataFrame) -> pl.DataFrame:
    """One row per gc_id, taken from the smallest source cities.id."""
    region = (
        pl.col("full_name_local")
        .str.splitn(",", 2)
        .struct.field("field_1")
        .str.strip_chars()
    )
    return (
        cities.sort("id")
        .group_by("gc_id", maintain_order=True)
        .agg(
            pl.col("id").first().alias("source_city_id"),
            pl.col("name").first(),
            pl.col("full_name_local").first(),
            pl.col("latitude").first(),
            pl.col("longitude").first(),
            pl.len().cast(pl.Int64).alias("duplicate_count"),
        )
        .with_columns(
            pl.when(region == "").then(None).otherwise(region).alias("region_name"),
        )
        .select(CITY_COLUMNS)
        .sort("gc_id")
    )


def attach_fx(prices: pl.DataFrame, rates: pl.DataFrame) -> pl.DataFrame:
    """Attach the rate whose date equals nearest_rate_date, for USD and EUR only."""
    foreign = rates.filter(pl.col("currency_code").is_in(list(FOREIGN_CURRENCIES))).select(
        "currency_code", "rate_date", "rate_micro"
    )
    return prices.join(
        foreign,
        left_on=["currency_code", "nearest_rate_date"],
        right_on=["currency_code", "rate_date"],
        how="left",
    ).sort("id")


def _normalize_deals(accepted: pl.DataFrame) -> pl.DataFrame:
    weight_valid = pl.col("weight_category").is_in(list(WEIGHT_CATEGORIES)).fill_null(False)
    volume_valid = pl.col("volume_category").is_in(list(VOLUME_CATEGORIES)).fill_null(False)
    is_rub = pl.col("currency_code") == RUB

    deals = accepted.with_columns(
        half_up(pl.col("price"), CENT_SCALE).alias("price_src_cents"),
        pl.when(is_rub).then(pl.lit(RATE_SCALE, dtype=pl.Int64)).otherwise(pl.col("rate_micro")).alias("fx_rate_micro"),
        pl.when(is_rub).then(pl.lit(None, dtype=pl.Date)).otherwise(pl.col("nearest_rate_date")).alias("fx_rate_date"),
        pl.when(weight_valid).then(pl.col("weight_category")).otherwise(weight_category(pl.col("total_weight"))).alias("weight_category"),
        pl.when(volume_valid).then(pl.col("volume_category")).otherwise(volume_category(pl.col("total_volume"))).alias("volume_category"),
        (~weight_valid).alias("weight_category_imputed"),
        (~volume_valid).alias("volume_category_imputed"),
        body_group(pl.col("body_type")).alias("body_group_code"),
    )

    if deals.height:
        max_product = int(deals["price_src_cents"].max()) * int(deals["fx_rate_micro"].max())
        if max_product > np.iinfo(np.int64).max - RATE_SCALE:
            raise ValueError("price_src * fx_rate exceeds the int64 conversion range")

    return (
        deals.with_columns(
            ((pl.col("price_src_cents") * pl.col("fx_rate_micro") + RATE_SCALE // 2) // RATE_SCALE).alias("price_rub_cents"),
        )
        .rename(
            {
                "id": "price_id",
                "distance": "distance_km",
                "total_weight": "total_weight_t",
                "total_volume": "total_volume_m3",
                "body_type": "body_type_id",
                "source": "source_name",
            }
        )
        .select(DEAL_COLUMNS)
        .sort("price_id")
    )


def _build_body_types(body_types: pl.DataFrame, deals: pl.DataFrame) -> pl.DataFrame:
    source = body_types.select(
        pl.col("id").alias("body_type_id"),
        pl.col("name"),
        pl.col("id_from_1c"),
        pl.col("std_id").alias("source_std_id"),
        body_group(pl.col("id")).alias("body_group_code"),
        pl.lit("deals_bigdata").alias("source_system"),
    )
    used = deals.select("body_type_id").drop_nulls().unique()
    inferred = used.join(source.select("body_type_id"), on="body_type_id", how="anti").select(
        pl.col("body_type_id"),
        pl.format("inferred body type {}", pl.col("body_type_id")).alias("name"),
        pl.col("body_type_id").alias("id_from_1c"),
        pl.lit(None, dtype=pl.Int64).alias("source_std_id"),
        body_group(pl.col("body_type_id")).alias("body_group_code"),
        pl.lit(INFERRED_SOURCE_SYSTEM).alias("source_system"),
    )
    return pl.concat([source, inferred], how="vertical").select(BODY_TYPE_COLUMNS).sort("body_type_id")


def _build_bridge(price_body_types: pl.DataFrame, deals: pl.DataFrame) -> pl.DataFrame:
    """One row per deal and business group. Links first, then prices.body_type, then unspecified."""
    deal_ids = deals.select("price_id")
    links = (
        price_body_types.join(deal_ids, on="price_id", how="semi")
        .select(
            pl.col("price_id"),
            body_group(pl.col("body_type_id")).alias("body_group_code"),
        )
        .unique()
        .with_columns(pl.lit("links").alias("resolution"))
    )
    fallback = deals.join(links.select("price_id").unique(), on="price_id", how="anti").select(
        pl.col("price_id"),
        pl.col("body_group_code"),
        pl.when(pl.col("body_type_id").is_null())
        .then(pl.lit("unspecified"))
        .otherwise(pl.lit("fallback_body_type"))
        .alias("resolution"),
    )
    return pl.concat([links, fallback], how="vertical").sort(["price_id", "body_group_code"])


def _json_default(value):
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    return str(value)


def _build_quarantine(prices: pl.DataFrame, reasons: pl.DataFrame) -> pl.DataFrame:
    rejected_ids = reasons.select("price_id").unique()
    rejected = prices.join(rejected_ids, left_on="id", right_on="price_id", how="semi").sort("id")
    records = pl.DataFrame(
        {
            "id": rejected["id"],
            "source_record": [
                json.dumps(row, default=_json_default, ensure_ascii=False)
                for row in rejected.iter_rows(named=True)
            ],
        },
        schema={"id": pl.Int64, "source_record": pl.Utf8},
    )
    snapshot = rejected.select(
        "id",
        "created_at",
        "loading_date",
        "loading_city_gcid",
        "unloading_city_gcid",
        "distance",
        "total_weight",
        "total_volume",
        "price",
        "currency_code",
        "nearest_rate_date",
        "price_type",
        "is_fact",
    ).join(records, on="id")
    return (
        reasons.join(snapshot, left_on="price_id", right_on="id", how="inner")
        .select(QUARANTINE_COLUMNS)
        .sort(["price_id", "reason_code"])
    )


def _stats(snapshot: SourceSnapshot, deals: pl.DataFrame, reasons: pl.DataFrame, deal_dates: pl.DataFrame,
           bridge: pl.DataFrame, dim_body_type: pl.DataFrame, as_of: date) -> dict:
    counts = dict(reasons.group_by("reason_code").len().iter_rows())
    reason_counts = {code: int(counts.get(code, 0)) for code in REASON_CODES}
    snapshot_day = deals.filter(pl.col("created_at") >= datetime.combine(as_of, datetime.min.time()))
    deals_with_dates = deal_dates.select("price_id").n_unique() if deal_dates.height else 0
    return {
        "extracted_count": snapshot.prices.height,
        "accepted_count": deals.height,
        "rejected_count": reasons.select("price_id").n_unique() if reasons.height else 0,
        "reason_rows": reasons.height,
        "reason_counts": reason_counts,
        "accepted_profile": {
            "same_city_routes": deals.filter(pl.col("loading_city_gcid") == pl.col("unloading_city_gcid")).height,
            "outlier_marked": deals.filter(pl.col("outlier_mark")).height,
            "non_fact": deals.filter(~pl.col("is_fact")).height,
            "complex_routes": deals.filter(pl.col("complex")).height,
            "future_loading_date": deals.filter(pl.col("loading_date") > as_of).height,
            "foreign_currency": deals.filter(pl.col("currency_code") != RUB).height,
            "weight_category_imputed": deals.filter(pl.col("weight_category_imputed")).height,
            "volume_category_imputed": deals.filter(pl.col("volume_category_imputed")).height,
            "without_price_dates": deals.height - deals_with_dates,
            "multi_group_deals": bridge.group_by("price_id").len().filter(pl.col("len") > 1).height,
            "inferred_body_types": dim_body_type.filter(pl.col("source_system") == INFERRED_SOURCE_SYSTEM).height,
            "created_on_as_of_date": snapshot_day.height,
            "created_on_as_of_date_outlier_marked": snapshot_day.filter(pl.col("outlier_mark")).height,
        },
    }


def transform_snapshot(snapshot: SourceSnapshot, as_of: date) -> TransformResult:
    dim_exchange_rate = build_exchange_rates(snapshot.exchange_rates)
    dim_city = build_cities(snapshot.cities)
    priced = attach_fx(snapshot.prices, dim_exchange_rate)
    reasons = find_reasons(priced, snapshot.cities["gc_id"].unique())
    accepted = priced.join(reasons.select("price_id").unique(), left_on="id", right_on="price_id", how="anti")
    deals = _normalize_deals(accepted)
    dim_body_type = _build_body_types(snapshot.body_types, deals)
    bridge = _build_bridge(snapshot.price_body_types, deals)
    deal_dates = (
        snapshot.price_dates.join(deals.select("price_id"), on="price_id", how="semi")
        .select("price_id", "price_date")
        .unique()
        .sort(["price_id", "price_date"])
    )
    quarantine = _build_quarantine(snapshot.prices, reasons)
    return TransformResult(
        deals=deals,
        reasons=reasons,
        quarantine=quarantine,
        deal_dates=deal_dates,
        bridge=bridge,
        dim_city=dim_city,
        dim_body_type=dim_body_type,
        dim_exchange_rate=dim_exchange_rate,
        stats=_stats(snapshot, deals, reasons, deal_dates, bridge, dim_body_type, as_of),
    )
