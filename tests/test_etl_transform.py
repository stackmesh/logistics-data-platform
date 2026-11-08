"""Validation and normalization rules. No database needed."""

from __future__ import annotations

from datetime import date, datetime

import polars as pl
import pytest

from etl.config import DateWindow
from etl.transform.normalize import transform_snapshot
from etl.transform.rules import REASON_CODES, Reason
from tests.etl_factories import AS_OF, make_snapshot


def _run(prices, **kwargs):
    return transform_snapshot(make_snapshot(prices, **kwargs), AS_OF)


def _deal(result, price_id):
    rows = result.deals.filter(pl.col("price_id") == price_id).to_dicts()
    assert len(rows) == 1, f"price {price_id} not accepted"
    return rows[0]


def _reasons(result, price_id):
    return set(result.reasons.filter(pl.col("price_id") == price_id)["reason_code"].to_list())


# 1. Currency conversion


def test_rub_rate_is_one_and_price_is_unchanged():
    deal = _deal(_run([{"id": 1, "price": 84500.0, "currency_code": "643"}]), 1)
    assert deal["fx_rate_micro"] == 1_000_000
    assert deal["price_src_cents"] == deal["price_rub_cents"] == 8_450_000
    assert deal["fx_rate_date"] is None
    assert deal["nearest_rate_date"] == date(2026, 9, 10)


def test_foreign_price_uses_exact_nearest_rate_date_not_loading_date():
    rates = [
        (1, "840", 91.2345, date(2026, 9, 10)),
        (2, "840", 70.0, date(2026, 9, 15)),
        (3, "978", 99.5, date(2026, 9, 10)),
    ]
    result = _run(
        [
            {"id": 1, "price": 2480.0, "currency_code": "840", "loading_date": date(2026, 9, 15)},
            {"id": 2, "price": 1000.5, "currency_code": "978"},
        ],
        rates=rates,
    )
    usd = _deal(result, 1)
    assert usd["fx_rate_micro"] == 91_234_500
    assert usd["fx_rate_date"] == date(2026, 9, 10)
    assert usd["price_rub_cents"] == 22_626_156
    eur = _deal(result, 2)
    assert eur["price_rub_cents"] == 9_954_975


def test_duplicate_rate_rows_use_greatest_source_id():
    rates = [(5, "840", 80.0, date(2026, 9, 10)), (9, "840", 90.0, date(2026, 9, 10))]
    result = _run([{"id": 1, "price": 100.0, "currency_code": "840"}], rates=rates)
    assert _deal(result, 1)["price_rub_cents"] == 900_000
    assert result.dim_exchange_rate.filter(pl.col("currency_code") == "840")["source_rate_id"].to_list() == [9]


def test_conversion_rounds_half_away_from_zero():
    rates = [(1, "840", 1.5, date(2026, 9, 10))]
    result = _run([{"id": 1, "price": 0.01, "currency_code": "840"}], rates=rates)
    assert _deal(result, 1)["price_rub_cents"] == 2


# 2. Missing exchange rates


def test_null_nearest_rate_date_quarantines_foreign_price():
    result = _run([{"id": 1, "price": 2480.0, "currency_code": "840", "nearest_rate_date": None}])
    assert _reasons(result, 1) == {Reason.FX_RATE_MISSING}
    detail = result.reasons.filter(pl.col("price_id") == 1)["reason_detail"].item()
    assert detail == "nearest_rate_date is NULL for currency 840"


def test_missing_rate_on_exact_date_quarantines_even_if_loading_date_has_rate():
    rates = [(1, "840", 90.0, date(2026, 9, 15))]
    result = _run(
        [{"id": 1, "price": 2480.0, "currency_code": "978", "nearest_rate_date": date(2026, 9, 10),
          "loading_date": date(2026, 9, 15)},
         {"id": 2, "price": 2480.0, "currency_code": "840", "nearest_rate_date": date(2026, 9, 10),
          "loading_date": date(2026, 9, 15)}],
        rates=rates,
    )
    assert _reasons(result, 1) == {Reason.FX_RATE_MISSING}
    assert _reasons(result, 2) == {Reason.FX_RATE_MISSING}
    assert result.deals.height == 0


def test_rub_with_null_nearest_rate_date_is_accepted():
    deal = _deal(_run([{"id": 1, "nearest_rate_date": None}]), 1)
    assert deal["nearest_rate_date"] is None and deal["fx_rate_micro"] == 1_000_000


# 3. RUB threshold


@pytest.mark.parametrize(
    ("price", "currency", "expected"),
    [
        (1000.0, "643", {Reason.RUB_PRICE_LE_1000}),
        (999.99, "643", {Reason.RUB_PRICE_LE_1000}),
        (0.5, "643", {Reason.RUB_PRICE_LE_1000}),
        (1000.01, "643", set()),
        (0.0, "643", {Reason.PRICE_NON_POSITIVE}),
        (-10.0, "643", {Reason.PRICE_NON_POSITIVE}),
        (500.0, "840", set()),
        (0.0, "840", {Reason.PRICE_NON_POSITIVE}),
    ],
)
def test_rub_threshold(price, currency, expected):
    assert _reasons(_run([{"id": 1, "price": price, "currency_code": currency}]), 1) == expected


# 4. Distance


@pytest.mark.parametrize(("distance", "rejected"), [(0.0, True), (-5.0, True), (0.1, False), (1.0, False)])
def test_distance_must_be_positive(distance, rejected):
    result = _run([{"id": 1, "distance": distance}])
    assert (_reasons(result, 1) == {Reason.DISTANCE_NON_POSITIVE}) is rejected
    assert (result.deals.height == 1) is not rejected


@pytest.mark.parametrize(
    ("field", "value", "reason"),
    [("total_weight", -0.1, Reason.WEIGHT_NEGATIVE), ("total_volume", -1.0, Reason.VOLUME_NEGATIVE)],
)
def test_negative_weight_and_volume_rejected_but_zero_kept(field, value, reason):
    assert _reasons(_run([{"id": 1, field: value}]), 1) == {reason}
    assert _run([{"id": 1, field: 0.0}]).deals.height == 1


# 5. Category boundaries


@pytest.mark.parametrize(
    ("weight", "expected"),
    [
        (0.0, "OTH"), (0.44, "OTH"), (0.45, "L5T"), (0.5, "L5T"), (2.44, "L5T"), (2.45, "5T"),
        (4.94, "5T"), (4.95, "10T"), (9.94, "10T"), (9.95, "FTL"), (29.94, "FTL"),
        (29.95, "G20T"), (40.0, "G20T"),
    ],
)
def test_weight_category_boundaries_when_source_missing(weight, expected):
    deal = _deal(_run([{"id": 1, "total_weight": weight, "weight_category": None}]), 1)
    assert deal["weight_category"] == expected
    assert deal["weight_category_imputed"] is True


@pytest.mark.parametrize(
    ("volume", "expected"), [(0.0, "OTH"), (0.001, "FTL"), (120.0, "FTL"), (120.001, "OTH")]
)
def test_volume_category_boundaries_when_source_missing(volume, expected):
    deal = _deal(_run([{"id": 1, "total_volume": volume, "volume_category": None}]), 1)
    assert deal["volume_category"] == expected
    assert deal["volume_category_imputed"] is True


def test_valid_source_categories_are_kept_and_invalid_ones_recalculated():
    result = _run(
        [
            {"id": 1, "total_weight": 20.0, "weight_category": "10T", "volume_category": "OTH"},
            {"id": 2, "total_weight": 20.0, "weight_category": "XXXX", "volume_category": "BAD"},
        ]
    )
    kept = _deal(result, 1)
    assert (kept["weight_category"], kept["volume_category"]) == ("10T", "OTH")
    assert not kept["weight_category_imputed"] and not kept["volume_category_imputed"]
    fixed = _deal(result, 2)
    assert (fixed["weight_category"], fixed["volume_category"]) == ("FTL", "FTL")
    assert fixed["weight_category_imputed"] and fixed["volume_category_imputed"]


# 6. Same-city routes


def test_same_city_route_is_accepted():
    result = _run([{"id": 1, "loading_city_gcid": 1003, "unloading_city_gcid": 1003}])
    assert result.reasons.height == 0
    assert _deal(result, 1)["loading_city_gcid"] == _deal(result, 1)["unloading_city_gcid"] == 1003
    assert result.stats["accepted_profile"]["same_city_routes"] == 1


# 7. Missing city references


def test_missing_origin_and_destination_have_separate_codes():
    result = _run(
        [
            {"id": 1, "loading_city_gcid": 9999},
            {"id": 2, "unloading_city_gcid": 9998},
            {"id": 3, "loading_city_gcid": 9999, "unloading_city_gcid": 9998},
        ]
    )
    assert _reasons(result, 1) == {Reason.ORIGIN_CITY_NOT_FOUND}
    assert _reasons(result, 2) == {Reason.DESTINATION_CITY_NOT_FOUND}
    assert _reasons(result, 3) == {Reason.ORIGIN_CITY_NOT_FOUND, Reason.DESTINATION_CITY_NOT_FOUND}
    assert result.deals.height == 0


def test_city_dimension_keeps_smallest_id_per_gc_id():
    cities = _run([{"id": 1}]).dim_city
    kazan = cities.filter(pl.col("gc_id") == 1003).to_dicts()[0]
    assert kazan["source_city_id"] == 3
    assert kazan["full_name_local"] == "Казань, Республика Татарстан"
    assert kazan["region_name"] == "Республика Татарстан"
    assert kazan["duplicate_count"] == 2
    assert cities["gc_id"].is_unique().all()


# 8. Outlier preservation and data scope


def test_outlier_non_fact_and_future_deals_are_kept_as_is():
    result = _run(
        [
            {"id": 1, "outlier_mark": True},
            {"id": 2, "is_fact": False, "price_type": 6},
            {"id": 3, "loading_date": date(2026, 11, 20), "created_at": datetime(2026, 10, 26, 9, 0),
             "is_fact": False, "outlier_mark": False},
        ]
    )
    assert result.reasons.height == 0
    assert _deal(result, 1)["outlier_mark"] is True
    assert _deal(result, 2)["is_fact"] is False
    assert _deal(result, 3)["outlier_mark"] is False
    profile = result.stats["accepted_profile"]
    assert profile["outlier_marked"] == 1
    assert profile["future_loading_date"] == 1
    assert profile["created_on_as_of_date"] == 1


# 9. Quarantine reason codes


def test_every_failed_rule_is_recorded_with_detail_and_source_record():
    result = _run(
        [
            {"id": 1, "price": 0.0, "distance": 0.0, "total_weight": -1.0, "total_volume": -1.0,
             "loading_city_gcid": 9999, "unloading_city_gcid": 9998},
            {"id": 2, "currency_code": "156"},
            {"id": 3},
        ]
    )
    assert _reasons(result, 1) == {
        Reason.PRICE_NON_POSITIVE, Reason.DISTANCE_NON_POSITIVE, Reason.WEIGHT_NEGATIVE,
        Reason.VOLUME_NEGATIVE, Reason.ORIGIN_CITY_NOT_FOUND, Reason.DESTINATION_CITY_NOT_FOUND,
    }
    assert _reasons(result, 2) == {Reason.CURRENCY_UNSUPPORTED}
    assert set(result.reasons["reason_code"].to_list()) <= set(REASON_CODES)
    assert result.quarantine.height == result.reasons.height == 7
    assert result.quarantine["reason_detail"].null_count() == 0
    assert result.quarantine.filter(pl.col("price_id") == 2)["source_record"].item().find('"currency_code": "156"') >= 0
    assert result.deals["price_id"].to_list() == [3]
    assert result.stats["rejected_count"] == 2
    assert result.stats["reason_counts"][Reason.PRICE_NON_POSITIVE] == 1


# Body groups


def test_child_types_in_one_group_do_not_multiply_a_deal():
    result = _run(
        [{"id": 1, "body_type": 56}, {"id": 2, "body_type": 56}, {"id": 3, "body_type": None},
         {"id": 4, "body_type": 15}],
        price_body_types=[(1, 1, 56), (2, 1, 61), (3, 2, 56), (4, 2, 15)],
    )
    bridge = {(row["price_id"], row["body_group_code"]): row["resolution"] for row in result.bridge.to_dicts()}
    assert bridge == {
        (1, "TENT"): "links",
        (2, "TENT"): "links",
        (2, "BORT"): "links",
        (3, "UNSPECIFIED"): "unspecified",
        (4, "BORT"): "fallback_body_type",
    }
    assert _deal(result, 3)["body_group_code"] == "UNSPECIFIED"


def test_unknown_body_type_becomes_inferred_dimension_member():
    result = _run([{"id": 1, "body_type": 99}])
    inferred = result.dim_body_type.filter(pl.col("body_type_id") == 99).to_dicts()
    assert inferred and inferred[0]["source_system"] == "etl_inferred"
    assert inferred[0]["body_group_code"] == "OTHER"


def test_window_is_rejected_when_reversed():
    with pytest.raises(ValueError):
        DateWindow(date(2026, 10, 26), date(2026, 7, 26))
    with pytest.raises(ValueError):
        DateWindow(date(2026, 10, 26), None)
