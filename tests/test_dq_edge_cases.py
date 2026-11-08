"""Edge cases through transform and the real warehouse merge, inside a rolled-back transaction.

Fixture ids start at 980_000_000, cities at 980001, and dates are in 2099.
"""

from __future__ import annotations

from datetime import date

import pytest
from sqlalchemy import text

from etl.config import DateWindow
from etl.load.warehouse import load_snapshot
from etl.transform.normalize import transform_snapshot
from tests.etl_factories import AS_OF, default_rates, make_snapshot

pytestmark = pytest.mark.integration

BASE = 980_000_000
WINDOW = DateWindow(date(2099, 1, 1), date(2099, 12, 31))
CITIES = [
    (980001, 980001, "Краеград", "Краеград, Пограничная область"),
    (980002, 980002, "Междуреченск", "Междуреченск, Пограничная область"),
    (980009, 980001, "Краеград", "Краеград, дубль"),
]
RATES = default_rates([date(2099, 1, 5), date(2099, 1, 10)], start_id=BASE)

CASES = {
    "null_exchange_date_usd": ({"currency_code": "840", "price": 2000.0, "nearest_rate_date": None},
                               {"FX_RATE_MISSING"}),
    "null_exchange_date_rub": ({"nearest_rate_date": None}, None),
    "foreign_without_exact_rate": ({"currency_code": "978", "price": 2000.0, "nearest_rate_date": date(2099, 1, 6)},
                                   {"FX_RATE_MISSING"}),
    "rub_exactly_1000": ({"price": 1000.0}, {"RUB_PRICE_LE_1000"}),
    "rub_just_above_1000": ({"price": 1000.01}, None),
    "zero_distance": ({"distance": 0.0}, {"DISTANCE_NON_POSITIVE"}),
    "negative_weight": ({"total_weight": -0.5}, {"WEIGHT_NEGATIVE"}),
    "zero_weight": ({"total_weight": 0.0, "weight_category": None}, None),
    "zero_volume": ({"total_volume": 0.0, "volume_category": None}, None),
    "volume_exactly_120": ({"total_volume": 120.0, "volume_category": None}, None),
    "volume_above_120": ({"total_volume": 120.5, "volume_category": None}, None),
    "missing_origin_city": ({"loading_city_gcid": 989999}, {"ORIGIN_CITY_NOT_FOUND"}),
    "missing_destination_city": ({"unloading_city_gcid": 989998}, {"DESTINATION_CITY_NOT_FOUND"}),
    "duplicate_city_gc_id": ({"loading_city_gcid": 980001, "unloading_city_gcid": 980001}, None),
    "child_types_same_group": ({"body_type": 56}, None),
    "outlier_marked": ({"outlier_mark": True}, None),
    "future_planned": ({"is_fact": False, "price_type": 6, "loading_date": date(2099, 6, 1),
                        "end_loading_date": date(2099, 6, 1)}, None),
}
IDS = {name: BASE + index for index, name in enumerate(CASES, start=1)}


def _row(name: str, overrides: dict) -> dict:
    return {
        "id": IDS[name],
        "item_id": IDS[name],
        "loading_city_gcid": 980001,
        "unloading_city_gcid": 980002,
        "loading_date": date(2099, 1, 12),
        "end_loading_date": date(2099, 1, 12),
        "nearest_rate_date": date(2099, 1, 10),
        **overrides,
    }


@pytest.fixture(scope="module")
def snapshot():
    prices = [_row(name, overrides) for name, (overrides, _) in CASES.items()]
    links = [(BASE + 10 * index, row["id"], row["body_type"] if "body_type" in row else 56)
             for index, row in enumerate(prices, start=1)]
    same_group = IDS["child_types_same_group"]
    links += [(BASE + 9001, same_group, 57), (BASE + 9002, same_group, 61)]
    return make_snapshot(prices, cities=CITIES, rates=RATES, price_body_types=links, window=WINDOW)


@pytest.fixture
def loaded(dwh_tx, snapshot):
    batch_id = dwh_tx.execute(text(
        "INSERT INTO audit.etl_batch (source_system, mode, status, start_date, end_date) "
        "VALUES ('test_edge', 'window', 'running', '2099-01-01', '2099-12-31') RETURNING batch_id"
    )).scalar_one()
    result = transform_snapshot(snapshot, AS_OF)
    load_snapshot(dwh_tx, batch_id=batch_id, window=WINDOW, snapshot=snapshot, result=result,
                  source_system="test_edge")
    return dwh_tx


def _fact(conn, name):
    return conn.execute(text("SELECT * FROM core.fact_deal WHERE price_id = :id"), {"id": IDS[name]}).mappings().first()


def _reasons(conn, name):
    return set(conn.execute(text("SELECT reason_code FROM audit.quarantine_price WHERE price_id = :id"),
                            {"id": IDS[name]}).scalars().all())


@pytest.mark.parametrize("name", list(CASES))
def test_edge_case_outcome(loaded, name):
    expected_reasons = CASES[name][1]
    fact = _fact(loaded, name)
    reasons = _reasons(loaded, name)
    if expected_reasons is None:
        assert fact is not None and reasons == set()
    else:
        assert fact is None and reasons == expected_reasons
        detail, record = loaded.execute(text(
            "SELECT reason_detail, source_record FROM audit.quarantine_price WHERE price_id = :id LIMIT 1"
        ), {"id": IDS[name]}).one()
        assert detail and record["id"] == IDS[name]


def test_conversion_and_dates_for_accepted_edges(loaded):
    above = _fact(loaded, "rub_just_above_1000")
    assert str(above["price_rub"]) == "1000.01" and str(above["fx_rate"]) == "1.000000"
    rub_null = _fact(loaded, "null_exchange_date_rub")
    assert rub_null["nearest_rate_date"] is None and rub_null["fx_rate_date"] is None


def test_categories_at_boundaries(loaded):
    assert _fact(loaded, "zero_weight")["weight_category"] == "OTH"
    assert _fact(loaded, "zero_weight")["weight_category_imputed"] is True
    assert _fact(loaded, "zero_volume")["volume_category"] == "OTH"
    assert _fact(loaded, "volume_exactly_120")["volume_category"] == "FTL"
    assert _fact(loaded, "volume_above_120")["volume_category"] == "OTH"


def test_duplicate_gc_id_resolves_to_one_city_without_multiplying(loaded):
    city = loaded.execute(text(
        "SELECT source_city_id, full_name_local, duplicate_count FROM core.dim_city WHERE gc_id = 980001"
    )).one()
    assert city.source_city_id == 980001 and city.duplicate_count == 2
    joined = loaded.execute(text(
        "SELECT count(*) FROM core.fact_deal f JOIN core.dim_city c ON c.gc_id = f.loading_city_gcid "
        "WHERE f.price_id = :id"
    ), {"id": IDS["duplicate_city_gc_id"]}).scalar_one()
    assert joined == 1


def test_child_body_types_in_one_group_keep_one_row(loaded):
    rows = loaded.execute(text(
        "SELECT body_group_code, resolution FROM core.bridge_deal_body_group WHERE price_id = :id"
    ), {"id": IDS["child_types_same_group"]}).all()
    assert [tuple(row) for row in rows] == [("TENT", "links")]
    raw_links = loaded.execute(text("SELECT count(*) FROM raw.price_body_types WHERE price_id = :id"),
                               {"id": IDS["child_types_same_group"]}).scalar_one()
    assert raw_links == 3


def test_outlier_and_future_deals_are_preserved(loaded):
    outlier = _fact(loaded, "outlier_marked")
    assert outlier["outlier_mark"] is True
    future = _fact(loaded, "future_planned")
    assert future["is_fact"] is False and future["loading_date"] > AS_OF
