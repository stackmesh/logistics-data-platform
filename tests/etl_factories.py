"""Small in-memory source snapshots for ETL tests."""

from __future__ import annotations

from datetime import date, datetime, timedelta, timezone

import polars as pl

from etl.config import DateWindow
from etl.extract.source import (
    BODY_TYPE_SCHEMA,
    CITY_SCHEMA,
    EXCHANGE_RATE_SCHEMA,
    PRICE_BODY_TYPE_SCHEMA,
    PRICE_DATE_SCHEMA,
    PRICE_SCHEMA,
    SourceSnapshot,
)

AS_OF = date(2026, 10, 26)

DEFAULT_PRICE = {
    "id": 1,
    "created_at": datetime(2026, 9, 10, 8, 0),
    "updated_at": datetime(2026, 9, 10, 8, 0),
    "subscriber_id": 101,
    "organization_id": 100707,
    "loading_date": date(2026, 9, 15),
    "loading_city_gcid": 1001,
    "unloading_city_gcid": 1003,
    "distance": 897.0,
    "total_weight": 20.0,
    "total_volume": 82.0,
    "body_type": 56,
    "price": 84500.0,
    "currency_code": "643",
    "complex": False,
    "price_type": 3,
    "item_id": 5000001,
    "is_fact": True,
    "nearest_rate_date": date(2026, 9, 10),
    "meta_item_id": None,
    "end_loading_date": date(2026, 9, 15),
    "source": "data-hub",
    "external_item_uuid": None,
    "outlier_mark": False,
    "weight_category": "FTL",
    "volume_category": "FTL",
    "price_group_id": None,
}

BODY_TYPES = [
    (1, "Бортовой", 1, 15), (2, "Изотерм", 2, 2), (3, "Цельнометаллический", 3, None),
    (4, "Рефрижератор", 4, 4), (15, "Борт", 15, 15), (46, "Реф. мультирежимный", 46, 4),
    (47, "Реф. с перегородкой", 47, 4), (48, "Реф.-тушевоз", 48, 4), (50, "Контейнер", 50, None),
    (56, "Тент", 56, 56), (57, "Шторный", 57, 56), (58, "Тент верх", 58, 56),
    (59, "Тент бок", 59, 56), (60, "Тент зад", 60, 56), (61, "Тент полный", 61, 56),
    (65, "Самосвал", 65, None), (68, "Автовоз", 68, None),
]


def cities_frame(rows: list[tuple]) -> pl.DataFrame:
    """rows: (id, gc_id, name, full_name_local)."""
    stamp = datetime(2026, 1, 1)
    return pl.DataFrame(
        [
            (city_id, full_name, full_name, gc_id, name, 37.6, 55.7, stamp, stamp)
            for city_id, gc_id, name, full_name in rows
        ],
        schema=CITY_SCHEMA,
        orient="row",
    )


DEFAULT_CITIES = [
    (1, 1001, "Москва", "Москва, Московская область"),
    (2, 1002, "Санкт-Петербург", "Санкт-Петербург, Ленинградская область"),
    (3, 1003, "Казань", "Казань, Республика Татарстан"),
    (41, 1001, "Москва", "Москва, Россия"),
    (42, 1003, "Казань", "Казань, Татарстан"),
]


def rates_frame(rows: list[tuple]) -> pl.DataFrame:
    """rows: (id, num_code, value, setting_date)."""
    return pl.DataFrame(rows, schema=EXCHANGE_RATE_SCHEMA, orient="row")


def default_rates(days: list[date], start_id: int = 1) -> list[tuple]:
    rows = []
    next_id = start_id
    for day in days:
        for code, value in (("643", 1.0), ("840", 90.5), ("978", 98.25)):
            rows.append((next_id, code, value, day))
            next_id += 1
    return rows


def make_snapshot(
    prices: list[dict],
    *,
    cities: list[tuple] | None = None,
    rates: list[tuple] | None = None,
    price_body_types: list[tuple] | None = None,
    price_dates: list[tuple] | None = None,
    window: DateWindow = DateWindow(),
    extracted_at: datetime = datetime(2026, 10, 27, tzinfo=timezone.utc),
) -> SourceSnapshot:
    rows = [{**DEFAULT_PRICE, **price} for price in prices]
    if price_body_types is None:
        price_body_types = [
            (row["id"] * 10, row["id"], row["body_type"]) for row in rows if row["body_type"] is not None
        ]
    if price_dates is None:
        price_dates = [(row["id"] * 10, row["id"], row["loading_date"]) for row in rows]
    if rates is None:
        base = date(2026, 9, 1)
        rates = default_rates([base + timedelta(days=offset) for offset in range(45)])
    return SourceSnapshot(
        window=window,
        extracted_at=extracted_at,
        prices=pl.DataFrame([tuple(row[name] for name in PRICE_SCHEMA) for row in rows],
                            schema=PRICE_SCHEMA, orient="row"),
        price_body_types=pl.DataFrame(price_body_types, schema=PRICE_BODY_TYPE_SCHEMA, orient="row"),
        price_dates=pl.DataFrame(price_dates, schema=PRICE_DATE_SCHEMA, orient="row"),
        cities=cities_frame(cities if cities is not None else DEFAULT_CITIES),
        body_types=pl.DataFrame(BODY_TYPES, schema=BODY_TYPE_SCHEMA, orient="row"),
        exchange_rates=rates_frame(rates),
    )
