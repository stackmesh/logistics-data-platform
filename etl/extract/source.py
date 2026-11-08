"""Read one consistent snapshot of deals and reference data from deals_bigdata."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime

import polars as pl
from sqlalchemy import text
from sqlalchemy.engine import Connection, Engine

from etl.config import DateWindow

PRICE_SCHEMA: dict[str, pl.DataType] = {
    "id": pl.Int64,
    "created_at": pl.Datetime("us"),
    "updated_at": pl.Datetime("us"),
    "subscriber_id": pl.Int64,
    "organization_id": pl.Int64,
    "loading_date": pl.Date,
    "loading_city_gcid": pl.Int64,
    "unloading_city_gcid": pl.Int64,
    "distance": pl.Float64,
    "total_weight": pl.Float64,
    "total_volume": pl.Float64,
    "body_type": pl.Int64,
    "price": pl.Float64,
    "currency_code": pl.Utf8,
    "complex": pl.Boolean,
    "price_type": pl.Int64,
    "item_id": pl.Int64,
    "is_fact": pl.Boolean,
    "nearest_rate_date": pl.Date,
    "meta_item_id": pl.Int64,
    "end_loading_date": pl.Date,
    "source": pl.Utf8,
    "external_item_uuid": pl.Utf8,
    "outlier_mark": pl.Boolean,
    "weight_category": pl.Utf8,
    "volume_category": pl.Utf8,
    "price_group_id": pl.Int64,
}

PRICE_BODY_TYPE_SCHEMA: dict[str, pl.DataType] = {
    "id": pl.Int64,
    "price_id": pl.Int64,
    "body_type_id": pl.Int64,
}

PRICE_DATE_SCHEMA: dict[str, pl.DataType] = {
    "id": pl.Int64,
    "price_id": pl.Int64,
    "price_date": pl.Date,
}

CITY_SCHEMA: dict[str, pl.DataType] = {
    "id": pl.Int64,
    "full_name_local": pl.Utf8,
    "full_name_gc": pl.Utf8,
    "gc_id": pl.Int64,
    "name": pl.Utf8,
    "longitude": pl.Float64,
    "latitude": pl.Float64,
    "created_at": pl.Datetime("us"),
    "updated_at": pl.Datetime("us"),
}

BODY_TYPE_SCHEMA: dict[str, pl.DataType] = {
    "id": pl.Int64,
    "name": pl.Utf8,
    "id_from_1c": pl.Int64,
    "std_id": pl.Int64,
}

EXCHANGE_RATE_SCHEMA: dict[str, pl.DataType] = {
    "id": pl.Int64,
    "num_code": pl.Utf8,
    "value": pl.Float64,
    "setting_date": pl.Date,
}

_PRICE_SELECT = ", ".join(
    "p.external_item_uuid::text AS external_item_uuid" if name == "external_item_uuid" else f"p.{name}"
    for name in PRICE_SCHEMA
)


@dataclass(frozen=True)
class SourceSnapshot:
    window: DateWindow
    extracted_at: datetime
    prices: pl.DataFrame
    price_body_types: pl.DataFrame
    price_dates: pl.DataFrame
    cities: pl.DataFrame
    body_types: pl.DataFrame
    exchange_rates: pl.DataFrame

    def counts(self) -> dict[str, int]:
        return {
            "prices": self.prices.height,
            "price_body_types": self.price_body_types.height,
            "price_dates": self.price_dates.height,
            "cities": self.cities.height,
            "body_types": self.body_types.height,
            "exchange_rates": self.exchange_rates.height,
        }


def _window_predicate(window: DateWindow) -> str:
    if window.is_full:
        return "TRUE"
    return "p.loading_date BETWEEN %(start_date)s AND %(end_date)s"


def _read(conn: Connection, sql: str, params: dict, schema: dict, fetch_size: int) -> pl.DataFrame:
    """Stream through a server-side cursor so at most fetch_size rows exist as Python objects."""
    driver = conn.connection.driver_connection
    frames = []
    with driver.cursor(name="etl_extract") as cursor:
        cursor.itersize = fetch_size
        cursor.execute(sql, params)
        while rows := cursor.fetchmany(fetch_size):
            frames.append(pl.DataFrame(rows, schema=schema, orient="row"))
    if not frames:
        return pl.DataFrame(schema=schema)
    return pl.concat(frames, how="vertical", rechunk=True)


def extract_snapshot(engine: Engine, window: DateWindow, fetch_size: int) -> SourceSnapshot:
    """Read the window and all reference tables in one REPEATABLE READ, READ ONLY transaction."""

    predicate = _window_predicate(window)
    params = window.sql_params()
    with engine.connect() as conn:
        conn.exec_driver_sql("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY")
        extracted_at = conn.execute(text("SELECT transaction_timestamp()")).scalar_one()
        prices = _read(
            conn,
            f"SELECT {_PRICE_SELECT} FROM prices AS p WHERE {predicate} ORDER BY p.id",
            params,
            PRICE_SCHEMA,
            fetch_size,
        )
        price_body_types = _read(
            conn,
            "SELECT b.id, b.price_id, b.body_type_id "
            "FROM price_body_types AS b JOIN prices AS p ON p.id = b.price_id "
            f"WHERE {predicate} ORDER BY b.price_id, b.body_type_id, b.id",
            params,
            PRICE_BODY_TYPE_SCHEMA,
            fetch_size,
        )
        price_dates = _read(
            conn,
            "SELECT d.id, d.price_id, d.price_date "
            "FROM price_dates AS d JOIN prices AS p ON p.id = d.price_id "
            f"WHERE {predicate} ORDER BY d.price_id, d.price_date, d.id",
            params,
            PRICE_DATE_SCHEMA,
            fetch_size,
        )
        cities = _read(
            conn,
            "SELECT id, full_name_local, full_name_gc, gc_id, name, longitude, latitude, created_at, updated_at "
            "FROM cities ORDER BY id",
            {},
            CITY_SCHEMA,
            fetch_size,
        )
        body_types = _read(
            conn,
            'SELECT id, name, "id_from_1C" AS id_from_1c, std_id FROM body_types ORDER BY id',
            {},
            BODY_TYPE_SCHEMA,
            fetch_size,
        )
        exchange_rates = _read(
            conn,
            "SELECT id, num_code, value, setting_date FROM exchange_rates ORDER BY id",
            {},
            EXCHANGE_RATE_SCHEMA,
            fetch_size,
        )
        conn.rollback()

    return SourceSnapshot(
        window=window,
        extracted_at=extracted_at,
        prices=prices,
        price_body_types=price_body_types,
        price_dates=price_dates,
        cities=cities,
        body_types=body_types,
        exchange_rates=exchange_rates,
    )
