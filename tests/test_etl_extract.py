"""Window extraction against the real source database."""

from __future__ import annotations

from datetime import date

import polars as pl
import pytest
from sqlalchemy import text

from etl.config import DateWindow, load_config, source_engine
from etl.extract.source import extract_snapshot

pytestmark = pytest.mark.integration


def test_window_is_inclusive_on_loading_date_and_children_follow_parents():
    config = load_config()
    engine = source_engine(config)
    window = DateWindow(date(2026, 10, 25), date(2026, 10, 26))
    try:
        snapshot = extract_snapshot(engine, window, fetch_size=1000)
        with engine.connect() as conn:
            expected = conn.execute(
                text("SELECT count(*), min(loading_date), max(loading_date) FROM prices "
                     "WHERE loading_date BETWEEN '2026-10-25' AND '2026-10-26'")
            ).one()
            city_rows = conn.execute(text("SELECT count(*) FROM cities")).scalar_one()
    finally:
        engine.dispose()

    prices = snapshot.prices
    assert prices.height == expected[0] > 0
    assert prices["loading_date"].min() == expected[1] == date(2026, 10, 25)
    assert prices["loading_date"].max() == expected[2] == date(2026, 10, 26)
    assert prices["id"].is_sorted()
    ids = set(prices["id"].to_list())
    assert set(snapshot.price_body_types["price_id"].unique().to_list()) <= ids
    assert set(snapshot.price_dates["price_id"].unique().to_list()) <= ids
    assert snapshot.cities.height == city_rows
    assert snapshot.exchange_rates.filter(pl.col("num_code") == "643").height > 0
    assert snapshot.extracted_at.tzinfo is not None
