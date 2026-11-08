"""Load idempotency and backfill against the real warehouse.

Each test runs in one transaction that is rolled back. Fixture ids and the year 2099
keep fixture rows away from loaded data, so window deletes never touch real deals.
"""

from __future__ import annotations

from datetime import date

import pytest
from sqlalchemy import create_engine, text

from etl.config import DateWindow, load_config
from etl.load.warehouse import load_snapshot
from etl.transform.normalize import transform_snapshot
from tests.etl_factories import AS_OF, default_rates, make_snapshot

CITIES = [
    (990001, 990001, "Тестград", "Тестград, Тестовая область"),
    (990002, 990002, "Проверск", "Проверск, Тестовая область"),
    (990003, 990001, "Тестград", "Тестград, дубль"),
]
RATE_DAYS = [date(2099, 1, 5), date(2099, 2, 5)]
JAN = DateWindow(date(2099, 1, 1), date(2099, 1, 31))
FEB = DateWindow(date(2099, 2, 1), date(2099, 2, 28))
BASE = 990_000_000


def _price(offset: int, **overrides) -> dict:
    loading = overrides.pop("loading_date", date(2099, 1, 10))
    return {
        "id": BASE + offset,
        "loading_city_gcid": 990001,
        "unloading_city_gcid": 990002,
        "loading_date": loading,
        "end_loading_date": loading,
        "nearest_rate_date": date(2099, 1, 5),
        "item_id": BASE + offset,
        **overrides,
    }


def _snapshot(prices: list[dict], window: DateWindow, **kwargs):
    return make_snapshot(
        prices,
        cities=CITIES,
        rates=default_rates(RATE_DAYS, start_id=BASE),
        window=window,
        **kwargs,
    )


@pytest.fixture(scope="module")
def engine():
    config = load_config()
    eng = create_engine(
        config.database.warehouse.url(config.database.host, config.database.port),
        **config.database.engine_kwargs(),
    )
    yield eng
    eng.dispose()


@pytest.fixture
def conn(engine):
    connection = engine.connect()
    transaction = connection.begin()
    try:
        yield connection
    finally:
        transaction.rollback()
        connection.close()


def _load(conn, snapshot, window):
    batch_id = conn.execute(
        text(
            "INSERT INTO audit.etl_batch (source_system, mode, status, start_date, end_date) "
            "VALUES ('test', :mode, 'running', :start_date, :end_date) RETURNING batch_id"
        ),
        {"mode": window.mode, "start_date": window.start_date, "end_date": window.end_date},
    ).scalar_one()
    result = transform_snapshot(snapshot, AS_OF)
    counts = load_snapshot(conn, batch_id=batch_id, window=window, snapshot=snapshot,
                           result=result, source_system="test")
    return batch_id, counts


def _scalar(conn, sql: str):
    return conn.execute(text(sql), {"low": BASE, "high": BASE + 999}).scalar_one()


def _fixture_counts(conn) -> dict:
    return {
        "fact": _scalar(conn, "SELECT count(*) FROM core.fact_deal WHERE price_id BETWEEN :low AND :high"),
        "dates": _scalar(conn, "SELECT count(*) FROM core.fact_deal_date WHERE price_id BETWEEN :low AND :high"),
        "bridge": _scalar(conn, "SELECT count(*) FROM core.bridge_deal_body_group WHERE price_id BETWEEN :low AND :high"),
        "quarantine": _scalar(conn, "SELECT count(*) FROM audit.quarantine_price WHERE price_id BETWEEN :low AND :high"),
        "raw": _scalar(conn, "SELECT count(*) FROM raw.prices WHERE id BETWEEN :low AND :high"),
        "rates": _scalar(conn, "SELECT count(*) FROM core.dim_exchange_rate WHERE rate_date >= '2099-01-01'"),
        "cities": _scalar(conn, "SELECT count(*) FROM core.dim_city WHERE gc_id >= 990001 AND gc_id <= 990002"),
    }


JAN_PRICES = [
    _price(1, price=84500.0, body_type=56),
    _price(2, price=2480.0, currency_code="840"),
    _price(3, loading_city_gcid=990002, unloading_city_gcid=990002),
    _price(4, price=900.0),
    _price(5, price=2480.0, currency_code="978", nearest_rate_date=None),
]


# 10. Duplicate prevention


def test_same_batch_loaded_twice_creates_no_duplicates(conn):
    snapshot = _snapshot(JAN_PRICES, JAN)
    _, first = _load(conn, snapshot, JAN)
    after_first = _fixture_counts(conn)
    _, second = _load(conn, snapshot, JAN)
    after_second = _fixture_counts(conn)

    assert after_first == after_second
    assert after_first["fact"] == 3 and after_first["quarantine"] == 2
    assert after_first["cities"] == 2 and after_first["rates"] == 6
    assert first["core"]["fact_deal"]["inserted"] == 3
    assert second["core"]["fact_deal"] == {"inserted": 0, "updated": 0, "deleted": 0}
    assert second["quarantine"] == {"inserted": 0, "updated": 0, "deleted": 0}
    assert second["core"]["dim_exchange_rate"] == {"inserted": 0, "updated": 0}
    assert second["raw"]["prices"] == {"deleted": 0, "inserted": 0, "updated": 0}


# 11. Repeated execution keeps historical results and lineage


def test_repeated_run_does_not_change_existing_rows(conn):
    snapshot = _snapshot(JAN_PRICES, JAN)
    first_batch, _ = _load(conn, snapshot, JAN)
    before = conn.execute(
        text("SELECT price_id, price_rub, fx_rate, batch_id FROM core.fact_deal "
             "WHERE price_id BETWEEN :low AND :high ORDER BY price_id"),
        {"low": BASE, "high": BASE + 999},
    ).all()
    second_batch, _ = _load(conn, snapshot, JAN)
    after = conn.execute(
        text("SELECT price_id, price_rub, fx_rate, batch_id FROM core.fact_deal "
             "WHERE price_id BETWEEN :low AND :high ORDER BY price_id"),
        {"low": BASE, "high": BASE + 999},
    ).all()

    assert second_batch != first_batch
    assert before == after
    assert {row.batch_id for row in after} == {first_batch}
    usd = next(row for row in after if row.price_id == BASE + 2)
    assert str(usd.fx_rate) == "90.500000" and str(usd.price_rub) == "224440.00"
    quarantine_batches = conn.execute(
        text("SELECT DISTINCT batch_id FROM audit.quarantine_price WHERE price_id BETWEEN :low AND :high"),
        {"low": BASE, "high": BASE + 999},
    ).scalars().all()
    assert quarantine_batches == [first_batch]


# 12. Backfill


def test_backfill_replaces_only_its_window(conn):
    feb_prices = [_price(101, loading_date=date(2099, 2, 10), nearest_rate_date=date(2099, 2, 5))]
    _load(conn, _snapshot(JAN_PRICES, JAN), JAN)
    feb_batch, _ = _load(conn, _snapshot(feb_prices, FEB), FEB)

    jan_v2 = [
        _price(1, price=90000.0, body_type=56),
        _price(3, loading_city_gcid=990002, unloading_city_gcid=990002, distance=0.0),
        _price(4, price=1500.0),
        _price(5, price=2480.0, currency_code="978", nearest_rate_date=None),
    ]
    links = [(1, BASE + 1, 56), (2, BASE + 1, 15)]
    _, counts = _load(conn, _snapshot(jan_v2, JAN, price_body_types=links), JAN)

    fact = counts["core"]["fact_deal"]
    assert fact["inserted"] == 1
    assert fact["updated"] == 1
    assert fact["deleted"] == 2
    assert counts["quarantine"]["inserted"] == 1 and counts["quarantine"]["deleted"] == 1

    deals = dict(conn.execute(
        text("SELECT price_id, price_rub FROM core.fact_deal WHERE price_id BETWEEN :low AND :high"),
        {"low": BASE, "high": BASE + 999},
    ).all())
    assert set(deals) == {BASE + 1, BASE + 4, BASE + 101}
    assert str(deals[BASE + 1]) == "90000.00"

    feb_row = conn.execute(
        text("SELECT batch_id FROM core.fact_deal WHERE price_id = :id"), {"id": BASE + 101}
    ).scalar_one()
    assert feb_row == feb_batch

    reasons = conn.execute(
        text("SELECT price_id, reason_code FROM audit.quarantine_price "
             "WHERE price_id BETWEEN :low AND :high ORDER BY 1, 2"),
        {"low": BASE, "high": BASE + 999},
    ).all()
    assert [tuple(row) for row in reasons] == [
        (BASE + 3, "DISTANCE_NON_POSITIVE"),
        (BASE + 5, "FX_RATE_MISSING"),
    ]
    bridge = conn.execute(
        text("SELECT body_group_code FROM core.bridge_deal_body_group WHERE price_id = :id ORDER BY 1"),
        {"id": BASE + 1},
    ).scalars().all()
    assert bridge == ["BORT", "TENT"]
    assert _scalar(conn, "SELECT count(*) FROM raw.prices WHERE id BETWEEN :low AND :high") == 5


def test_reprocessing_after_failed_load_leaves_no_partial_rows(conn):
    bad = _snapshot([_price(1, price_type=7)], JAN)
    savepoint = conn.begin_nested()
    with pytest.raises(Exception):
        _load(conn, bad, JAN)
    savepoint.rollback()
    assert _fixture_counts(conn)["fact"] == 0
    assert _fixture_counts(conn)["raw"] == 0

    _, counts = _load(conn, _snapshot([_price(1)], JAN), JAN)
    assert counts["core"]["fact_deal"]["inserted"] == 1
