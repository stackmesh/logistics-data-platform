"""Every extracted source deal has exactly one explainable outcome in the warehouse."""

from __future__ import annotations

from datetime import date

import polars as pl
import pytest
from sqlalchemy import text

from etl.config import DateWindow
from etl.quality.reconcile import build_outcomes, by_month, summarize

pytestmark = pytest.mark.integration

WINDOWS = {
    "full": DateWindow(),
    "brief_window": DateWindow(date(2026, 7, 26), date(2026, 10, 26)),
    "as_of_day": DateWindow(date(2026, 10, 26), date(2026, 10, 26)),
    "future": DateWindow(date(2026, 10, 27), date(2027, 12, 31)),
    "first_year": DateWindow(date(2023, 1, 1), date(2023, 12, 31)),
}


def _outcomes(src_engine, connection, window):
    with src_engine.connect() as source:
        source.exec_driver_sql("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY")
        return build_outcomes(source, connection, window)


@pytest.mark.parametrize("name", list(WINDOWS))
def test_window_reconciles_without_unexplained_records(name, loaded_warehouse, src_engine, dwh_tx):
    window = WINDOWS[name]
    report = summarize(_outcomes(src_engine, dwh_tx, window))

    assert report["source_count"] > 0
    assert report["unexplained"] == 0, report["mismatch_examples"]
    assert report["duplicate_price_ids"] == 0
    assert all(count == 0 for count in report["mismatches"].values()), report["mismatches"]
    assert report["source_count"] == report["raw_count"]
    assert report["source_count"] == report["fact_count"] + report["quarantined_count"]
    assert report["accepted_explained"] == report["fact_count"]
    assert report["quarantined_explained"] == report["quarantined_count"]


def test_every_month_balances(loaded_warehouse, src_engine, dwh_tx):
    months = by_month(_outcomes(src_engine, dwh_tx, DateWindow()))
    assert len(months) > 12
    for month in months:
        assert month["unexplained"] == 0, month
        assert month["source"] == month["raw"] == month["accepted"] + month["quarantined"], month


def test_batch_audit_matches_reconciliation(loaded_warehouse, src_engine, dwh_tx):
    report = summarize(_outcomes(src_engine, dwh_tx, DateWindow()))
    batch = dwh_tx.execute(
        text("SELECT status, extracted_count, accepted_count, rejected_count, completed_at "
             "FROM audit.etl_batch WHERE batch_id = :id"),
        {"id": loaded_warehouse["batch_id"]},
    ).one()
    assert batch.status == "succeeded" and batch.completed_at is not None
    assert batch.extracted_count == report["source_count"]
    assert batch.accepted_count == report["fact_count"]
    assert batch.rejected_count == report["quarantined_count"]
    stats = dict(dwh_tx.execute(
        text("SELECT reason_code, rows_rejected FROM audit.dq_statistic WHERE batch_id = :id AND rows_rejected > 0"),
        {"id": loaded_warehouse["batch_id"]},
    ).all())
    assert stats == report["reason_counts"]


def test_no_duplicate_keys_anywhere(loaded_warehouse, dwh_tx):
    checks = dwh_tx.execute(text("""
        SELECT
          (SELECT count(*) - count(DISTINCT price_id) FROM core.fact_deal) AS fact,
          (SELECT count(*) - count(DISTINCT id) FROM raw.prices) AS raw,
          (SELECT count(*) - count(DISTINCT (price_id, price_date)) FROM core.fact_deal_date) AS dates,
          (SELECT count(*) - count(DISTINCT (price_id, body_group_code)) FROM core.bridge_deal_body_group) AS bridge,
          (SELECT count(*) - count(DISTINCT (price_id, reason_code)) FROM audit.quarantine_price) AS quarantine,
          (SELECT count(*) - count(DISTINCT (currency_code, rate_date)) FROM core.dim_exchange_rate) AS rates,
          (SELECT count(*) - count(DISTINCT gc_id) FROM core.dim_city) AS cities,
          (SELECT count(*) FROM core.fact_deal f
            WHERE EXISTS (SELECT 1 FROM audit.quarantine_price q WHERE q.price_id = f.price_id)) AS fact_and_quarantine
    """)).mappings().one()
    assert all(value == 0 for value in checks.values()), dict(checks)


def test_reconciliation_detects_tampering(loaded_warehouse, src_engine, dwh_tx):
    """Negative control: the oracle must flag a wrong amount, a lost deal, and a double outcome."""
    ids = dwh_tx.execute(text(
        "SELECT price_id FROM core.fact_deal WHERE loading_date = '2026-10-26' ORDER BY price_id LIMIT 3"
    )).scalars().all()
    dwh_tx.execute(text("UPDATE core.fact_deal SET price_rub = price_rub + 0.01 WHERE price_id = :id"),
                   {"id": ids[0]})
    dwh_tx.execute(text("UPDATE core.fact_deal SET weight_category = 'OTH' WHERE price_id = :id "
                        "AND weight_category <> 'OTH'"), {"id": ids[1]})
    dwh_tx.execute(text("DELETE FROM core.fact_deal WHERE price_id = :id"), {"id": ids[2]})
    window = WINDOWS["as_of_day"]
    frame = _outcomes(src_engine, dwh_tx, window)
    report = summarize(frame)

    assert report["mismatches"]["price_rub"] == 1
    assert report["mismatches"]["missing"] == 1
    assert report["unexplained"] >= 2
    flagged = set(frame.filter(pl.col("outcome") == "unexplained")["price_id"].to_list())
    assert {ids[0], ids[2]} <= flagged
