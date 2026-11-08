"""Repeated and overlapping runs against the real 300k snapshot."""

from __future__ import annotations

from datetime import date

import pytest
from sqlalchemy import text

from etl.config import DateWindow
from etl.extract.source import extract_snapshot
from etl.load.warehouse import load_snapshot
from etl.pipeline.runner import run_pipeline
from etl.quality.fingerprint import fingerprint, logical
from etl.transform.normalize import transform_snapshot

pytestmark = pytest.mark.integration

BRIEF_WINDOW = DateWindow(date(2026, 7, 26), date(2026, 10, 26))
WINDOW_A = DateWindow(date(2026, 8, 1), date(2026, 9, 30))
WINDOW_B = DateWindow(date(2026, 9, 1), date(2026, 10, 31))
UNION = DateWindow(date(2026, 8, 1), date(2026, 10, 31))
OVERLAP = DateWindow(date(2026, 9, 1), date(2026, 9, 30))
B_ONLY = DateWindow(date(2026, 10, 1), date(2026, 10, 31))


def _fp(engine, window):
    with engine.connect() as conn:
        return fingerprint(conn, window)


def _no_changes(summary):
    load = summary["load"]
    assert load["core"]["fact_deal"] == {"inserted": 0, "updated": 0, "deleted": 0,
                                         "unchanged": summary["accepted_count"]}
    assert load["core"]["fact_deal_date"] == {"inserted": 0, "deleted": 0}
    assert load["core"]["bridge_deal_body_group"] == {"inserted": 0, "updated": 0, "deleted": 0}
    assert load["quarantine"] == {"inserted": 0, "updated": 0, "deleted": 0}
    for table in ("prices", "price_body_types", "price_dates"):
        assert load["raw"][table] == {"inserted": 0, "updated": 0, "deleted": 0}, table


def test_same_window_twice_leaves_dataset_identical(loaded_warehouse, etl_config, dwh_engine):
    full_before = _fp(dwh_engine, DateWindow())
    first = run_pipeline(BRIEF_WINDOW, etl_config)
    after_first = _fp(dwh_engine, BRIEF_WINDOW)
    second = run_pipeline(BRIEF_WINDOW, etl_config)
    after_second = _fp(dwh_engine, BRIEF_WINDOW)

    assert after_first == after_second
    assert after_first["fact_rows"] > 40_000
    for key in ("extracted_count", "accepted_count", "rejected_count", "reason_counts"):
        assert first[key] == second[key], key
    _no_changes(first)
    _no_changes(second)
    assert _fp(dwh_engine, DateWindow()) == full_before


def test_overlapping_windows_do_not_multiply_records(loaded_warehouse, etl_config, dwh_engine):
    full_before = _fp(dwh_engine, DateWindow())
    overlap_before = _fp(dwh_engine, OVERLAP)
    first = run_pipeline(WINDOW_A, etl_config)
    second = run_pipeline(WINDOW_B, etl_config)

    _no_changes(first)
    _no_changes(second)
    assert _fp(dwh_engine, OVERLAP) == overlap_before
    assert _fp(dwh_engine, DateWindow()) == full_before


def _new_batch(conn, window):
    return conn.execute(
        text("INSERT INTO audit.etl_batch (source_system, mode, status, start_date, end_date) "
             "VALUES ('test_backfill', 'window', 'running', :s, :e) RETURNING batch_id"),
        {"s": window.start_date, "e": window.end_date},
    ).scalar_one()


def test_backfill_into_empty_range_with_overlapping_windows(loaded_warehouse, etl_config, src_engine, dwh_tx):
    """Remove three months, reload them as two overlapping windows, compare with the full load."""
    baseline = logical(fingerprint(dwh_tx, UNION))
    baseline_overlap = logical(fingerprint(dwh_tx, OVERLAP))
    expected_b_only = fingerprint(dwh_tx, B_ONLY)["fact_rows"]
    params = UNION.sql_params()
    dwh_tx.execute(text("DELETE FROM core.fact_deal WHERE loading_date BETWEEN :start_date AND :end_date"), params)
    dwh_tx.execute(text("DELETE FROM audit.quarantine_price WHERE loading_date BETWEEN :start_date AND :end_date"),
                   params)
    dwh_tx.execute(text("DELETE FROM raw.prices WHERE loading_date BETWEEN :start_date AND :end_date"), params)
    assert fingerprint(dwh_tx, UNION)["fact_rows"] == 0

    loads = {}
    for name, window in (("A", WINDOW_A), ("B", WINDOW_B)):
        snapshot = extract_snapshot(src_engine, window, etl_config.fetch_size)
        result = transform_snapshot(snapshot, etl_config.as_of_date)
        loads[name] = load_snapshot(dwh_tx, batch_id=_new_batch(dwh_tx, window), window=window,
                                    snapshot=snapshot, result=result, source_system="test_backfill")
        loads[name]["accepted"] = result.stats["accepted_count"]

    assert logical(fingerprint(dwh_tx, UNION)) == baseline
    assert logical(fingerprint(dwh_tx, OVERLAP)) == baseline_overlap
    b = loads["B"]["core"]["fact_deal"]
    assert b["inserted"] == expected_b_only
    assert b["updated"] == 0 and b["deleted"] == 0
    assert loads["A"]["core"]["fact_deal"]["inserted"] == loads["A"]["accepted"]
    assert loads["B"]["raw"]["prices"]["updated"] == 0
