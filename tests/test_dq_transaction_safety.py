"""Failed, killed, and concurrent loads cannot leave partial results in the warehouse."""

from __future__ import annotations

import threading
import time
from dataclasses import replace
from datetime import date

import polars as pl
import pytest
from sqlalchemy import text

import etl.load.warehouse as warehouse_module
import etl.pipeline.runner as runner
from etl.config import DateWindow
from etl.extract.source import extract_snapshot
from etl.load.warehouse import ADVISORY_LOCK_KEY, load_snapshot
from etl.quality.fingerprint import fingerprint
from etl.transform.normalize import transform_snapshot

pytestmark = pytest.mark.integration

WINDOW = DateWindow(date(2026, 10, 20), date(2026, 10, 26))


class InjectedFailure(RuntimeError):
    pass


def _fp(engine):
    with engine.connect() as conn:
        return fingerprint(conn, DateWindow())


def _tampering_extract(original):
    """Return a snapshot whose prices are doubled, so any partial commit would be visible."""
    def extract(engine, window, fetch_size):
        snapshot = original(engine, window, fetch_size)
        return replace(snapshot, prices=snapshot.prices.with_columns(pl.col("price") * 2))
    return extract


def _assert_failed_batch_left_nothing(engine, before):
    assert _fp(engine) == before
    with engine.connect() as conn:
        batch = conn.execute(text(
            "SELECT batch_id, status, error_message, completed_at FROM audit.etl_batch "
            "ORDER BY batch_id DESC LIMIT 1"
        )).one()
        assert batch.status == "failed"
        assert "InjectedFailure" in batch.error_message
        assert batch.completed_at is not None
        leftovers = conn.execute(text("""
            SELECT (SELECT count(*) FROM core.fact_deal WHERE batch_id = :b)
                 + (SELECT count(*) FROM raw.prices WHERE batch_id = :b)
                 + (SELECT count(*) FROM audit.quarantine_price WHERE batch_id = :b)
                 + (SELECT count(*) FROM audit.dq_statistic WHERE batch_id = :b)
        """), {"b": batch.batch_id}).scalar_one()
        assert leftovers == 0


def test_failure_after_all_writes_rolls_back_everything(loaded_warehouse, etl_config, dwh_engine, monkeypatch):
    before = _fp(dwh_engine)
    real_load = runner.load_snapshot

    def load_then_fail(*args, **kwargs):
        real_load(*args, **kwargs)
        raise InjectedFailure("after every merge, before commit")

    monkeypatch.setattr(runner, "extract_snapshot", _tampering_extract(runner.extract_snapshot))
    monkeypatch.setattr(runner, "load_snapshot", load_then_fail)
    with pytest.raises(InjectedFailure):
        runner.run_pipeline(WINDOW, etl_config)
    _assert_failed_batch_left_nothing(dwh_engine, before)


def test_failure_between_raw_and_core_rolls_back_raw(loaded_warehouse, etl_config, dwh_engine, monkeypatch):
    before = _fp(dwh_engine)

    def fail(*args, **kwargs):
        raise InjectedFailure("after raw and dimension merges")

    monkeypatch.setattr(runner, "extract_snapshot", _tampering_extract(runner.extract_snapshot))
    monkeypatch.setattr(warehouse_module, "_load_facts", fail)
    with pytest.raises(InjectedFailure):
        runner.run_pipeline(WINDOW, etl_config)
    _assert_failed_batch_left_nothing(dwh_engine, before)


def test_killed_session_mid_load_leaves_core_untouched(loaded_warehouse, etl_config, src_engine, dwh_engine):
    before = _fp(dwh_engine)
    snapshot = extract_snapshot(src_engine, WINDOW, etl_config.fetch_size)
    snapshot = replace(snapshot, prices=snapshot.prices.with_columns(pl.col("price") * 2))
    result = transform_snapshot(snapshot, etl_config.as_of_date)

    victim = dwh_engine.connect()
    try:
        victim.begin()
        batch_id = victim.execute(text(
            "INSERT INTO audit.etl_batch (source_system, mode, status, start_date, end_date) "
            "VALUES ('test_kill', 'window', 'running', '2026-10-20', '2026-10-26') RETURNING batch_id"
        )).scalar_one()
        load_snapshot(victim, batch_id=batch_id, window=WINDOW, snapshot=snapshot, result=result,
                      source_system="test_kill")
        pid = victim.execute(text("SELECT pg_backend_pid()")).scalar_one()
        with dwh_engine.connect() as killer:
            assert killer.execute(text("SELECT pg_terminate_backend(:pid)"), {"pid": pid}).scalar_one()
    finally:
        victim.invalidate()
        victim.close()

    assert _fp(dwh_engine) == before
    with dwh_engine.connect() as conn:
        assert conn.execute(text("SELECT count(*) FROM audit.etl_batch WHERE batch_id = :b"),
                            {"b": batch_id}).scalar_one() == 0


def test_concurrent_run_waits_for_the_advisory_lock(loaded_warehouse, etl_config, dwh_engine):
    before = _fp(dwh_engine)
    outcome = {}

    def run():
        try:
            outcome["summary"] = runner.run_pipeline(WINDOW, etl_config)
        except Exception as exc:  # noqa: BLE001
            outcome["error"] = exc

    holder = dwh_engine.connect()
    holder.begin()
    holder.execute(text("SELECT pg_advisory_xact_lock(hashtext(:key))"), {"key": ADVISORY_LOCK_KEY})
    worker = threading.Thread(target=run)
    worker.start()
    try:
        waiting = False
        deadline = time.monotonic() + 60
        while time.monotonic() < deadline and not waiting:
            time.sleep(0.5)
            with dwh_engine.connect() as probe:
                waiting = probe.execute(text(
                    "SELECT count(*) > 0 FROM pg_stat_activity "
                    "WHERE application_name = 'logistics_etl_load' AND wait_event_type = 'Lock' "
                    "AND wait_event = 'advisory'"
                )).scalar_one()
        assert waiting, "second loader did not block on the advisory lock"
        assert worker.is_alive()
    finally:
        holder.rollback()
        holder.close()
    worker.join(timeout=120)
    assert not worker.is_alive()
    assert "error" not in outcome, outcome.get("error")
    assert outcome["summary"]["load"]["core"]["fact_deal"]["inserted"] == 0
    assert _fp(dwh_engine) == before
