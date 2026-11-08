"""ClickHouse copy: planning and fingerprint units, and sync/check integration against a scratch database."""

from __future__ import annotations

import re
import uuid
from decimal import Decimal

import pytest

from etl.clickhouse import schema
from etl.clickhouse.check import NUMERIC_TOLERANCES, postgres_round, within_tolerance
from etl.clickhouse.migrate import render, split_statements
from etl.clickhouse.schema import CLICKHOUSE_SQL_DIR, Column
from etl.clickhouse.sync import PartitionAction, SyncError, batch_token, main, parse_month_range, plan_partitions
from logistics.settings import SettingsError

# ---- units ---------------------------------------------------------------------------------------


def test_month_range_parsing():
    assert parse_month_range("2024-11..2025-02") == {"202411", "202412", "202501", "202502"}
    assert parse_month_range("2024-03") == {"202403"}
    for bad in ("2024-13", "2024-1", "2025-02..2024-11", "2024-01..2024-02..2024-03", ""):
        with pytest.raises(ValueError):
            parse_month_range(bad)


def test_plan_partitions_covers_every_case():
    pg = {"202401": (10, "a"), "202402": (5, "b"), "202403": (7, "c"), "202404": (1, "d")}
    ch = {"202401": (10, "a"), "202402": (5, "x"), "202404": (1, "d"), "202312": (3, "z")}
    plan = {a.partition: (a.action, a.reason) for a in plan_partitions("t", pg, ch, forced={"202404"})}
    assert plan == {
        "202312": ("drop", "absent_in_postgres"),
        "202401": ("skip", "in_sync"),
        "202402": ("load", "fingerprint_mismatch"),
        "202403": ("load", "missing_in_clickhouse"),
        "202404": ("load", "forced"),
    }


def test_same_count_different_rows_is_a_mismatch():
    # A lost row replaced by a duplicate keeps the count; the hash sum still differs.
    [action] = plan_partitions("t", {"p": (3, "100")}, {"p": (3, "101")})
    assert action == PartitionAction("t", "p", "load", "fingerprint_mismatch", (3, "100"), (3, "101"))


def test_batch_tokens_are_unique_per_run_partition_and_batch():
    tokens = {batch_token(run, "fact_deal", part, n) for run in ("r1", "r2") for part in ("1", "2") for n in range(3)}
    assert len(tokens) == 12


def test_clickhouse_datetime_literal_is_utc_microseconds():
    from datetime import datetime, timedelta, timezone

    from etl.clickhouse.sync import _ch_datetime64

    moscow = timezone(timedelta(hours=3))
    value = datetime(2026, 10, 26, 15, 0, 1, 123456, tzinfo=moscow)
    assert _ch_datetime64(value) == "toDateTime64('2026-10-26 12:00:01.123456', 6, 'UTC')"


def test_tolerance_comparison():
    assert within_tolerance(None, None, 0.005)
    assert not within_tolerance(1.0, None, 0.005)
    assert not within_tolerance(None, Decimal("1.00"), 0.005)
    assert within_tolerance(100.004999, Decimal("100.00"), NUMERIC_TOLERANCES["median_trip"])
    assert not within_tolerance(100.0051, Decimal("100.00"), NUMERIC_TOLERANCES["median_trip"])
    assert not within_tolerance(float("nan"), Decimal("1"), 1.0)


def test_postgres_rounding_is_half_away_from_zero_at_15_digits():
    assert postgres_round(100.015, 2) == Decimal("100.02")
    assert postgres_round(100.025, 2) == Decimal("100.03")
    assert postgres_round(-2.00005, 4) == Decimal("-2.0001")


def test_canonical_expressions_render_each_kind():
    assert Column("d", "date").pg_canonical() == "(d - DATE '1970-01-01')::text"
    assert Column("d", "date").ch_canonical() == "toString(toInt32(d))"
    assert Column("x", "decimal", 2, nullable=True).pg_canonical() == r"coalesce(round(x, 2)::text, '\N')"
    assert Column("x", "decimal", 2, nullable=True).ch_canonical() == r"ifNull(toDecimalString(x, 2), '\\N')"
    assert not Column("f", "float").fingerprinted
    with pytest.raises(ValueError):
        Column("x", "decimal")
    with pytest.raises(ValueError):
        Column("x", "money")


def test_partition_literals_are_validated_and_escaped():
    assert schema.FACT_DEAL.ch_partition_literal("202401") == "202401"
    with pytest.raises(ValueError):
        schema.FACT_DEAL.ch_partition_literal("2024; DROP TABLE x")
    assert schema.ML_PRICE_PREDICTIONS.ch_partition_literal("v'1\\") == "'v\\'1\\\\'"
    with pytest.raises(ValueError):
        schema.DIM_CITY.ch_partition_literal("all")


def test_every_synced_column_exists_in_the_ddl():
    ddl = (CLICKHOUSE_SQL_DIR / "001_schema.sql").read_text(encoding="utf-8")
    for spec in schema.SYNCED_TABLES:
        match = re.search(rf"CREATE TABLE IF NOT EXISTS {{database}}\.{spec.name}\s*\((.*?)\n\)", ddl, re.S)
        assert match, spec.name
        declared = {line.split()[0] for line in match.group(1).strip().splitlines() if line.strip()}
        assert set(spec.column_names) <= declared, spec.name
        assert (spec.name in schema.RUN_ID_TABLES) == ("_sync_run_id" in declared), spec.name


def test_ddl_rendering():
    statements = split_statements(render((CLICKHOUSE_SQL_DIR / "001_schema.sql").read_text(), "analytics"))
    assert statements[0] == "CREATE DATABASE IF NOT EXISTS analytics"
    assert all("{database}" not in s for s in statements)
    assert len(statements) == 11
    with pytest.raises(SettingsError):
        render("x", "bad-name; DROP")


def test_usage_errors_exit_2(monkeypatch):
    monkeypatch.setattr("etl.clickhouse.sync.configure_logging", lambda: None)
    assert main(["--months", "2024-13"]) == 2
    assert main(["--full", "--months", "2024-01"]) == 2
    assert main(["--unknown"]) == 2


def test_missing_clickhouse_settings_raise(monkeypatch):
    from etl.clickhouse.config import load_clickhouse_settings

    for name in ("CLICKHOUSE_HOST", "CLICKHOUSE_USER", "CLICKHOUSE_PASSWORD", "CLICKHOUSE_DB"):
        monkeypatch.delenv(name, raising=False)
    with pytest.raises(SettingsError):
        load_clickhouse_settings()
    monkeypatch.setenv("CLICKHOUSE_HOST", "h")
    monkeypatch.setenv("CLICKHOUSE_USER", "u")
    monkeypatch.setenv("CLICKHOUSE_PASSWORD", "p")
    monkeypatch.setenv("CLICKHOUSE_INSERT_BATCH_ROWS", "0")
    with pytest.raises(SettingsError):
        load_clickhouse_settings("db")


# ---- integration ---------------------------------------------------------------------------------


@pytest.fixture(scope="session")
def ch_settings():
    from etl.clickhouse.config import clickhouse_client, load_clickhouse_settings

    try:
        settings = load_clickhouse_settings(f"logistics_test_{uuid.uuid4().hex[:8]}")
        client = clickhouse_client(settings)
        client.command("SELECT 1")
    except Exception as exc:  # noqa: BLE001
        pytest.skip(f"ClickHouse unavailable: {type(exc).__name__}: {exc}")
    yield settings
    client.command(f"DROP DATABASE IF EXISTS {settings.database}")
    client.close()


@pytest.fixture(scope="module")
def ch(ch_settings):
    from etl.clickhouse.config import clickhouse_client

    client = clickhouse_client(ch_settings)
    yield client
    client.close()


@pytest.fixture(scope="module")
def synced(ch_settings, request):
    """A full sync into the scratch database. Returns the report. Skips before loading if ClickHouse is down."""
    from etl.clickhouse.sync import ClickHouseSync

    request.getfixturevalue("loaded_warehouse")
    report = ClickHouseSync(request.getfixturevalue("etl_config"), ch_settings).run()
    assert report.status == "ok", report.checks
    return report


def _sync(etl_config, ch_settings, **kwargs):
    from etl.clickhouse.sync import ClickHouseSync

    return ClickHouseSync(etl_config, ch_settings, **kwargs).run()


def _loads(report, table=None):
    return {(a["table"], a["partition"]): a["reason"] for a in report.actions
            if a["action"] == "load" and (table is None or a["table"] == table)}


@pytest.mark.integration
def test_full_sync_is_consistent(ch_settings, synced, etl_config):
    assert synced.checks["ok"]
    loaded = _loads(synced)
    assert loaded and set(loaded.values()) == {"missing_in_clickhouse"}
    corridor = next(r for r in synced.checks["results"] if r["name"] == "market_corridor_vs_postgres_mart")
    assert corridor["details"]["missing_in_clickhouse"] == corridor["details"]["extra_in_clickhouse"] == 0
    assert all(n == 0 for n in corridor["details"]["rounding_flips"].values())


@pytest.mark.integration
def test_resync_transfers_nothing(ch_settings, synced, etl_config, ch):
    report = _sync(etl_config, ch_settings)
    assert report.status == "ok"
    assert report.actions == []
    assert report.incremental_rows == 0
    assert report.derived == {"route_month_stats": [], "market_corridor_rebuilt": False, "ml_prediction_daily": []}
    final, raw = ch.query(f"SELECT (SELECT count() FROM {ch_settings.database}.ml_price_predictions FINAL), "
                          f"(SELECT count() FROM {ch_settings.database}.ml_price_predictions)").result_rows[0]
    assert raw >= final  # overlap re-reads are collapsed by FINAL until a merge removes them
    ch.command(f"OPTIMIZE TABLE {ch_settings.database}.ml_price_predictions FINAL")
    assert ch.query(f"SELECT count() FROM {ch_settings.database}.ml_price_predictions").result_rows[0][0] == final


@pytest.mark.integration
def test_late_prediction_is_recovered_by_the_overlap_window(ch_settings, synced, etl_config, ch):
    """A row committed late, with scored_at before the watermark, is missing in ClickHouse."""
    db = ch_settings.database
    ch.command(f"DELETE FROM {db}.ml_price_predictions WHERE price_id = "
               f"(SELECT min(price_id) FROM {db}.ml_price_predictions)")
    report = _sync(etl_config, ch_settings)
    assert report.status == "ok", report.checks
    assert report.incremental_rows > 0
    assert _loads(report, "ml_price_predictions") == {}
    assert report.derived["ml_prediction_daily"]


@pytest.mark.integration
def test_clickhouse_drift_is_detected_and_repaired(ch_settings, synced, etl_config, ch):
    db = ch_settings.database
    ch.command(f"DELETE FROM {db}.fact_deal WHERE toYYYYMM(loading_date) = 202405 "
               f"AND price_id IN (SELECT price_id FROM {db}.fact_deal WHERE toYYYYMM(loading_date) = 202405 "
               "ORDER BY price_id LIMIT 3)")
    ch.command(f"INSERT INTO {db}.fact_price_observation SELECT * FROM {db}.fact_price_observation "
               "WHERE toYYYYMM(price_month) = 202406 ORDER BY price_id LIMIT 5")
    ch.command(f"ALTER TABLE {db}.route_month_stats DROP PARTITION 202407")
    version = ch.query(f"SELECT any(model_version) FROM {db}.ml_price_predictions").result_rows[0][0]
    # A prediction deleted in PostgreSQL leaves no row for a watermark to find; only reconciliation sees it.
    ch.command(f"INSERT INTO {db}.ml_price_predictions SELECT * REPLACE (price_id + 1000000000000 AS price_id) "
               f"FROM {db}.ml_price_predictions FINAL ORDER BY price_id LIMIT 2")

    planned = _sync(etl_config, ch_settings, plan_only=True)
    assert planned.status == "planned"
    assert _loads(planned) == {("fact_deal", "202405"): "fingerprint_mismatch",
                               ("fact_price_observation", "202406"): "fingerprint_mismatch",
                               ("ml_price_predictions", version): "fingerprint_mismatch"}

    report = _sync(etl_config, ch_settings)
    assert report.status == "ok", report.checks
    assert set(_loads(report)) == set(_loads(planned))
    assert "202406" in report.derived["route_month_stats"] and "202407" in report.derived["route_month_stats"]
    assert report.derived["market_corridor_rebuilt"] is True


@pytest.mark.integration
def test_postgres_changes_reach_clickhouse(ch_settings, synced, etl_config):
    """Update, move, and delete in an uncommitted PostgreSQL transaction; sync reads that transaction."""
    from etl.clickhouse.config import postgres_connection

    with postgres_connection(etl_config, "test_clickhouse_sync", options="-c timezone=UTC") as pg:
        moved, from_month, to_month = pg.execute("""
            SELECT price_id, to_char(loading_date, 'YYYYMM'), to_char(loading_date + 40, 'YYYYMM')
            FROM core.fact_deal WHERE loading_date < DATE '2026-01-01'
            ORDER BY price_id LIMIT 1""").fetchone()
        repriced, price_month = pg.execute("""
            SELECT price_id, to_char(price_month, 'YYYYMM') FROM mart.market_corridor_observations(DATE '2026-10-26')
            ORDER BY price_id LIMIT 1""").fetchone()
        deleted, version = pg.execute(
            "SELECT price_id, model_version FROM ml.ml_price_predictions ORDER BY price_id LIMIT 1").fetchone()
        pg.execute("UPDATE core.fact_deal SET loading_date = loading_date + 40, "
                   "end_loading_date = end_loading_date + 40 WHERE price_id = %s", (moved,))
        pg.execute("UPDATE core.fact_deal SET price_rub = price_rub + 1000 WHERE price_id = %s", (repriced,))
        pg.execute("DELETE FROM ml.ml_price_predictions WHERE price_id = %s", (deleted,))
        refresh_id = pg.execute("INSERT INTO audit.mart_refresh (mart_name, as_of_date, status) "
                                "VALUES ('mart_market_corridor', DATE '2026-10-26', 'running') "
                                "RETURNING refresh_id").fetchone()[0]
        pg.execute("CALL mart.refresh_market_corridor(DATE '2026-10-26', %s)", (refresh_id,))

        report = _sync(etl_config, ch_settings, pg_connection=pg)
        assert report.status == "ok", report.checks
        loads = _loads(report)
        assert {("fact_deal", from_month), ("fact_deal", to_month),
                ("fact_price_observation", price_month), ("ml_price_predictions", version)} <= set(loads)
        assert price_month in report.derived["route_month_stats"]
        assert report.derived["market_corridor_rebuilt"] is True
        pg.rollback()

    restored = _sync(etl_config, ch_settings)
    assert restored.status == "ok", restored.checks
    assert {("fact_deal", from_month), ("fact_deal", to_month)} <= set(_loads(restored))


@pytest.mark.integration
def test_backfill_reloads_only_requested_months(ch_settings, synced, etl_config):
    report = _sync(etl_config, ch_settings, months={"202401", "202402"})
    assert report.status == "ok"
    assert _loads(report) == {(t, m): "forced" for t in ("fact_deal", "fact_price_observation")
                              for m in ("202401", "202402")}
    assert report.derived["route_month_stats"] == ["202401", "202402"]


@pytest.mark.integration
def test_unverified_stage_is_never_published(ch_settings, synced, etl_config, ch, monkeypatch):
    from etl.clickhouse.sync import ClickHouseSync

    db = ch_settings.database
    before = ch.query(f"SELECT count(), sum(cityHash64(price_id, price_rub)) FROM {db}.fact_deal "
                      "WHERE toYYYYMM(loading_date) = 202403").result_rows[0]
    original = ClickHouseSync._insert_stream

    def lossy(self, spec, target, batches, partition):
        def drop_last():
            for rows in batches:
                yield rows[:-1]
        return original(self, spec, target, drop_last(), partition)

    monkeypatch.setattr(ClickHouseSync, "_insert_stream", lossy)
    with pytest.raises(SyncError, match="staged"):
        ClickHouseSync(etl_config, ch_settings, months={"202403"}).run()
    after = ch.query(f"SELECT count(), sum(cityHash64(price_id, price_rub)) FROM {db}.fact_deal "
                     "WHERE toYYYYMM(loading_date) = 202403").result_rows[0]
    assert after == before
    failed = ch.query(f"SELECT count() FROM {db}.sync_log WHERE status = 'failed' AND table_name = 'fact_deal' "
                      "AND partition = '202403'").result_rows[0][0]
    assert failed == 1


@pytest.mark.integration
def test_concurrent_sync_is_refused(ch_settings, synced, etl_config):
    from etl.clickhouse.config import postgres_connection
    from etl.clickhouse.sync import SYNC_LOCK_KEY

    with postgres_connection(etl_config, "test_clickhouse_lock", autocommit=True) as holder:
        holder.execute("SELECT pg_advisory_lock(hashtext(%s))", (SYNC_LOCK_KEY,))
        with pytest.raises(SyncError, match="lock"):
            _sync(etl_config, ch_settings)
        holder.execute("SELECT pg_advisory_unlock(hashtext(%s))", (SYNC_LOCK_KEY,))
