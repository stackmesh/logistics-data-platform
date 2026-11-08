"""Incremental PostgreSQL -> ClickHouse sync. PostgreSQL is the source of truth; ClickHouse is rebuilt from it.

Every run reads one REPEATABLE READ snapshot of PostgreSQL and, per table and partition:

1. compares the PostgreSQL and ClickHouse fingerprints (row count + row hash sum, see schema.py);
2. reloads only partitions that differ, are missing, or are forced (--full, --months backfill);
   a partition is streamed in batches into a staging table, verified against PostgreSQL, swapped in with
   REPLACE PARTITION (atomic for readers), and verified again;
3. drops ClickHouse partitions that no longer exist in PostgreSQL.

Predictions are additionally pulled by an overlapping scored_at watermark into a ReplacingMergeTree and
then reconciled by the same fingerprints, so deletes and late commits are repaired, not lost.
Exit codes: 0 synced and consistent, 1 failure or inconsistency, 2 usage error.
"""

from __future__ import annotations

import argparse
import logging
import re
import sys
import time
import uuid
from dataclasses import asdict, dataclass, field
from datetime import date, datetime, timedelta, timezone

from psycopg import IsolationLevel

from etl.clickhouse import derived
from etl.clickhouse.check import run_checks
from etl.clickhouse.config import (
    ClickHouseSettings,
    clickhouse_client,
    load_clickhouse_settings,
    postgres_connection,
)
from etl.clickhouse.migrate import apply_schema
from etl.clickhouse.schema import (
    FACT_PRICE_OBSERVATION,
    ML_PRICE_PREDICTIONS,
    OBSERVATION_TEMP_SQL,
    OBSERVATION_TEMP_TABLE,
    RUN_ID_TABLES,
    SYNCED_TABLES,
    TableSpec,
)
from etl.config import load_config
from etl.log import configure_logging, log_event
from logistics.settings import SettingsError

logger = logging.getLogger("etl.clickhouse.sync")
SYNC_LOCK_KEY = "logistics_dwh.clickhouse_sync"
MONTHLY_TABLES = {"fact_deal", "fact_price_observation"}
INSERT_ATTEMPTS = 3
STAGE_DEDUP_WINDOW = 10_000
_MONTH = re.compile(r"^(\d{4})-(\d{2})$")


class SyncError(RuntimeError):
    """A load could not be verified. ClickHouse keeps the previous verified partition."""


Fingerprint = tuple[int, str]


@dataclass(frozen=True)
class PartitionAction:
    table: str
    partition: str
    action: str  # load, drop, skip
    reason: str
    pg: Fingerprint | None
    ch: Fingerprint | None


@dataclass
class SyncReport:
    run_id: str
    as_of_date: str
    mode: str
    actions: list[dict] = field(default_factory=list)
    derived: dict = field(default_factory=dict)
    incremental_rows: int = 0
    watermark: str | None = None
    checks: dict | None = None
    status: str = "running"


def parse_month_range(text: str) -> set[str]:
    """'2024-01..2024-06' or '2024-03' to month partitions {'202401', ...}."""
    bounds = [part.strip() for part in text.split("..")]
    if len(bounds) == 1:
        bounds = bounds * 2
    if len(bounds) != 2:
        raise ValueError(f"invalid month range {text!r}")
    months = []
    for bound in bounds:
        match = _MONTH.match(bound)
        if not match or not 1 <= int(match.group(2)) <= 12:
            raise ValueError(f"invalid month {bound!r}, expected YYYY-MM")
        months.append(int(match.group(1)) * 12 + int(match.group(2)) - 1)
    start, end = months
    if start > end:
        raise ValueError(f"month range {text!r} is reversed")
    return {f"{m // 12:04d}{m % 12 + 1:02d}" for m in range(start, end + 1)}


def plan_partitions(
    table: str,
    pg: dict[str, Fingerprint],
    ch: dict[str, Fingerprint],
    forced: set[str] | None = None,
) -> list[PartitionAction]:
    """Diff of fingerprints. forced=None forces nothing; a forced partition absent in PostgreSQL is dropped."""
    forced = forced or set()
    actions = []
    for partition in sorted(set(pg) | set(ch)):
        source, target = pg.get(partition), ch.get(partition)
        if source is None:
            actions.append(PartitionAction(table, partition, "drop", "absent_in_postgres", None, target))
        elif target is None:
            actions.append(PartitionAction(table, partition, "load", "missing_in_clickhouse", source, None))
        elif source != target:
            actions.append(PartitionAction(table, partition, "load", "fingerprint_mismatch", source, target))
        elif partition in forced:
            actions.append(PartitionAction(table, partition, "load", "forced", source, target))
        else:
            actions.append(PartitionAction(table, partition, "skip", "in_sync", source, target))
    return actions


def _ch_datetime64(value: datetime) -> str:
    utc = value.astimezone(timezone.utc)
    return f"toDateTime64('{utc.strftime('%Y-%m-%d %H:%M:%S.%f')}', 6, 'UTC')"


def batch_token(run_id: str, table: str, partition: str, batch_no: int) -> str:
    return f"{run_id}:{table}:{partition}:{batch_no}"


class ClickHouseSync:
    def __init__(self, config, settings: ClickHouseSettings, *, full: bool = False,
                 months: set[str] | None = None, plan_only: bool = False, run_checks_after: bool = True,
                 pg_connection=None):
        """pg_connection: an open transaction to read instead of a new REPEATABLE READ snapshot (tests)."""
        self.config = config
        self.settings = settings
        self.db = settings.database
        self.full = full
        self.months = months or set()
        self.plan_only = plan_only
        self.run_checks_after = run_checks_after
        self.run_id = str(uuid.uuid4())
        self.started_at = datetime.now(timezone.utc)
        self._t0 = time.monotonic()
        self.ch = clickhouse_client(settings)
        self.pg = pg_connection
        self._external_pg = pg_connection is not None
        self.report: SyncReport | None = None

    # ---- PostgreSQL side -------------------------------------------------------------------------

    def _open_snapshot(self) -> date:
        if not self._external_pg:
            self.pg = postgres_connection(self.config, "logistics_clickhouse_sync", autocommit=True,
                                          options="-c timezone=UTC")
        locked = self.pg.execute("SELECT pg_try_advisory_lock(hashtext(%s))", (SYNC_LOCK_KEY,)).fetchone()[0]
        if not locked:
            raise SyncError("another ClickHouse sync holds the lock")
        if not self._external_pg:
            self.pg.autocommit = False
            self.pg.isolation_level = IsolationLevel.REPEATABLE_READ
        row = self.pg.execute("SELECT max(as_of_date) FROM mart.mart_market_corridor").fetchone()
        as_of = row[0] or self.config.as_of_date
        self.pg.execute(OBSERVATION_TEMP_SQL, {"as_of": as_of})
        self.pg.execute(f"ANALYZE {OBSERVATION_TEMP_TABLE}")
        return as_of

    def _close_snapshot(self) -> None:
        if self.pg is None:
            return
        if self._external_pg:
            self.pg.execute(f"DROP TABLE IF EXISTS {OBSERVATION_TEMP_TABLE}")
            self.pg.execute("SELECT pg_advisory_unlock(hashtext(%s))", (SYNC_LOCK_KEY,))
            return
        try:
            self.pg.rollback()
            self.pg.autocommit = True
            self.pg.execute("SELECT pg_advisory_unlock(hashtext(%s))", (SYNC_LOCK_KEY,))
        finally:
            self.pg.close()
            self.pg = None

    def pg_fingerprints(self, spec: TableSpec, partition: str | None = None) -> dict[str, Fingerprint]:
        if partition is None:
            rows = self.pg.execute(spec.pg_fingerprint_sql()).fetchall()
        else:
            rows = self.pg.execute(spec.pg_fingerprint_sql(spec.pg_partition_filter()),
                                   {"partition": partition}).fetchall()
        return {part: (int(count), digest) for part, count, digest in rows}

    def _stream(self, spec: TableSpec, where: str | None, params: dict, order_by: str):
        condition = f"WHERE {where}" if where else ""
        sql = f"SELECT {spec.pg_select_list()} FROM {spec.pg_source} {condition} ORDER BY {order_by}"
        with self.pg.cursor(name=f"ch_sync_{spec.name}") as cursor:
            cursor.itersize = self.settings.insert_batch_rows
            cursor.execute(sql, params)
            while True:
                rows = cursor.fetchmany(self.settings.insert_batch_rows)
                if not rows:
                    return
                yield rows

    # ---- ClickHouse side -------------------------------------------------------------------------

    def ch_fingerprints(self, spec: TableSpec, table: str | None = None,
                        partition: str | None = None) -> dict[str, Fingerprint]:
        where = f"{spec.ch_partition} = {spec.ch_partition_literal(partition)}" if partition else None
        return self.ch_fingerprints_where(spec, where, table)

    def ch_fingerprints_where(self, spec: TableSpec, where: str | None,
                              table: str | None = None) -> dict[str, Fingerprint]:
        rows = self.ch.query(spec.ch_fingerprint_sql(self.db, table, where)).result_rows
        return {part: (int(count), digest) for part, count, digest in rows}

    def _insert(self, table: str, rows: list, columns: list[str], token: str) -> None:
        for attempt in range(1, INSERT_ATTEMPTS + 1):
            try:
                self.ch.insert(f"{self.db}.{table}", rows, column_names=columns,
                               settings={"insert_deduplication_token": token})
                return
            except Exception as exc:
                # A retried block carries the same token, so a block stored before the failure is not
                # stored again (non_replicated_deduplication_window on the target).
                if attempt == INSERT_ATTEMPTS:
                    raise
                log_event(logger, "clickhouse_insert_retry", logging.WARNING, table=table, token=token,
                          attempt=attempt, error=f"{type(exc).__name__}: {exc}")
                time.sleep(2 ** attempt)

    def _insert_stream(self, spec: TableSpec, target: str, batches, partition: str) -> int:
        columns = spec.column_names + (["_sync_run_id"] if spec.name in RUN_ID_TABLES else [])
        run_uuid = uuid.UUID(self.run_id)
        total = 0
        for batch_no, rows in enumerate(batches):
            if spec.name in RUN_ID_TABLES:
                rows = [(*row, run_uuid) for row in rows]
            self._insert(target, rows, columns, batch_token(self.run_id, spec.name, partition, batch_no))
            total += len(rows)
        return total

    def _prepare_stage(self, table: str) -> str:
        stage = f"{table}_stage"
        self.ch.command(f"CREATE TABLE IF NOT EXISTS {self.db}.{stage} AS {self.db}.{table}")
        self.ch.command(f"ALTER TABLE {self.db}.{stage} MODIFY SETTING "
                        f"non_replicated_deduplication_window = {STAGE_DEDUP_WINDOW}")
        self.ch.command(f"TRUNCATE TABLE {self.db}.{stage}")
        return stage

    def _log(self, table: str, partition: str, action: str, status: str, started: float,
             pg: Fingerprint | None = None, ch: Fingerprint | None = None, detail: str = "") -> None:
        row = [uuid.UUID(self.run_id), self.started_at, table, partition, action,
               pg[0] if pg else None, ch[0] if ch else None, pg[1] if pg else None, ch[1] if ch else None,
               round(time.monotonic() - started, 3), status, detail]
        self.ch.insert(f"{self.db}.sync_log", [row], column_names=[
            "run_id", "started_at", "table_name", "partition", "action", "pg_rows", "ch_rows",
            "pg_hash", "ch_hash", "duration_s", "status", "detail"])

    # ---- base table loads ------------------------------------------------------------------------

    def load_partition(self, spec: TableSpec, action: PartitionAction) -> None:
        started = time.monotonic()
        stage = self._prepare_stage(spec.name)
        if spec.load_mode == "exchange":
            batches = self._stream(spec, None, {}, spec.pg_order)
        else:
            batches = self._stream(spec, spec.pg_partition_filter(), {"partition": action.partition},
                                   spec.pg_order)
        rows = self._insert_stream(spec, stage, batches, action.partition)
        expected = action.pg
        staged = self.ch_fingerprints(spec, stage).get(action.partition)
        if staged != expected:
            self._log(spec.name, action.partition, "load", "failed", started, expected, staged, "stage mismatch")
            raise SyncError(f"{spec.name}/{action.partition}: staged {staged} != postgres {expected}")
        if spec.load_mode == "exchange":
            self.ch.command(f"EXCHANGE TABLES {self.db}.{stage} AND {self.db}.{spec.name}")
            loaded = self.ch_fingerprints(spec).get(action.partition)
        else:
            literal = spec.ch_partition_literal(action.partition)
            self.ch.command(f"ALTER TABLE {self.db}.{spec.name} REPLACE PARTITION {literal} FROM {self.db}.{stage}")
            loaded = self.ch_fingerprints(spec, partition=action.partition).get(action.partition)
        self.ch.command(f"TRUNCATE TABLE {self.db}.{stage}")
        if loaded != expected:
            self._log(spec.name, action.partition, "load", "failed", started, expected, loaded, "target mismatch")
            raise SyncError(f"{spec.name}/{action.partition}: loaded {loaded} != postgres {expected}")
        self._log(spec.name, action.partition, "load", "ok", started, expected, loaded,
                  f"{action.reason}; rows={rows}")
        log_event(logger, "clickhouse_partition_loaded", table=spec.name, partition=action.partition,
                  reason=action.reason, rows=rows, seconds=round(time.monotonic() - started, 3))

    def drop_partition(self, spec: TableSpec, action: PartitionAction) -> None:
        started = time.monotonic()
        if spec.load_mode == "exchange":
            self.ch.command(f"TRUNCATE TABLE {self.db}.{spec.name}")
        else:
            literal = spec.ch_partition_literal(action.partition)
            self.ch.command(f"ALTER TABLE {self.db}.{spec.name} DROP PARTITION {literal}")
        remaining = self.ch_fingerprints(spec, partition=None if spec.load_mode == "exchange"
                                         else action.partition)
        if remaining.get(action.partition):
            raise SyncError(f"{spec.name}/{action.partition}: rows remain after drop")
        self._log(spec.name, action.partition, "drop", "ok", started, None, action.ch, action.reason)
        log_event(logger, "clickhouse_partition_dropped", table=spec.name, partition=action.partition)

    def forced_partitions(self, spec: TableSpec, pg: dict[str, Fingerprint]) -> set[str]:
        if self.full:
            return set(pg)
        if self.months and spec.name in MONTHLY_TABLES:
            return self.months
        return set()

    def plan_table(self, spec: TableSpec) -> list[PartitionAction]:
        pg = self.pg_fingerprints(spec)
        ch = self.ch_fingerprints(spec)
        forced = self.forced_partitions(spec, pg)
        empty = sorted(forced - set(pg) - set(ch))
        if empty:
            log_event(logger, "clickhouse_backfill_month_empty", table=spec.name, partitions=empty)
        return plan_partitions(spec.name, pg, ch, forced)

    # ---- predictions: incremental watermark ------------------------------------------------------

    def read_watermark(self) -> datetime | None:
        rows = self.ch.query(f"SELECT watermark FROM {self.db}.sync_watermark FINAL WHERE source = %(s)s",
                             parameters={"s": ML_PRICE_PREDICTIONS.name}).result_rows
        if not rows:
            return None
        value = rows[0][0]
        return value.replace(tzinfo=timezone.utc) if value.tzinfo is None else value.astimezone(timezone.utc)

    def incremental_predictions(self) -> tuple[int, set[str], datetime | None]:
        """Rows scored after (watermark - overlap). Returns rows, touched partitions, the new watermark."""
        spec = ML_PRICE_PREDICTIONS
        watermark = self.read_watermark()
        source_max = self.pg.execute(f"SELECT max(scored_at) FROM {spec.pg_source}").fetchone()[0]
        if watermark is None or self.full:
            return 0, set(), source_max
        overlap_start = watermark - timedelta(seconds=self.settings.watermark_overlap_s)
        # Re-read the overlap window only when it differs (a late commit with an older scored_at, a delete);
        # otherwise read strictly after the watermark, so repeated runs insert nothing.
        window_pg = {p: (int(n), h) for p, n, h in self.pg.execute(
            spec.pg_fingerprint_sql("scored_at > %(start)s AND scored_at <= %(end)s"),
            {"start": overlap_start, "end": watermark}).fetchall()}
        window_ch = self.ch_fingerprints_where(
            spec, f"scored_at > {_ch_datetime64(overlap_start)} AND scored_at <= {_ch_datetime64(watermark)}")
        since = watermark if window_pg == window_ch else overlap_start
        touched = {row[0] for row in self.pg.execute(
            f"SELECT DISTINCT model_version FROM {spec.pg_source} WHERE scored_at > %(since)s", {"since": since})}
        batches = self._stream(spec, "scored_at > %(since)s", {"since": since}, "scored_at, price_id, model_name")
        rows = self._insert_stream(spec, spec.name, batches, f"incremental:{since.isoformat()}")
        log_event(logger, "clickhouse_predictions_incremental", since=since, overlap_reread=since != watermark,
                  rows=rows, partitions=sorted(touched))
        return rows, touched, max(filter(None, [source_max, watermark]), default=None)

    def write_watermark(self, value: datetime | None) -> None:
        if value is None:
            return
        self.ch.insert(f"{self.db}.sync_watermark", [[ML_PRICE_PREDICTIONS.name, value, uuid.UUID(self.run_id),
                                                      datetime.now(timezone.utc)]],
                       column_names=["source", "watermark", "run_id", "updated_at"])

    # ---- derived tables --------------------------------------------------------------------------

    def rebuild_route_month_stats(self, changed: set[str]) -> list[str]:
        """Rebuild changed observation months, and any month whose observation count disagrees."""
        obs = dict(self.ch.query(f"SELECT toString(toYYYYMM(price_month)), count() FROM "
                                 f"{self.db}.fact_price_observation GROUP BY 1").result_rows)
        stats = dict(self.ch.query(f"SELECT toString(toYYYYMM(price_month)), sum(observations) FROM "
                                   f"{self.db}.route_month_stats GROUP BY 1").result_rows)
        targets = {p for p in set(obs) | set(stats) if obs.get(p) != stats.get(p)} | (changed & set(obs))
        for partition in sorted(targets):
            if partition not in obs:
                self.ch.command(f"ALTER TABLE {self.db}.route_month_stats DROP PARTITION {int(partition)}")
                continue
            sql = derived.route_month_stats_insert(self.db, "route_month_stats_stage", partition)
            stage = self._prepare_stage("route_month_stats")
            self.ch.command(sql)
            self.ch.command(f"ALTER TABLE {self.db}.route_month_stats REPLACE PARTITION {int(partition)} "
                            f"FROM {self.db}.{stage}")
            self.ch.command(f"TRUNCATE TABLE {self.db}.{stage}")
        return sorted(targets)

    def rebuild_market_corridor(self, as_of: date, force: bool) -> bool:
        cells = self.ch.query(f"SELECT count() FROM ({derived.corridor_cells_sql(self.db)})").result_rows[0][0]
        current = self.ch.query(f"SELECT count(), min(as_of_date), max(as_of_date) FROM {self.db}.market_corridor"
                                ).result_rows[0]
        if not force and current[0] == cells and current[1] == as_of and current[2] == as_of:
            return False
        stage = self._prepare_stage("market_corridor")
        self.ch.command(derived.market_corridor_insert(self.db, stage), parameters={"as_of": as_of})
        built = self.ch.query(f"SELECT count() FROM {self.db}.{stage}").result_rows[0][0]
        if built != cells:
            raise SyncError(f"market_corridor: built {built} rows for {cells} cells (missing dimension rows?)")
        self.ch.command(f"EXCHANGE TABLES {self.db}.{stage} AND {self.db}.market_corridor")
        self.ch.command(f"TRUNCATE TABLE {self.db}.{stage}")
        return True

    def rebuild_prediction_daily(self, changed: set[str]) -> list[str]:
        spec = ML_PRICE_PREDICTIONS
        preds = dict(self.ch.query(f"SELECT model_version, count() FROM {self.db}.ml_price_predictions FINAL "
                                   "GROUP BY model_version").result_rows)
        daily = dict(self.ch.query(f"SELECT model_version, sum(deals) FROM {self.db}.ml_prediction_daily "
                                   "GROUP BY model_version").result_rows)
        targets = {p for p in set(preds) | set(daily) if preds.get(p) != daily.get(p)} | (changed & set(preds))
        for partition in sorted(targets):
            literal = spec.ch_partition_literal(partition)
            if partition not in preds:
                self.ch.command(f"ALTER TABLE {self.db}.ml_prediction_daily DROP PARTITION {literal}")
                continue
            stage = self._prepare_stage("ml_prediction_daily")
            self.ch.command(derived.prediction_daily_insert(self.db, stage, literal))
            self.ch.command(f"ALTER TABLE {self.db}.ml_prediction_daily REPLACE PARTITION {literal} "
                            f"FROM {self.db}.{stage}")
            self.ch.command(f"TRUNCATE TABLE {self.db}.{stage}")
        return sorted(targets)

    # ---- orchestration ---------------------------------------------------------------------------

    def run(self) -> SyncReport:
        mode = "full" if self.full else ("backfill" if self.months else "incremental")
        apply_schema(self.ch, self.db)
        try:
            as_of = self._open_snapshot()
            self.report = SyncReport(run_id=self.run_id, as_of_date=as_of.isoformat(),
                                     mode=("plan:" + mode) if self.plan_only else mode)
            log_event(logger, "clickhouse_sync_started", run_id=self.run_id, mode=self.report.mode,
                      as_of_date=as_of, database=self.db)
            changed: dict[str, set[str]] = {}
            new_watermark = None
            for spec in SYNCED_TABLES:
                touched: set[str] = set()
                if spec is ML_PRICE_PREDICTIONS and not self.plan_only:
                    rows, touched, new_watermark = self.incremental_predictions()
                    self.report.incremental_rows = rows
                actions = self.plan_table(spec)
                self.report.actions += [asdict(a) for a in actions if a.action != "skip"]
                if self.plan_only:
                    continue
                for action in actions:
                    if action.action == "load":
                        self.load_partition(spec, action)
                    elif action.action == "drop":
                        self.drop_partition(spec, action)
                changed[spec.name] = touched | {a.partition for a in actions if a.action != "skip"}
            if self.plan_only:
                self.report.status = "planned"
                return self.report

            stats = self.rebuild_route_month_stats(changed[FACT_PRICE_OBSERVATION.name])
            corridor = self.rebuild_market_corridor(
                as_of, force=bool(stats or changed["dim_city"] or changed["dim_body_group"] or self.full))
            daily = self.rebuild_prediction_daily(changed[ML_PRICE_PREDICTIONS.name])
            self.report.derived = {"route_month_stats": stats, "market_corridor_rebuilt": corridor,
                                   "ml_prediction_daily": daily}
            self.write_watermark(new_watermark)
            self.report.watermark = new_watermark.isoformat() if new_watermark else None

            if self.run_checks_after:
                result = run_checks(self.pg, self.ch, self.db, as_of)
                self.report.checks = result.as_dict()
                self.report.status = "ok" if result.ok else "inconsistent"
            else:
                self.report.status = "ok"
            self._log("_run", "", "summary", self.report.status, self._t0,
                      detail=f"mode={self.report.mode} actions={len(self.report.actions)}")
            return self.report
        except Exception:
            if self.report is not None:
                self.report.status = "failed"
            raise
        finally:
            self._close_snapshot()


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="python -m etl.clickhouse.sync", description=__doc__.split("\n")[0])
    group = parser.add_mutually_exclusive_group()
    group.add_argument("--full", action="store_true", help="reload every partition of every table")
    group.add_argument("--months", metavar="YYYY-MM[..YYYY-MM]",
                       help="backfill: reload these months of fact_deal and fact_price_observation")
    parser.add_argument("--plan", action="store_true", help="print the partition plan and change no data")
    parser.add_argument("--skip-check", action="store_true", help="skip the consistency checks after loading")
    parser.add_argument("--database", help="ClickHouse database (default CLICKHOUSE_DB)")
    return parser


def main(argv: list[str] | None = None) -> int:
    configure_logging()
    parser = build_parser()
    try:
        args = parser.parse_args(argv)
        months = parse_month_range(args.months) if args.months else None
    except SystemExit as exc:
        return 2 if exc.code else 0
    except ValueError as exc:
        log_event(logger, "clickhouse_sync_usage_error", logging.ERROR, error=str(exc))
        return 2
    try:
        sync = ClickHouseSync(load_config(), load_clickhouse_settings(args.database), full=args.full,
                              months=months, plan_only=args.plan, run_checks_after=not args.skip_check)
        report = sync.run()
    except SettingsError as exc:
        log_event(logger, "clickhouse_sync_config_error", logging.ERROR, error=str(exc))
        return 2
    except Exception as exc:
        log_event(logger, "clickhouse_sync_failed", logging.ERROR, error=f"{type(exc).__name__}: {exc}")
        return 1
    log_event(logger, "clickhouse_sync_finished", **asdict(report))
    return 0 if report.status in {"ok", "planned"} else 1


if __name__ == "__main__":
    sys.exit(main())
