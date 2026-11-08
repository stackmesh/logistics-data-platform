"""Consistency of the ClickHouse copy against PostgreSQL, the reference result.

Base tables must match exactly (partition fingerprints). Derived analytics are compared with PostgreSQL's
own outputs: mart.mart_market_corridor and ml.v_prediction_error_summary. Counts, dates, categories, and
NULL-ness must be equal; numeric values must agree within NUMERIC_TOLERANCES.

Why tolerances at all: PostgreSQL stores corridor values rounded (numeric(18,2) trips, numeric(18,4) per km,
numeric(12,6) cv, numeric(12,4) percent changes) and computes means and standard deviations in numeric.
ClickHouse keeps unrounded Float64. The percentile inputs are bit-identical doubles in both systems, so the
remaining difference is at most half a unit of the PostgreSQL rounding plus float rounding error, which is
bounded here by RELATIVE_TOLERANCE of the value.
Exit codes: 0 consistent, 1 inconsistent or failed, 2 configuration error.
"""

from __future__ import annotations

import json
import logging
import math
import sys
from dataclasses import asdict, dataclass, field
from datetime import date
from decimal import ROUND_HALF_UP, Decimal

from psycopg import IsolationLevel

from etl.clickhouse.config import clickhouse_client, load_clickhouse_settings, postgres_connection
from etl.clickhouse.schema import OBSERVATION_TEMP_SQL, OBSERVATION_TEMP_TABLE, SYNCED_TABLES
from etl.config import load_config
from etl.log import configure_logging, log_event
from logistics.settings import SettingsError

logger = logging.getLogger("etl.clickhouse.check")

RELATIVE_TOLERANCE = 1e-9
# Absolute tolerance per field: half of the last stored decimal place of the PostgreSQL column.
NUMERIC_TOLERANCES = {
    "p25_trip": 0.005, "median_trip": 0.005, "p75_trip": 0.005, "avg_trip": 0.005,
    "p25_km": 0.00005, "median_km": 0.00005, "p75_km": 0.00005,
    "cv_rate": 0.0000005,
    "mom_pct": 0.00005, "yoy_pct": 0.00005,
    "mean_actual_rub": 0.005, "mean_predicted_rub": 0.005, "mae_rub": 0.005, "rmse_rub": 0.005,
    "bias_rub": 0.005, "mape_pct": 0.00005,
}
DECIMALS = {"p25_trip": 2, "median_trip": 2, "p75_trip": 2, "avg_trip": 2, "p25_km": 4, "median_km": 4,
            "p75_km": 4, "cv_rate": 6, "mom_pct": 4, "yoy_pct": 4}

CORRIDOR_KEYS = ("price_month", "loading_city_gcid", "unloading_city_gcid", "body_group_code", "weight_category")
CORRIDOR_EXACT = ("origin_city_name", "destination_city_name", "body_group_name", "trips_count", "carriers_count",
                  "max_price_date", "freshness_days", "reliability_level", "as_of_date")
CORRIDOR_NUMERIC = ("p25_trip", "median_trip", "p75_trip", "avg_trip", "p25_km", "median_km", "p75_km",
                    "cv_rate", "mom_pct", "yoy_pct")
PREDICTION_NUMERIC = ("mean_actual_rub", "mean_predicted_rub", "mae_rub", "rmse_rub", "mape_pct", "bias_rub")


@dataclass
class CheckResult:
    name: str
    ok: bool
    details: dict = field(default_factory=dict)


@dataclass
class CheckReport:
    results: list[CheckResult] = field(default_factory=list)

    @property
    def ok(self) -> bool:
        return all(result.ok for result in self.results)

    def as_dict(self) -> dict:
        return {"ok": self.ok, "results": [asdict(result) for result in self.results]}


def within_tolerance(actual: float | None, reference: float | Decimal | None, absolute: float) -> bool:
    """NULL only matches NULL. Otherwise |actual - reference| <= absolute + RELATIVE_TOLERANCE * |reference|."""
    if actual is None or reference is None:
        return actual is None and reference is None
    actual, reference = float(actual), float(reference)
    if math.isnan(actual) or math.isnan(reference):
        return False
    return abs(actual - reference) <= absolute + RELATIVE_TOLERANCE * abs(reference)


def postgres_round(value: float, decimals: int) -> Decimal:
    """float8 -> numeric(p, decimals) as PostgreSQL does it: 15 significant digits, then half away from zero."""
    return Decimal(format(value, ".15g")).quantize(Decimal(1).scaleb(-decimals), rounding=ROUND_HALF_UP)


def compare_rows(ch_rows: dict, pg_rows: dict, exact: tuple[str, ...], numeric: tuple[str, ...],
                 sample: int = 5) -> dict:
    """Row-by-row comparison keyed by the grain. Returns counts, worst differences, and sample violations."""
    missing = sorted(set(pg_rows) - set(ch_rows))
    extra = sorted(set(ch_rows) - set(pg_rows))
    exact_violations: dict[str, int] = {name: 0 for name in exact}
    numeric_violations: dict[str, int] = {name: 0 for name in numeric}
    max_abs_diff: dict[str, float] = {name: 0.0 for name in numeric}
    rounding_flips: dict[str, int] = {name: 0 for name in numeric if name in DECIMALS}
    samples = []
    for key in sorted(set(pg_rows) & set(ch_rows)):
        ch, pg = ch_rows[key], pg_rows[key]
        for name in exact:
            if ch[name] != pg[name]:
                exact_violations[name] += 1
                if len(samples) < sample:
                    samples.append({"key": [str(k) for k in key], "field": name, "clickhouse": str(ch[name]),
                                    "postgres": str(pg[name])})
        for name in numeric:
            a, b = ch[name], pg[name]
            if not within_tolerance(a, b, NUMERIC_TOLERANCES[name]):
                numeric_violations[name] += 1
                if len(samples) < sample:
                    samples.append({"key": [str(k) for k in key], "field": name, "clickhouse": a,
                                    "postgres": None if b is None else float(b)})
            elif a is not None:
                max_abs_diff[name] = max(max_abs_diff[name], abs(float(a) - float(b)))
                if name in rounding_flips and postgres_round(a, DECIMALS[name]) != Decimal(b):
                    rounding_flips[name] += 1
    ok = not missing and not extra and not any(exact_violations.values()) and not any(numeric_violations.values())
    return {
        "ok": ok,
        "postgres_rows": len(pg_rows),
        "clickhouse_rows": len(ch_rows),
        "missing_in_clickhouse": len(missing),
        "extra_in_clickhouse": len(extra),
        "exact_violations": exact_violations,
        "numeric_violations": numeric_violations,
        "max_abs_diff": max_abs_diff,
        "rounding_flips": rounding_flips,
        "samples": samples + [{"missing": [str(k) for k in key]} for key in missing[:sample]]
        + [{"extra": [str(k) for k in key]} for key in extra[:sample]],
    }


def check_fingerprints(pg, ch, database: str) -> CheckResult:
    tables = {}
    for spec in SYNCED_TABLES:
        source = {p: (int(n), h) for p, n, h in pg.execute(spec.pg_fingerprint_sql()).fetchall()}
        target = {p: (int(n), h) for p, n, h in ch.query(spec.ch_fingerprint_sql(database)).result_rows}
        mismatched = sorted(p for p in set(source) | set(target) if source.get(p) != target.get(p))
        tables[spec.name] = {
            "partitions": len(source),
            "postgres_rows": sum(n for n, _ in source.values()),
            "clickhouse_rows": sum(n for n, _ in target.values()),
            "mismatched_partitions": mismatched[:20],
        }
    return CheckResult("base_table_fingerprints", all(not t["mismatched_partitions"] for t in tables.values()),
                       tables)


def check_route_month_stats(ch, database: str) -> CheckResult:
    rows = ch.query(f"""
        SELECT o.part, o.n, s.n
        FROM (SELECT toYYYYMM(price_month) AS part, count() AS n FROM {database}.fact_price_observation
              GROUP BY part) AS o
        FULL OUTER JOIN (SELECT toYYYYMM(price_month) AS part, sum(observations) AS n
                         FROM {database}.route_month_stats GROUP BY part) AS s USING (part)
        SETTINGS join_use_nulls = 1""").result_rows
    bad = [{"partition": str(p), "observations": a, "stats": b} for p, a, b in rows if a != b]
    return CheckResult("route_month_stats_vs_observations", not bad, {"partitions": len(rows), "mismatched": bad})


def check_market_corridor(pg, ch, database: str) -> CheckResult:
    columns = CORRIDOR_KEYS + CORRIDOR_EXACT + CORRIDOR_NUMERIC
    pg_rows = {}
    for row in pg.execute(f"SELECT {', '.join(columns)} FROM mart.mart_market_corridor").fetchall():
        record = dict(zip(columns, row, strict=True))
        pg_rows[tuple(record[k] for k in CORRIDOR_KEYS)] = record
    ch_rows = {}
    for row in ch.query(f"SELECT {', '.join(columns)} FROM {database}.market_corridor").result_rows:
        record = dict(zip(columns, row, strict=True))
        ch_rows[tuple(record[k] for k in CORRIDOR_KEYS)] = record
    details = compare_rows(ch_rows, pg_rows, CORRIDOR_EXACT, CORRIDOR_NUMERIC)
    details["tolerances"] = {name: NUMERIC_TOLERANCES[name] for name in CORRIDOR_NUMERIC}
    return CheckResult("market_corridor_vs_postgres_mart", details.pop("ok"), details)


def check_prediction_summary(pg, ch, database: str) -> CheckResult:
    keys = ("model_name", "model_version", "record_type")
    pg_rows = {}
    for row in pg.execute("""
            SELECT model_name, model_version, record_type, deals, first_loading_date, last_loading_date,
                   mean_actual_rub, mean_predicted_rub, mae_rub, rmse_rub, mape_pct, bias_rub
            FROM ml.v_prediction_error_summary""").fetchall():
        names = keys + ("deals", "first_loading_date", "last_loading_date") + PREDICTION_NUMERIC
        record = dict(zip(names, row, strict=True))
        pg_rows[tuple(record[k] for k in keys)] = record
    ch_rows = {}
    for row in ch.query(f"""
            SELECT model_name, model_version, record_type, sum(deals), min(loading_date), max(loading_date),
                   sum(actual_deals), sum(actual_sum), sum(predicted_sum), sum(abs_error_sum), sum(sq_error_sum),
                   sum(ape_sum), sum(signed_error_sum)
            FROM {database}.ml_prediction_daily
            GROUP BY model_name, model_version, record_type""").result_rows:
        name, version, record_type, deals, first, last, actual_n, actual, predicted, abs_e, sq_e, ape, signed = row

        def mean(total, n):
            return None if not n else float(total) / n

        ch_rows[(name, version, record_type)] = {
            "deals": deals, "first_loading_date": first, "last_loading_date": last,
            "mean_actual_rub": mean(actual, actual_n), "mean_predicted_rub": mean(predicted, deals),
            "mae_rub": mean(abs_e, actual_n),
            "rmse_rub": None if not actual_n else math.sqrt(float(sq_e) / actual_n),
            "mape_pct": mean(ape, actual_n), "bias_rub": mean(signed, actual_n),
        }
    details = compare_rows(ch_rows, pg_rows, ("deals", "first_loading_date", "last_loading_date"),
                           PREDICTION_NUMERIC)
    details["tolerances"] = {name: NUMERIC_TOLERANCES[name] for name in PREDICTION_NUMERIC}
    return CheckResult("prediction_error_summary_vs_postgres_view", details.pop("ok"), details)


def run_checks(pg, ch, database: str, as_of: date) -> CheckReport:
    """pg must be inside the snapshot that holds the observation temp table for as_of."""
    report = CheckReport()
    report.results.append(check_fingerprints(pg, ch, database))
    report.results.append(check_route_month_stats(ch, database))
    report.results.append(check_market_corridor(pg, ch, database))
    report.results.append(check_prediction_summary(pg, ch, database))
    for result in report.results:
        log_event(logger, "clickhouse_check", logging.INFO if result.ok else logging.ERROR,
                  check=result.name, ok=result.ok, as_of_date=as_of)
    return report


def main(argv: list[str] | None = None) -> int:
    configure_logging()
    database = argv[0] if argv else None
    try:
        config = load_config()
        settings = load_clickhouse_settings(database)
    except SettingsError as exc:
        log_event(logger, "clickhouse_check_config_error", logging.ERROR, error=str(exc))
        return 2
    try:
        with postgres_connection(config, "logistics_clickhouse_check", options="-c timezone=UTC") as pg:
            pg.isolation_level = IsolationLevel.REPEATABLE_READ
            as_of = pg.execute("SELECT max(as_of_date) FROM mart.mart_market_corridor").fetchone()[0]
            as_of = as_of or config.as_of_date
            pg.execute(OBSERVATION_TEMP_SQL, {"as_of": as_of})
            pg.execute(f"ANALYZE {OBSERVATION_TEMP_TABLE}")
            report = run_checks(pg, clickhouse_client(settings), settings.database, as_of)
            pg.rollback()
    except Exception as exc:
        log_event(logger, "clickhouse_check_failed", logging.ERROR, error=f"{type(exc).__name__}: {exc}")
        return 1
    log_event(logger, "clickhouse_check_report", logging.INFO if report.ok else logging.ERROR,
              database=settings.database, **json.loads(json.dumps(report.as_dict(), default=str)))
    return 0 if report.ok else 1


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
