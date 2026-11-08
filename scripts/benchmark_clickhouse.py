"""Benchmark PostgreSQL against ClickHouse on identical observation datasets and identical queries.

The dataset is the market corridor population (mart.market_corridor_observations) at scale factor k: k copies
with price_id offset by i * 10^12, so every copy is a distinct deal. PostgreSQL gets an UNLOGGED table in
schema bench with B-tree indexes on the route keys and the price date, vacuumed and analyzed. ClickHouse gets
a copy of fact_price_observation, merged to one part per partition. Before timing, both tables must have
equal fingerprints (row count + row hash per month); otherwise the benchmark stops.

Each query runs once to warm caches, then --runs times; the median and minimum wall-clock times are reported,
including result transfer to this client. Results of both systems are compared (counts exact, floats within
1e-9 relative). Requires a synced ClickHouse database (python -m etl.clickhouse.sync).

    python scripts/benchmark_clickhouse.py [--scales 1,10] [--runs 5] [--output result.json] [--keep]
"""

from __future__ import annotations

import argparse
import json
import math
import os
import platform
import statistics
import sys
import time
from dataclasses import replace
from datetime import date
from decimal import Decimal
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from etl.clickhouse.config import clickhouse_client, load_clickhouse_settings, postgres_connection  # noqa: E402
from etl.clickhouse.schema import FACT_PRICE_OBSERVATION, OBSERVATION_TEMP_SQL, OBSERVATION_TEMP_TABLE  # noqa: E402
from etl.config import load_config  # noqa: E402

PRICE_ID_STRIDE = 10**12
PG_COLUMNS = ", ".join(FACT_PRICE_OBSERVATION.column_names)

QUERIES = {
    "route_pricing": (
        "Route pricing aggregation: trips, mean, and median price per origin-destination pair",
        """SELECT loading_city_gcid, unloading_city_gcid, count(DISTINCT price_id), count(*),
                  avg(price_rub)::float8, avg(price_km), percentile_cont(0.5) WITHIN GROUP (ORDER BY price_rub)
           FROM {t} GROUP BY 1, 2 ORDER BY 1, 2""",
        """SELECT loading_city_gcid, unloading_city_gcid, uniqExact(price_id), count(),
                  toFloat64(avg(price_rub)), avg(price_km), quantileExactInclusive(0.5)(toFloat64(price_rub))
           FROM {t} GROUP BY 1, 2 ORDER BY 1, 2""",
    ),
    "monthly_corridors": (
        "Monthly market corridors at mart grain (month x route x body group x weight); summarized result",
        """SELECT count(*), sum(p25), sum(p50), sum(p75), sum(k50), sum(trips), sum(carriers), sum(cv)
           FROM (SELECT percentile_cont(0.25) WITHIN GROUP (ORDER BY price_rub) AS p25,
                        percentile_cont(0.50) WITHIN GROUP (ORDER BY price_rub) AS p50,
                        percentile_cont(0.75) WITHIN GROUP (ORDER BY price_rub) AS p75,
                        percentile_cont(0.50) WITHIN GROUP (ORDER BY price_km) AS k50,
                        count(DISTINCT price_id) AS trips,
                        count(DISTINCT organization_id) FILTER (WHERE price_type IN (3, 9)) AS carriers,
                        (CASE WHEN count(*) < 2 THEN NULL ELSE stddev_samp(price_rub) / avg(price_rub) END)::float8
                            AS cv
                 FROM {t}
                 GROUP BY price_month, loading_city_gcid, unloading_city_gcid, body_group_code,
                          weight_category) AS cell""",
        """SELECT count(), sum(q[1]), sum(q[2]), sum(q[3]), sum(k50), sum(trips), sum(carriers), sum(cv)
           FROM (SELECT quantilesExactInclusive(0.25, 0.5, 0.75)(toFloat64(price_rub)) AS q,
                        quantileExactInclusive(0.5)(price_km) AS k50,
                        uniqExact(price_id) AS trips,
                        uniqExactIf(organization_id, price_type IN (3, 9)) AS carriers,
                        if(count() < 2, NULL, stddevSampStable(toFloat64(price_rub)) / toFloat64(avg(price_rub)))
                            AS cv
                 FROM {t}
                 GROUP BY price_month, loading_city_gcid, unloading_city_gcid, body_group_code, weight_category)""",
    ),
    "percentiles": (
        "P25/P50/P75 of trip and per-km price per month and body group",
        """SELECT price_month, body_group_code,
                  percentile_cont(ARRAY[0.25, 0.5, 0.75]) WITHIN GROUP (ORDER BY price_rub),
                  percentile_cont(ARRAY[0.25, 0.5, 0.75]) WITHIN GROUP (ORDER BY price_km)
           FROM {t} GROUP BY 1, 2 ORDER BY 1, 2""",
        """SELECT price_month, body_group_code,
                  quantilesExactInclusive(0.25, 0.5, 0.75)(toFloat64(price_rub)),
                  quantilesExactInclusive(0.25, 0.5, 0.75)(price_km)
           FROM {t} GROUP BY 1, 2 ORDER BY 1, 2""",
    ),
    "large_date_range": (
        "Two years of price dates (2024-01-01..2025-12-31) by month: volume, trips, carriers, mean, median",
        """SELECT price_month, count(*), count(DISTINCT price_id), count(DISTINCT organization_id),
                  avg(price_rub)::float8, percentile_cont(0.5) WITHIN GROUP (ORDER BY price_rub)
           FROM {t} WHERE price_date BETWEEN DATE '2024-01-01' AND DATE '2025-12-31'
           GROUP BY 1 ORDER BY 1""",
        """SELECT price_month, count(), uniqExact(price_id), uniqExact(organization_id),
                  toFloat64(avg(price_rub)), quantileExactInclusive(0.5)(toFloat64(price_rub))
           FROM {t} WHERE price_date BETWEEN '2024-01-01' AND '2025-12-31'
           GROUP BY 1 ORDER BY 1""",
    ),
    "route_filter": (
        "One route (busiest origin-destination pair): monthly corridor by body group and weight category",
        """SELECT price_month, body_group_code, weight_category, count(DISTINCT price_id),
                  percentile_cont(ARRAY[0.25, 0.5, 0.75]) WITHIN GROUP (ORDER BY price_rub)
           FROM {t} WHERE loading_city_gcid = {origin} AND unloading_city_gcid = {destination}
           GROUP BY 1, 2, 3 ORDER BY 1, 2, 3""",
        """SELECT price_month, body_group_code, weight_category, uniqExact(price_id),
                  quantilesExactInclusive(0.25, 0.5, 0.75)(toFloat64(price_rub))
           FROM {t} WHERE loading_city_gcid = {origin} AND unloading_city_gcid = {destination}
           GROUP BY 1, 2, 3 ORDER BY 1, 2, 3""",
    ),
}


def _normalize(value):
    if isinstance(value, Decimal):
        return float(value)
    if isinstance(value, (list, tuple)):
        return [_normalize(v) for v in value]
    return value


def results_match(pg_rows, ch_rows) -> tuple[bool, str]:
    if len(pg_rows) != len(ch_rows):
        return False, f"row count {len(pg_rows)} != {len(ch_rows)}"
    for index, (a, b) in enumerate(zip(pg_rows, ch_rows, strict=True)):
        a, b = _normalize(list(a)), _normalize(list(b))
        if not _values_match(a, b):
            return False, f"row {index}: {a} != {b}"
    return True, "equal"


def _values_match(a, b) -> bool:
    if isinstance(a, list) and isinstance(b, list):
        return len(a) == len(b) and all(_values_match(x, y) for x, y in zip(a, b, strict=True))
    if isinstance(a, float) or isinstance(b, float):
        if a is None or b is None:
            return a is None and b is None
        return math.isclose(float(a), float(b), rel_tol=1e-9, abs_tol=1e-6)
    return a == b


def _timed(fn, runs: int) -> tuple[list[float], object]:
    result = fn()  # warm-up
    times = []
    for _ in range(runs):
        started = time.perf_counter()
        result = fn()
        times.append(time.perf_counter() - started)
    return times, result


def _summary(times: list[float]) -> dict:
    return {"median_s": round(statistics.median(times), 4), "min_s": round(min(times), 4),
            "runs": [round(t, 4) for t in times]}


def prepare_postgres(pg, scale: int) -> str:
    table = f"bench.price_observation_x{scale}"
    pg.execute("CREATE SCHEMA IF NOT EXISTS bench")
    pg.execute(f"DROP TABLE IF EXISTS {table}")
    pg.execute(f"CREATE UNLOGGED TABLE {table} AS SELECT {PG_COLUMNS} FROM {OBSERVATION_TEMP_TABLE} WITH NO DATA")
    pg.execute(f"""
        INSERT INTO {table}
        SELECT {PG_COLUMNS.replace('price_id', f'price_id + copy * {PRICE_ID_STRIDE} AS price_id', 1)}
        FROM {OBSERVATION_TEMP_TABLE}, generate_series(0, %(last)s) AS copy
        ORDER BY price_month, price_id""", {"last": scale - 1})
    pg.execute(f"CREATE INDEX ON {table} (loading_city_gcid, unloading_city_gcid, body_group_code, "
               "weight_category, price_month)")
    pg.execute(f"CREATE INDEX ON {table} (price_date)")
    pg.commit()
    pg.autocommit = True
    pg.execute(f"VACUUM (ANALYZE) {table}")
    pg.autocommit = False
    return table


def prepare_clickhouse(ch, database: str, scale: int) -> str:
    table = f"{database}.bench_price_observation_x{scale}"
    ch.command(f"DROP TABLE IF EXISTS {table}")
    ch.command(f"CREATE TABLE {table} AS {database}.fact_price_observation")
    ch.command(f"INSERT INTO {table} SELECT o.* REPLACE (o.price_id + n.number * {PRICE_ID_STRIDE} AS price_id) "
               f"FROM {database}.fact_price_observation AS o CROSS JOIN numbers({scale}) AS n "
               "SETTINGS max_partitions_per_insert_block = 0")
    ch.command(f"OPTIMIZE TABLE {table} FINAL")
    return table


def fingerprints_equal(pg, ch, pg_table: str, ch_table: str) -> tuple[bool, int]:
    spec = replace(FACT_PRICE_OBSERVATION, pg_source=pg_table)
    source = {p: (int(n), h) for p, n, h in pg.execute(spec.pg_fingerprint_sql()).fetchall()}
    database, table = ch_table.split(".")
    target = {p: (int(n), h) for p, n, h in ch.query(spec.ch_fingerprint_sql(database, table)).result_rows}
    return source == target, sum(n for n, _ in source.values())


def environment(pg, ch) -> dict:
    settings = {name: pg.execute(f"SHOW {name}").fetchone()[0] for name in (
        "max_parallel_workers_per_gather", "max_parallel_workers", "work_mem", "shared_buffers", "jit")}
    mem_kb = next(int(line.split()[1]) for line in Path("/proc/meminfo").read_text().splitlines()
                  if line.startswith("MemTotal"))
    return {
        "cpu_count": os.cpu_count(),
        "cpu_model": next((line.split(":", 1)[1].strip() for line in Path("/proc/cpuinfo").read_text().splitlines()
                           if line.startswith("model name")), platform.processor()),
        "memory_gb": round(mem_kb / 1024 / 1024, 1),
        "postgres": pg.execute("SHOW server_version").fetchone()[0],
        "postgres_settings": settings,
        "clickhouse": ch.query("SELECT version()").result_rows[0][0],
        "clickhouse_max_threads": ch.query("SELECT getSetting('max_threads')").result_rows[0][0],
    }


def run(scales: list[int], runs: int, keep: bool) -> dict:
    config = load_config()
    settings = load_clickhouse_settings()
    ch = clickhouse_client(settings)
    output = {"generated_on": date.today().isoformat(), "runs_per_query": runs, "scales": []}
    with postgres_connection(config, "logistics_clickhouse_benchmark", options="-c timezone=UTC") as pg:
        output["environment"] = environment(pg, ch)
        as_of = pg.execute("SELECT max(as_of_date) FROM mart.mart_market_corridor").fetchone()[0] or config.as_of_date
        output["as_of_date"] = as_of.isoformat()
        tables = []
        try:
            for scale in scales:
                pg.execute(OBSERVATION_TEMP_SQL.replace("ON COMMIT DROP", ""), {"as_of": as_of})
                pg_table = prepare_postgres(pg, scale)
                pg.execute(f"DROP TABLE IF EXISTS {OBSERVATION_TEMP_TABLE}")
                ch_table = prepare_clickhouse(ch, settings.database, scale)
                tables.append((pg_table, ch_table))
                identical, rows = fingerprints_equal(pg, ch, pg_table, ch_table)
                if not identical:
                    raise RuntimeError(f"scale {scale}: PostgreSQL and ClickHouse datasets differ")
                origin, destination = pg.execute(f"SELECT loading_city_gcid, unloading_city_gcid FROM {pg_table} "
                                                 "GROUP BY 1, 2 ORDER BY count(*) DESC, 1, 2 LIMIT 1").fetchone()
                pg_size = pg.execute(f"SELECT pg_total_relation_size('{pg_table}')").fetchone()[0]
                ch_size = ch.query(f"SELECT sum(bytes_on_disk) FROM system.parts WHERE active AND "
                                   f"database = '{settings.database}' AND table = '{ch_table.split('.')[1]}'"
                                   ).result_rows[0][0]
                entry = {"scale": scale, "rows": rows, "datasets_identical": identical,
                         "route_filter_route": [origin, destination],
                         "storage_bytes": {"postgres_table_and_indexes": pg_size, "clickhouse_compressed": ch_size},
                         "queries": {}}
                for name, (description, pg_sql, ch_sql) in QUERIES.items():
                    fmt = {"origin": origin, "destination": destination}
                    pg_query = pg_sql.format(t=pg_table, **fmt)
                    ch_query = ch_sql.format(t=ch_table, **fmt)
                    result = {"description": description}
                    for workers in (None, 8):
                        if workers:
                            pg.execute(f"SET max_parallel_workers_per_gather = {workers}")
                        times, pg_rows = _timed(lambda q=pg_query: pg.execute(q).fetchall(), runs)
                        pg.execute("RESET max_parallel_workers_per_gather")
                        result["postgres_default" if workers is None else "postgres_8_workers"] = _summary(times)
                    for threads in (0, 1):
                        query_settings = {"max_threads": threads} if threads else {}
                        times, ch_rows = _timed(
                            lambda q=ch_query, s=query_settings: ch.query(q, settings=s).result_rows, runs)
                        result["clickhouse_default" if threads == 0 else "clickhouse_1_thread"] = _summary(times)
                    match, detail = results_match(pg_rows, ch_rows)
                    result["results_match"] = match
                    result["result_rows"] = len(pg_rows)
                    if not match:
                        result["mismatch"] = detail
                    result["speedup_default"] = round(result["postgres_default"]["median_s"]
                                                      / result["clickhouse_default"]["median_s"], 1)
                    entry["queries"][name] = result
                    print(json.dumps({"scale": scale, "query": name, "pg_s": result["postgres_default"]["median_s"],
                                      "ch_s": result["clickhouse_default"]["median_s"], "match": match}),
                          file=sys.stderr)
                output["scales"].append(entry)
                pg.commit()
        finally:
            pg.rollback()
            if not keep:
                for pg_table, ch_table in tables:
                    pg.execute(f"DROP TABLE IF EXISTS {pg_table}")
                    ch.command(f"DROP TABLE IF EXISTS {ch_table}")
                pg.execute("DROP SCHEMA IF EXISTS bench")
                pg.commit()
    return output


def markdown(output: dict) -> str:
    lines = ["| Scale | Rows | Query | PostgreSQL default | PostgreSQL 8 workers | ClickHouse default | "
             "ClickHouse 1 thread | Speedup | Results equal |", "|---|---|---|---|---|---|---|---|---|"]
    for entry in output["scales"]:
        for name, q in entry["queries"].items():
            lines.append(f"| {entry['scale']}x | {entry['rows']:,} | {name} | {q['postgres_default']['median_s']:.3f} s"
                         f" | {q['postgres_8_workers']['median_s']:.3f} s | {q['clickhouse_default']['median_s']:.3f} s"
                         f" | {q['clickhouse_1_thread']['median_s']:.3f} s | {q['speedup_default']}x"
                         f" | {'yes' if q['results_match'] else 'NO'} |")
    return "\n".join(lines)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    parser.add_argument("--scales", default="1,10")
    parser.add_argument("--runs", type=int, default=5)
    parser.add_argument("--output", type=Path)
    parser.add_argument("--keep", action="store_true", help="keep the benchmark tables")
    args = parser.parse_args()
    scales = [int(s) for s in args.scales.split(",")]
    if any(s < 1 for s in scales) or args.runs < 1:
        parser.error("scales and runs must be positive")
    output = run(scales, args.runs, args.keep)
    if args.output:
        args.output.write_text(json.dumps(output, indent=2, default=str) + "\n", encoding="utf-8")
    print(markdown(output))
    mismatched = [(e["scale"], n) for e in output["scales"] for n, q in e["queries"].items() if not q["results_match"]]
    return 1 if mismatched else 0


if __name__ == "__main__":
    sys.exit(main())
