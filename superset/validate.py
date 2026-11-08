"""Check Superset chart results against direct PostgreSQL queries.

Each dashboard chart's saved query is sent to the chart data API under several filter combinations (the
filters the dashboard's native filters would add) and compared with reference SQL that reads the mart table,
the mart observation function, and mart.ml_price_predictions directly, not the BI views.

Environment: SUPERSET_URL, SUPERSET_ADMIN_USERNAME, SUPERSET_ADMIN_PASSWORD, POSTGRES_HOST, POSTGRES_PORT,
DWH_DB_NAME, DWH_DB_USER, DWH_DB_PASSWORD. Exit code 0 when every check matches, 1 otherwise.
"""

from __future__ import annotations

import datetime as dt
import json
import logging
import math
import os
import sys
from decimal import Decimal
from typing import Any, Callable

import psycopg2

from provision import CHARTS, DASHBOARD_SLUG, Superset

log = logging.getLogger("superset.validate")

REL_TOL = 1e-6

OBSERVATIONS_SQL = """
WITH snapshot AS (SELECT max(as_of_date) AS as_of_date FROM mart.mart_market_corridor)
SELECT o.price_id, o.price_month, o.price_rub, o.price_km,
       CASE WHEN o.price_type IN (3, 9) THEN o.organization_id END AS carrier_id,
       c.price_month AS cell_month, c.origin_city_name, c.destination_city_name, c.body_group_name,
       c.weight_category, c.reliability_level
FROM snapshot AS s
CROSS JOIN LATERAL mart.market_corridor_observations(s.as_of_date) AS o
JOIN mart.mart_market_corridor AS c
  ON (c.price_month, c.loading_city_gcid, c.unloading_city_gcid, c.body_group_code, c.weight_category)
   = (o.price_month, o.loading_city_gcid, o.unloading_city_gcid, o.body_group_code, o.weight_category)
"""

# Reference columns for the dashboard filter columns, per source.
OBS_COLUMNS = {"month": "to_char(price_month, 'YYYY-MM')", "origin_city": "origin_city_name",
               "destination_city": "destination_city_name", "body_group": "body_group_name",
               "weight_category": "weight_category", "reliability_level": "reliability_level"}
ROUTE_COLUMNS = OBS_COLUMNS
FORECAST_SQL = """
SELECT p.price_id, p.loading_date, p.actual_price_rub, p.predicted_price_rub, p.abs_error_rub, p.ape_pct,
       p.model_name, p.model_version, p.split_role,
       CASE WHEN p.split_role = 'test' THEN 'historical' ELSE 'future' END AS record_type,
       lc.name AS origin_city, uc.name AS destination_city, bg.body_group_name, f.weight_category
FROM ml.ml_price_predictions AS p
JOIN core.fact_deal AS f ON f.price_id = p.price_id
JOIN core.dim_city AS lc ON lc.gc_id = p.loading_city_gcid
JOIN core.dim_city AS uc ON uc.gc_id = p.unloading_city_gcid
JOIN core.dim_body_group AS bg ON bg.body_group_code = p.body_group
"""
FORECAST_COLUMNS = {"month": "to_char(loading_date, 'YYYY-MM')", "origin_city": "origin_city",
                    "destination_city": "destination_city", "body_group": "body_group_name",
                    "weight_category": "weight_category"}


class Validator:
    def __init__(self, api: Superset, conn: Any, chart_ids: dict[str, int]):
        self.api = api
        self.conn = conn
        self.chart_ids = chart_ids
        self.results: list[tuple[str, str, bool, str]] = []

    # -- data access -------------------------------------------------------------------------------------------

    def sql(self, query: str, params: Any = None) -> list[dict]:
        with self.conn.cursor() as cur:
            cur.execute(query, params)
            names = [d.name for d in cur.description]
            return [dict(zip(names, row, strict=True)) for row in cur.fetchall()]

    def chart(self, key: str, filters: list[dict], mutate: Callable[[dict], None] | None = None) -> list[dict]:
        chart = self.api.get(f"/api/v1/chart/{self.chart_ids[key]}")["result"]
        context = json.loads(chart["query_context"])
        context["force"] = True
        query = context["queries"][0]
        query["filters"] = [*query["filters"], *filters]
        if mutate:
            mutate(query)
        response = self.api.post("/api/v1/chart/data", context)
        return response["result"][0]["data"]

    # -- comparison --------------------------------------------------------------------------------------------

    def record(self, scenario: str, check: str, ok: bool, detail: str) -> None:
        self.results.append((scenario, check, ok, detail))
        (log.info if ok else log.error)("%-28s %-48s %s %s", scenario, check, "OK " if ok else "BAD", detail)

    @staticmethod
    def same(a: Any, b: Any) -> bool:
        if a is None or b is None:
            return a is None and b is None
        if isinstance(a, (int, float, Decimal)) and isinstance(b, (int, float, Decimal)):
            a, b = float(a), float(b)
            if math.isnan(a) or math.isnan(b):
                return math.isnan(a) and math.isnan(b)
            return math.isclose(a, b, rel_tol=REL_TOL, abs_tol=1e-9)
        return a == b

    def compare_value(self, scenario: str, check: str, superset_value: Any, sql_value: Any) -> None:
        self.record(scenario, check, self.same(superset_value, sql_value),
                    f"superset={fmt(superset_value)} sql={fmt(sql_value)}")

    def compare_rows(self, scenario: str, check: str, superset_rows: dict, sql_rows: dict) -> None:
        missing = set(sql_rows) - set(superset_rows)
        extra = set(superset_rows) - set(sql_rows)
        diffs = [k for k in set(sql_rows) & set(superset_rows)
                 if not all(self.same(x, y) for x, y in zip(superset_rows[k], sql_rows[k], strict=True))]
        ok = not missing and not extra and not diffs
        detail = f"rows={len(superset_rows)}/{len(sql_rows)}"
        if not ok:
            sample = sorted(diffs, key=str)[:2]
            detail += (f" missing={len(missing)} extra={len(extra)} differing={len(diffs)} "
                       f"e.g. {[(k, superset_rows[k], sql_rows[k]) for k in sample]}")
        self.record(scenario, check, ok, detail)


def fmt(value: Any) -> str:
    if isinstance(value, (float, Decimal)):
        return f"{float(value):,.4f}"
    return str(value)


def where(filters: dict[str, list[str]], columns: dict[str, str]) -> tuple[str, list]:
    clauses, params = [], []
    for name, values in filters.items():
        if name in columns:
            clauses.append(f"{columns[name]} = ANY(%s)")
            params.append(values)
    return (" AND ".join(clauses) or "TRUE"), params


def superset_filters(filters: dict[str, list[str]], allowed: set[str] | None = None) -> list[dict]:
    return [{"col": name, "op": "IN", "val": values} for name, values in filters.items()
            if allowed is None or name in allowed]


def month_key(value: Any) -> str:
    if isinstance(value, (int, float)):
        return dt.datetime.fromtimestamp(value / 1000, dt.timezone.utc).strftime("%Y-%m")
    return str(value)[:7]


def day_key(value: Any) -> str:
    if isinstance(value, (int, float)):
        return dt.datetime.fromtimestamp(value / 1000, dt.timezone.utc).strftime("%Y-%m-%d")
    return str(value)[:10]


# ---------------------------------------------------------------------------------------------------------------
# Checks
# ---------------------------------------------------------------------------------------------------------------


def check_population(v: Validator) -> None:
    """The observation set behind v_market_observation reproduces every mart cell."""
    rows = v.sql(f"""
        WITH obs AS ({OBSERVATIONS_SQL}),
        cell AS (
            SELECT cell_month, origin_city_name, destination_city_name, body_group_name, weight_category,
                   count(*) AS trips, count(DISTINCT carrier_id) AS carriers,
                   percentile_cont(0.5) WITHIN GROUP (ORDER BY price_rub) AS median_trip
            FROM obs GROUP BY 1, 2, 3, 4, 5)
        SELECT (SELECT count(*) FROM mart.mart_market_corridor) AS mart_cells,
               count(*) AS obs_cells,
               count(*) FILTER (WHERE m.trips_count <> c.trips OR m.carriers_count <> c.carriers
                                OR abs(m.median_trip - c.median_trip::numeric) > 0.01) AS mismatched
        FROM cell AS c
        JOIN mart.mart_market_corridor AS m
          ON (m.price_month, m.origin_city_name, m.destination_city_name, m.body_group_name, m.weight_category)
           = (c.cell_month, c.origin_city_name, c.destination_city_name, c.body_group_name, c.weight_category)
    """)[0]
    ok = rows["mart_cells"] == rows["obs_cells"] and rows["mismatched"] == 0
    v.record("all data", "observations reproduce mart cells", ok,
             f"mart_cells={rows['mart_cells']} matched_cells={rows['obs_cells']} mismatched={rows['mismatched']}")


def check_overview(v: Validator, scenario: str, filters: dict[str, list[str]]) -> None:
    clause, params = where(filters, OBS_COLUMNS)
    ref = v.sql(f"""
        SELECT count(DISTINCT price_id) AS completed_trips, count(DISTINCT carrier_id) AS unique_carriers,
               percentile_cont(0.5) WITHIN GROUP (ORDER BY price_rub) AS median_rate, avg(price_rub) AS avg_rate,
               avg(price_km) AS avg_price_km,
               avg(CASE WHEN reliability_level IN ('HIGH', 'MEDIUM') THEN 1.0 ELSE 0.0 END) AS reliable_share
        FROM ({OBSERVATIONS_SQL}) AS o WHERE {clause}""", params)[0]
    sf = superset_filters(filters)
    for key, metric in [("kpi_trips", "completed_trips"), ("kpi_carriers", "unique_carriers"),
                        ("kpi_median", "median_rate"), ("kpi_avg", "avg_rate"), ("kpi_km", "avg_price_km"),
                        ("kpi_reliable", "reliable_share")]:
        data = v.chart(key, sf)
        v.compare_value(scenario, CHARTS[key]["name"], data[0][metric] if data else None, ref[metric])

    ref_rel = {r["reliability_level"]: (r["n"],) for r in v.sql(
        f"SELECT reliability_level, count(*) AS n FROM ({OBSERVATIONS_SQL}) AS o WHERE {clause} GROUP BY 1", params)}
    got = {r["reliability_level"]: (r["observations"],) for r in v.chart("overview_reliability", sf)}
    v.compare_rows(scenario, CHARTS["overview_reliability"]["name"], got, ref_rel)

    ref_body = {r["body_group_name"]: (r["m"],) for r in v.sql(
        f"""SELECT body_group_name, percentile_cont(0.5) WITHIN GROUP (ORDER BY price_rub) AS m
            FROM ({OBSERVATIONS_SQL}) AS o WHERE {clause} GROUP BY 1""", params)}
    got = {r["body_group"]: (r["median_rate"],) for r in v.chart("overview_body", sf)}
    v.compare_rows(scenario, CHARTS["overview_body"]["name"], got, ref_body)


def check_route_table(v: Validator, scenario: str, filters: dict[str, list[str]]) -> None:
    clause, params = where(filters, ROUTE_COLUMNS)
    value_columns = ["p25_trip", "p50_trip", "p75_trip", "p25_km", "p50_km", "p75_km", "mom_pct", "yoy_pct",
                     "trips_count", "carriers_count", "reliability_level"]
    ref = {(to_month(r["price_month"]), r["origin_city_name"], r["destination_city_name"], r["body_group_name"],
            r["weight_category"]): tuple(r[c] for c in value_columns)
           for r in v.sql(f"""
               SELECT price_month, origin_city_name, destination_city_name, body_group_name, weight_category,
                      p25_trip, median_trip AS p50_trip, p75_trip, p25_km, median_km AS p50_km, p75_km, mom_pct,
                      yoy_pct, trips_count, carriers_count, reliability_level
               FROM mart.mart_market_corridor WHERE {clause}""", params)}

    def unpaged(query: dict) -> None:
        query["row_limit"] = 1_000_000

    got = {(r["month"], r["origin_city"], r["destination_city"], r["body_group"], r["weight_category"]):
           tuple(r[c] for c in value_columns) for r in v.chart("route_table", superset_filters(filters), unpaged)}
    v.compare_rows(scenario, "Route pricing (all columns)", got, ref)


def to_month(value: dt.date) -> str:
    return value.strftime("%Y-%m")


def check_dynamics(v: Validator, scenario: str, filters: dict[str, list[str]]) -> None:
    clause, params = where(filters, OBS_COLUMNS)
    monthly = v.sql(f"""
        SELECT to_char(price_month, 'YYYY-MM') AS month,
               percentile_cont(0.25) WITHIN GROUP (ORDER BY price_rub) AS p25,
               percentile_cont(0.5) WITHIN GROUP (ORDER BY price_rub) AS p50,
               percentile_cont(0.75) WITHIN GROUP (ORDER BY price_rub) AS p75
        FROM ({OBSERVATIONS_SQL}) AS o WHERE {clause} GROUP BY 1 ORDER BY 1""", params)
    sf = superset_filters(filters)

    got = {month_key(r["price_month"]): (r["median_rate"],) for r in v.chart("dyn_median", sf)}
    v.compare_rows(scenario, CHARTS["dyn_median"]["name"], got, {r["month"]: (r["p50"],) for r in monthly})

    got = {month_key(r["price_month"]): (r["p25_rate"], r["median_rate"], r["p75_rate"])
           for r in v.chart("dyn_corridor", sf)}
    v.compare_rows(scenario, CHARTS["dyn_corridor"]["name"], got,
                   {r["month"]: (r["p25"], r["p50"], r["p75"]) for r in monthly})

    by_month = {r["month"]: float(r["p50"]) for r in monthly}
    for key, months_back, label in [("dyn_mom", 1, "1 month ago"), ("dyn_yoy", 12, "1 year ago")]:
        ref = {}
        for month, median in by_month.items():
            year, mon = int(month[:4]), int(month[5:])
            index = year * 12 + mon - 1 - months_back
            previous = by_month.get(f"{index // 12:04d}-{index % 12 + 1:02d}")
            ref[month] = (median / previous - 1 if previous else None,)
        got = {}
        for r in v.chart(key, sf):
            current, previous = r["median_rate"], r.get(f"median_rate__{label}")
            got[month_key(r["price_month"])] = (current / previous - 1 if current is not None and previous else None,)
        v.compare_rows(scenario, CHARTS[key]["name"] + " (% change)", got, ref)

    ref = {(r["y"], r["m"]): (r["median"],) for r in v.sql(f"""
        SELECT extract(year FROM price_month)::int AS y, extract(month FROM price_month)::int AS m,
               percentile_cont(0.5) WITHIN GROUP (ORDER BY price_rub) AS median
        FROM ({OBSERVATIONS_SQL}) AS o WHERE {clause} GROUP BY 1, 2""", params)}
    got = {(r["year"], r["month_of_year"]): (r["median_rate"],) for r in v.chart("dyn_season", sf)}
    v.compare_rows(scenario, CHARTS["dyn_season"]["name"], got, ref)


def check_forecast(v: Validator, scenario: str, filters: dict[str, list[str]]) -> None:
    clause, params = where(filters, FORECAST_COLUMNS)
    sf = superset_filters(filters, set(FORECAST_COLUMNS))
    base = f"SELECT * FROM ({FORECAST_SQL}) AS p WHERE {clause}"
    ref = v.sql(f"""
        SELECT avg(abs_error_rub) FILTER (WHERE record_type = 'historical') AS mae,
               percentile_cont(0.5) WITHIN GROUP (ORDER BY ape_pct) FILTER (WHERE record_type = 'historical')
                   AS median_ape,
               count(*) FILTER (WHERE record_type = 'historical') AS hist,
               count(*) FILTER (WHERE record_type = 'future') AS future
        FROM ({base}) AS b""", params)[0]

    def first(key: str, metric: str) -> Any:
        data = v.chart(key, sf)
        return data[0][metric] if data else None

    v.compare_value(scenario, CHARTS["fc_mae"]["name"], first("fc_mae", "mae"), ref["mae"])
    v.compare_value(scenario, CHARTS["fc_median_ape"]["name"], first("fc_median_ape", "median_ape"),
                    ref["median_ape"])
    v.compare_value(scenario, CHARTS["fc_hist_count"]["name"], first("fc_hist_count", "deals"), ref["hist"])
    v.compare_value(scenario, CHARTS["fc_future_count"]["name"], first("fc_future_count", "deals"), ref["future"])

    ref_models = {(r["model_name"], r["model_version"], r["record_type"]): (r["n"],) for r in v.sql(
        f"SELECT model_name, model_version, record_type, count(*) AS n FROM ({base}) AS b GROUP BY 1, 2, 3", params)}
    got = {(r["model_name"], r["model_version"], r["record_type"]): (r["deals"],) for r in v.chart("fc_models", sf)}
    v.compare_rows(scenario, CHARTS["fc_models"]["name"], got, ref_models)

    ref_daily = {day_key(r["d"]): (r["a"], r["p"]) for r in v.sql(f"""
        SELECT loading_date AS d, avg(actual_price_rub) AS a, avg(predicted_price_rub) AS p
        FROM ({base}) AS b WHERE record_type = 'historical' GROUP BY 1""", params)}
    got = {day_key(r["loading_date"]): (r["avg_actual"], r["avg_predicted"]) for r in v.chart("fc_hist_trend", sf)}
    v.compare_rows(scenario, CHARTS["fc_hist_trend"]["name"], got, ref_daily)

    ref_weekly = {day_key(r["w"]): (r["p"],) for r in v.sql(f"""
        SELECT date_trunc('week', loading_date)::date AS w, avg(predicted_price_rub) AS p
        FROM ({base}) AS b WHERE record_type = 'future' GROUP BY 1""", params)}
    got = {day_key(r["loading_date"]): (r["avg_predicted"],) for r in v.chart("fc_future_trend", sf)}
    v.compare_rows(scenario, CHARTS["fc_future_trend"]["name"], got, ref_weekly)

    ref_band = {r["band"]: (r["n"],) for r in v.sql(f"""
        SELECT CASE WHEN ape_pct < 5 THEN '00-05%%' WHEN ape_pct < 10 THEN '05-10%%'
                    WHEN ape_pct < 20 THEN '10-20%%' WHEN ape_pct < 30 THEN '20-30%%'
                    WHEN ape_pct < 50 THEN '30-50%%' ELSE '50%%+' END AS band,
               count(*) AS n
        FROM ({base}) AS b WHERE record_type = 'historical' GROUP BY 1""", params)}
    got = {r["ape_band"]: (r["deals"],) for r in v.chart("fc_ape_band", sf)}
    v.compare_rows(scenario, CHARTS["fc_ape_band"]["name"], got, ref_band)

    def unpaged(query: dict) -> None:
        query["row_limit"] = 1_000_000

    ref_hist = {r["price_id"]: (r["actual_price_rub"], r["predicted_price_rub"], r["abs_error_rub"],
                                r["signed_pct"], r["model_version"]) for r in v.sql(f"""
        SELECT price_id, actual_price_rub, predicted_price_rub, abs_error_rub, model_version,
               round((predicted_price_rub - actual_price_rub) / actual_price_rub * 100, 4) AS signed_pct
        FROM ({base}) AS b WHERE record_type = 'historical'""", params)}
    rows = v.chart("fc_hist_table", sf, lambda q: (unpaged(q), q["columns"].append("price_id")))
    got = {r["price_id"]: (r["actual_price_rub"], r["predicted_price_rub"], r["abs_error_rub"], r["pct_error"],
                           r["model_version"]) for r in rows}
    v.compare_rows(scenario, CHARTS["fc_hist_table"]["name"], got, ref_hist)

    future_actuals = v.chart("fc_future_table", sf, lambda q: (unpaged(q), q["columns"].append("actual_price_rub")))
    leaked = sum(1 for r in future_actuals if r["actual_price_rub"] is not None)
    v.record(scenario, "Future rows carry no actual price", leaked == 0,
             f"future_rows={len(future_actuals)} with_actual={leaked}")


def main() -> int:
    logging.basicConfig(level=os.environ.get("LOG_LEVEL", "INFO"), format="%(levelname)s %(message)s")
    env = os.environ
    api = Superset(env["SUPERSET_URL"], env["SUPERSET_ADMIN_USERNAME"], env["SUPERSET_ADMIN_PASSWORD"])
    dashboard = api.get(f"/api/v1/dashboard/{DASHBOARD_SLUG}")["result"]
    charts = api.get(f"/api/v1/dashboard/{dashboard['id']}/charts")["result"]
    by_name = {c["slice_name"]: c["id"] for c in charts}
    chart_ids = {key: by_name[spec["name"]] for key, spec in CHARTS.items()}

    conn = psycopg2.connect(host=env["POSTGRES_HOST"], port=env["POSTGRES_PORT"], dbname=env["DWH_DB_NAME"],
                            user=env["DWH_DB_USER"], password=env["DWH_DB_PASSWORD"])
    conn.set_session(readonly=True, autocommit=True)
    v = Validator(api, conn, chart_ids)

    top = v.sql("""SELECT origin_city_name, body_group_name, max(to_char(price_month, 'YYYY-MM')) AS month
                   FROM mart.mart_market_corridor GROUP BY 1, 2 ORDER BY sum(trips_count) DESC LIMIT 1""")[0]
    previous_month = v.sql("""SELECT to_char(max(price_month) - interval '1 month', 'YYYY-MM') AS m
                              FROM mart.mart_market_corridor""")[0]["m"]
    scenarios = {
        "no filters": {},
        f"month {previous_month}": {"month": [previous_month]},
        f"{top['origin_city_name']} / {top['body_group_name']}": {"origin_city": [top["origin_city_name"]],
                                                                   "body_group": [top["body_group_name"]]},
        "FTL, HIGH+MEDIUM": {"weight_category": ["FTL"], "reliability_level": ["HIGH", "MEDIUM"]},
    }
    route_scenarios = {
        f"month {previous_month}": {"month": [previous_month]},
        f"{top['origin_city_name']} / {top['body_group_name']}": scenarios[
            f"{top['origin_city_name']} / {top['body_group_name']}"],
        "FTL, HIGH+MEDIUM": scenarios["FTL, HIGH+MEDIUM"],
    }
    forecast_scenarios = {
        "no filters": {},
        "month 2026-10": {"month": ["2026-10"]},
        f"{top['origin_city_name']} / {top['body_group_name']}": scenarios[
            f"{top['origin_city_name']} / {top['body_group_name']}"],
        "FTL": {"weight_category": ["FTL"]},
    }

    check_population(v)
    for name, filters in scenarios.items():
        check_overview(v, name, filters)
        if "month" not in filters:
            check_dynamics(v, name, filters)
    for name, filters in route_scenarios.items():
        check_route_table(v, name, filters)
    for name, filters in forecast_scenarios.items():
        check_forecast(v, name, filters)

    failed = [r for r in v.results if not r[2]]
    print(f"\n{len(v.results) - len(failed)} of {len(v.results)} checks match PostgreSQL")
    if env.get("VALIDATION_REPORT"):
        with open(env["VALIDATION_REPORT"], "w", encoding="utf-8") as handle:
            json.dump([{"scenario": s, "check": c, "ok": ok, "detail": d} for s, c, ok, d in v.results], handle,
                      ensure_ascii=False, indent=1)
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
