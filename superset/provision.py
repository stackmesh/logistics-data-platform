"""Provision the Smart Logistics Superset dashboard over the REST API. Repeatable: objects are matched by
name and updated in place.

Environment: SUPERSET_URL, SUPERSET_ADMIN_USERNAME, SUPERSET_ADMIN_PASSWORD, BI_DB_HOST, BI_DB_PORT,
DWH_DB_NAME, BI_DB_USER, BI_DB_PASSWORD.
"""

from __future__ import annotations

import json
import logging
import os
import sys
import time
from typing import Any
from urllib.parse import quote

log = logging.getLogger("superset.provision")

DATABASE_NAME = "Smart Logistics DWH (mart)"
SCHEMA = "mart"
DASHBOARD_TITLE = "Smart Logistics — Market Rate Intelligence"
DASHBOARD_SLUG = "market-rate-intelligence"

RUB = ",.0f"
RUB_KM = ",.1f"
# Superset time comparison requires a closed time range; this one spans all warehouse data.
ALL_TIME = "2000-01-01 : 2100-01-01"
# Paged tables reach every route cell; SQL_MAX_ROW in superset_config.py must be at least this.
TABLE_ROW_LIMIT = 250_000

# ---------------------------------------------------------------------------------------------------------------
# Datasets: the three BI views. Metrics restate mart definitions; none introduces a new business rule.
# ---------------------------------------------------------------------------------------------------------------

DATASETS: dict[str, dict[str, Any]] = {
    "v_market_observation": {
        "description": "One row per market observation (deal, price month, body group), the population of the "
        "market corridor mart. Use for market-level KPIs and medians over any filter combination.",
        "main_dttm_col": "price_month",
        "columns": {
            "price_month": "Month (date)",
            "month": "Month",
            "year": "Year",
            "month_of_year": "Month of year",
            "price_date": "Price date",
            "price_id": "Deal ID",
            "origin_city": "Origin city",
            "destination_city": "Destination city",
            "body_group": "Body group",
            "weight_category": "Weight category",
            "price_rub": "Trip price, RUB",
            "price_km": "Price per km, RUB",
            "carrier_id": "Carrier ID",
            "reliability_level": "Reliability",
            "as_of_date": "Snapshot date",
        },
        "metrics": {
            "completed_trips": ("COUNT(DISTINCT price_id)", "Completed trips", ",d",
                                "Distinct deals in the market corridor population."),
            "observations": ("COUNT(*)", "Observations", ",d",
                             "Deal x month x body group rows; equals SUM(trips_count) of the mart cells."),
            "unique_carriers": ("COUNT(DISTINCT carrier_id)", "Unique carriers", ",d",
                                "Distinct carrier organizations (price types 3 and 9), as in carriers_count."),
            "median_rate": ("percentile_cont(0.5) WITHIN GROUP (ORDER BY price_rub)", "Median market rate, RUB", RUB,
                            "Median trip price over observations, the mart's median_trip at market level."),
            "p25_rate": ("percentile_cont(0.25) WITHIN GROUP (ORDER BY price_rub)", "P25 rate, RUB", RUB, ""),
            "p75_rate": ("percentile_cont(0.75) WITHIN GROUP (ORDER BY price_rub)", "P75 rate, RUB", RUB, ""),
            "avg_rate": ("AVG(price_rub)", "Average market rate, RUB", RUB, "Mean trip price over observations."),
            "avg_price_km": ("AVG(price_km)", "Average price per km, RUB", RUB_KM,
                             "Mean of price_rub / distance_km over observations."),
            "median_price_km": ("percentile_cont(0.5) WITHIN GROUP (ORDER BY price_km)", "Median price per km, RUB",
                                RUB_KM, ""),
            "reliable_share": ("AVG(CASE WHEN reliability_level IN ('HIGH', 'MEDIUM') THEN 1.0 ELSE 0.0 END)",
                               "Observations in reliable cells", ".1%",
                               "Share of observations whose mart cell is HIGH or MEDIUM reliability."),
        },
    },
    "v_route_pricing": {
        "description": "Market corridor mart: one row per month, route, body group, and weight category.",
        "main_dttm_col": "price_month",
        "columns": {
            "price_month": "Month (date)",
            "month": "Month",
            "origin_city": "Origin city",
            "destination_city": "Destination city",
            "body_group": "Body group",
            "weight_category": "Weight category",
            "p25_trip": "P25 trip, RUB",
            "p50_trip": "P50 trip, RUB",
            "p75_trip": "P75 trip, RUB",
            "avg_trip": "Average trip, RUB",
            "p25_km": "P25 per km, RUB",
            "p50_km": "P50 per km, RUB",
            "p75_km": "P75 per km, RUB",
            "mom_pct": "MoM %",
            "yoy_pct": "YoY %",
            "trips_count": "Trips",
            "carriers_count": "Carriers",
            "reliability_level": "Reliability",
            "cv_rate": "Coefficient of variation",
            "max_price_date": "Latest price date",
            "freshness_days": "Days since latest price",
            "as_of_date": "Snapshot date",
        },
        "metrics": {
            "route_cells": ("COUNT(*)", "Route cells", ",d", ""),
            "route_trips": ("SUM(trips_count)", "Trips (cell sum)", ",d", ""),
        },
    },
    "v_fact_vs_forecast": {
        "description": "Actual vs predicted transportation price per deal. Historical rows have actuals; future "
        "rows are predictions only (a source price on a future record is never treated as actual).",
        "main_dttm_col": "loading_date",
        "columns": {
            "record_type": "Record type",
            "split_role": "Split",
            "model_name": "Model",
            "model_version": "Model version",
            "price_id": "Deal ID",
            "loading_date": "Loading date",
            "month": "Month",
            "origin_city": "Origin city",
            "destination_city": "Destination city",
            "body_group": "Body group",
            "weight_category": "Weight category",
            "distance_km": "Distance, km",
            "actual_price_rub": "Actual price, RUB",
            "predicted_price_rub": "Predicted price, RUB",
            "error_rub": "Error, RUB",
            "abs_error_rub": "Absolute error, RUB",
            "pct_error": "Error %",
            "ape_pct": "Absolute error %",
            "ape_band": "Absolute error band",
            "scored_at": "Scored at",
        },
        "metrics": {
            "deals": ("COUNT(*)", "Deals", ",d", ""),
            "avg_actual": ("AVG(actual_price_rub)", "Average actual price, RUB", RUB, ""),
            "avg_predicted": ("AVG(predicted_price_rub)", "Average predicted price, RUB", RUB, ""),
            "median_actual": ("percentile_cont(0.5) WITHIN GROUP (ORDER BY actual_price_rub)",
                              "Median actual price, RUB", RUB, ""),
            "median_predicted": ("percentile_cont(0.5) WITHIN GROUP (ORDER BY predicted_price_rub)",
                                 "Median predicted price, RUB", RUB, ""),
            "mae": ("AVG(abs_error_rub)", "Mean absolute error, RUB", RUB, "Historical rows only."),
            "mape": ("AVG(ape_pct)", "Mean absolute error %", ",.1f", "Historical rows only; percent units."),
            "median_ape": ("percentile_cont(0.5) WITHIN GROUP (ORDER BY ape_pct)", "Median absolute error %",
                           ",.1f", "Historical rows only; percent units."),
            "mean_pct_error": ("AVG(pct_error)", "Mean error % (bias)", ",.1f",
                               "Positive: predictions above actuals."),
        },
    },
}

# ---------------------------------------------------------------------------------------------------------------
# Charts. Each spec yields Superset form data; query_context mirrors it so the chart data API works headless.
# ---------------------------------------------------------------------------------------------------------------

HISTORICAL = {"expressionType": "SIMPLE", "subject": "record_type", "operator": "==", "comparator": "historical",
              "clause": "WHERE"}
FUTURE = {"expressionType": "SIMPLE", "subject": "record_type", "operator": "==", "comparator": "future",
          "clause": "WHERE"}


def big_number(name: str, dataset: str, metric: str, subtitle: str, fmt: str, filters: list | None = None) -> dict:
    return {"name": name, "dataset": dataset, "viz_type": "big_number_total", "metrics": [metric],
            "filters": filters or [],
            "params": {"metric": metric, "subheader": subtitle, "subtitle": subtitle, "y_axis_format": fmt,
                       "header_font_size": 0.4, "subheader_font_size": 0.15, "subtitle_font_size": 0.15}}


def timeseries(name: str, dataset: str, viz_type: str, x_axis: str, grain: str, metrics: list[str], fmt: str,
               filters: list | None = None, time_compare: str | None = None, extra: dict | None = None) -> dict:
    params = {"x_axis": x_axis, "time_grain_sqla": grain, "metrics": metrics, "groupby": [],
              "y_axis_format": fmt, "x_axis_time_format": "%Y-%m" if grain == "P1M" else "smart_date",
              "show_legend": True, "legendOrientation": "top", "rich_tooltip": True, "tooltipTimeFormat": "%Y-%m-%d",
              "truncate_metric": True, "row_limit": 10000, "comparison_type": "values", "time_compare": [],
              "markerEnabled": True, "markerSize": 4, "zoomable": False}
    filters = list(filters or [])
    if time_compare:
        params.update({"time_compare": [time_compare], "comparison_type": "percentage", "y_axis_format": ".1%",
                       "time_range": ALL_TIME})
        filters.append({"expressionType": "SIMPLE", "subject": x_axis, "operator": "TEMPORAL_RANGE",
                        "comparator": ALL_TIME, "clause": "WHERE"})
    params.update(extra or {})
    return {"name": name, "dataset": dataset, "viz_type": viz_type, "metrics": metrics, "filters": filters,
            "x_axis": x_axis, "time_grain": grain, "time_offsets": [time_compare] if time_compare else [],
            "params": params}


def table(name: str, dataset: str, columns: list[str], order_by: list[tuple[str, bool]], formats: dict[str, str],
          filters: list | None = None, page_length: int = 25, extra: dict | None = None) -> dict:
    params = {"query_mode": "raw", "all_columns": columns,
              "order_by_cols": [json.dumps([col, asc]) for col, asc in order_by],
              "server_pagination": True, "server_page_length": page_length, "page_length": page_length,
              "row_limit": TABLE_ROW_LIMIT, "include_search": True, "show_cell_bars": False, "allow_rearrange_columns": True,
              "table_timestamp_format": "%Y-%m-%d",
              "column_config": {col: {"d3NumberFormat": fmt} for col, fmt in formats.items()}}
    params.update(extra or {})
    return {"name": name, "dataset": dataset, "viz_type": "table", "columns": columns, "order_by": order_by,
            "filters": filters or [], "row_limit": page_length, "params": params}


def aggregate_table(name: str, dataset: str, groupby: list[str], metrics: list[str], filters: list | None = None,
                    order_by: list[tuple[str, bool]] | None = None) -> dict:
    params = {"query_mode": "aggregate", "groupby": groupby, "metrics": metrics, "row_limit": 1000,
              "include_search": False, "show_cell_bars": False, "order_desc": True,
              "timeseries_limit_metric": metrics[0] if metrics else None}
    return {"name": name, "dataset": dataset, "viz_type": "table", "groupby": groupby, "metrics": metrics,
            "filters": filters or [], "order_by": order_by or [], "row_limit": 1000, "params": params}


def categorical_bar(name: str, dataset: str, x_axis: str, metrics: list[str], fmt: str,
                    filters: list | None = None, horizontal: bool = False) -> dict:
    params = {"orientation": "horizontal" if horizontal else "vertical", "x_axis": x_axis, "metrics": metrics, "groupby": [], "y_axis_format": fmt, "row_limit": 1000,
              "x_axis_sort": x_axis, "x_axis_sort_asc": True, "show_legend": False, "show_value": True,
              "rich_tooltip": True, "truncate_metric": True}
    return {"name": name, "dataset": dataset, "viz_type": "echarts_timeseries_bar", "metrics": metrics,
            "filters": filters or [], "x_axis": x_axis, "time_grain": None, "time_offsets": [], "params": params}


def heatmap(name: str, dataset: str, x_axis: str, y_axis: str, metric: str, fmt: str) -> dict:
    params = {"x_axis": x_axis, "groupby": y_axis, "metric": metric, "linear_color_scheme": "blue_white_yellow",
              "normalize_across": "heatmap", "show_values": True, "show_legend": True, "y_axis_format": fmt,
              "sort_x_axis": "alpha_asc", "sort_y_axis": "alpha_asc", "xscale_interval": -1, "yscale_interval": -1,
              "left_margin": "auto", "bottom_margin": "auto", "row_limit": 10000, "value_bounds": [None, None]}
    return {"name": name, "dataset": dataset, "viz_type": "heatmap_v2", "metrics": [metric], "filters": [],
            "groupby": [x_axis, y_axis], "row_limit": 10000, "params": params}


def pie(name: str, dataset: str, groupby: str, metric: str) -> dict:
    params = {"groupby": [groupby], "metric": metric, "show_labels": True, "label_type": "key_percent",
              "number_format": ",d", "show_legend": False, "donut": True, "innerRadius": 45, "outerRadius": 75,
              "sort_by_metric": True, "row_limit": 100}
    return {"name": name, "dataset": dataset, "viz_type": "pie", "groupby": [groupby], "metrics": [metric],
            "filters": [], "row_limit": 100, "params": params}


ROUTE_COLUMNS = ["month", "origin_city", "destination_city", "body_group", "weight_category", "p25_trip", "p50_trip",
                 "p75_trip", "p25_km", "p50_km", "p75_km", "mom_pct", "yoy_pct", "trips_count", "carriers_count",
                 "reliability_level"]
FORECAST_COLUMNS = ["loading_date", "origin_city", "destination_city", "body_group", "weight_category",
                    "distance_km", "actual_price_rub", "predicted_price_rub", "abs_error_rub", "pct_error",
                    "model_version"]

CHARTS: dict[str, dict] = {
    # Market overview
    "kpi_trips": big_number("Total completed trips", "v_market_observation", "completed_trips",
                            "distinct deals", ",d"),
    "kpi_carriers": big_number("Unique carriers", "v_market_observation", "unique_carriers",
                               "carrier organizations", ",d"),
    "kpi_median": big_number("Median market rate", "v_market_observation", "median_rate", "RUB per trip", RUB),
    "kpi_avg": big_number("Average market rate", "v_market_observation", "avg_rate", "RUB per trip", RUB),
    "kpi_km": big_number("Average price per kilometer", "v_market_observation", "avg_price_km", "RUB per km", RUB_KM),
    "kpi_reliable": big_number("Market data reliability", "v_market_observation", "reliable_share",
                               "in HIGH / MEDIUM cells", ".1%"),
    "overview_reliability": pie("Observations by reliability level", "v_market_observation", "reliability_level",
                                "observations"),
    "overview_trend": timeseries("Monthly median market rate", "v_market_observation", "echarts_timeseries_line",
                                 "price_month", "P1M", ["median_rate"], RUB),
    "overview_body": categorical_bar("Median rate by body group", "v_market_observation", "body_group",
                                     ["median_rate"], RUB, horizontal=True),
    # Route pricing
    "route_table": table(
        "Route pricing", "v_route_pricing", ROUTE_COLUMNS,
        [("month", False), ("trips_count", False)],
        {"p25_trip": RUB, "p50_trip": RUB, "p75_trip": RUB, "p25_km": RUB_KM, "p50_km": RUB_KM, "p75_km": RUB_KM,
         "mom_pct": "+,.1f", "yoy_pct": "+,.1f", "trips_count": ",d", "carriers_count": ",d"},
        extra={"conditional_formatting": [
            {"column": "mom_pct", "operator": ">", "targetValue": 0, "colorScheme": "#E04355"},
            {"column": "mom_pct", "operator": "<", "targetValue": 0, "colorScheme": "#5AC189"},
            {"column": "yoy_pct", "operator": ">", "targetValue": 0, "colorScheme": "#E04355"},
            {"column": "yoy_pct", "operator": "<", "targetValue": 0, "colorScheme": "#5AC189"},
        ]}),
    # Market dynamics
    "dyn_median": timeseries("Monthly median rate", "v_market_observation", "echarts_timeseries_line",
                             "price_month", "P1M", ["median_rate"], RUB),
    "dyn_corridor": timeseries("Price corridor P25 / P50 / P75", "v_market_observation", "echarts_timeseries_line",
                               "price_month", "P1M", ["p25_rate", "median_rate", "p75_rate"], RUB),
    "dyn_mom": timeseries("Median rate, month over month", "v_market_observation", "echarts_timeseries_bar",
                          "price_month", "P1M", ["median_rate"], ".1%", time_compare="1 month ago"),
    "dyn_yoy": timeseries("Median rate, year over year", "v_market_observation", "echarts_timeseries_bar",
                          "price_month", "P1M", ["median_rate"], ".1%", time_compare="1 year ago"),
    "dyn_season": heatmap("Seasonality: median rate by month of year", "v_market_observation", "month_of_year",
                          "year", "median_rate", RUB),
    # Fact vs forecast
    "fc_mae": big_number("Mean absolute error", "v_fact_vs_forecast", "mae", "RUB, historical deals", RUB,
                         [HISTORICAL]),
    "fc_median_ape": big_number("Median absolute error %", "v_fact_vs_forecast", "median_ape",
                                "percent, historical deals", ",.1f", [HISTORICAL]),
    "fc_hist_count": big_number("Historical deals scored", "v_fact_vs_forecast", "deals", "with actual price", ",d",
                                [HISTORICAL]),
    "fc_future_count": big_number("Future deals predicted", "v_fact_vs_forecast", "deals", "no actual price yet",
                                  ",d", [FUTURE]),
    "fc_models": aggregate_table("Model versions", "v_fact_vs_forecast", ["model_name", "model_version",
                                 "record_type"], ["deals"], order_by=[("deals", False)]),
    "fc_hist_trend": timeseries("Historical: actual vs predicted (daily average)", "v_fact_vs_forecast",
                                "echarts_timeseries_line", "loading_date", "P1D", ["avg_actual", "avg_predicted"],
                                RUB, [HISTORICAL]),
    "fc_future_trend": timeseries("Future: predicted price (weekly average)", "v_fact_vs_forecast",
                                  "echarts_timeseries_line", "loading_date", "P1W", ["avg_predicted"], RUB,
                                  [FUTURE]),
    "fc_ape_band": categorical_bar("Historical deals by absolute error band", "v_fact_vs_forecast", "ape_band",
                                   ["deals"], ",d", [HISTORICAL]),
    "fc_hist_table": table("Historical deals: actual vs predicted", "v_fact_vs_forecast", FORECAST_COLUMNS,
                           [("loading_date", False), ("abs_error_rub", False)],
                           {"distance_km": ",.0f", "actual_price_rub": RUB, "predicted_price_rub": RUB,
                            "abs_error_rub": RUB, "pct_error": "+,.1f"}, [HISTORICAL], page_length=20),
    "fc_future_table": table("Future deals: predicted price", "v_fact_vs_forecast",
                             ["loading_date", "origin_city", "destination_city", "body_group", "weight_category",
                              "distance_km", "predicted_price_rub", "model_version"],
                             [("loading_date", True)], {"distance_km": ",.0f", "predicted_price_rub": RUB},
                             [FUTURE], page_length=20),
}

HELP = {
    "overview": """### Market overview
Snapshot of the transport market in the warehouse. Use the filter bar on the left to narrow by month, route,
body group, weight category, or reliability.

* **Completed trips** counts distinct deals; **observations** count a deal once per month and body group,
  the unit behind every median.
* **Market data reliability**: a route cell is **HIGH** with 30+ trips, 5+ carriers, and a price within
  30 days; **MEDIUM** with 10+ trips; otherwise **LOW_DATA**. Treat LOW_DATA prices as indicative only.
""",
    "route": """### Route pricing
One row per month, origin, destination, body group, and weight category. **P25 / P50 / P75** bound the
typical price: half of the trips fall between P25 and P75. **MoM % / YoY %** compare the cell's median with
the previous month and the same month last year (red: more expensive, green: cheaper).

Weight categories: **L5T** small loads, **5T**, **10T**, **FTL** full truckload, **G20T** heavy loads above
20 t, **OTH** other or unknown. Search the table, or sort by clicking a column header.
""",
    "dynamics": """### Market dynamics
Market-level medians over the filtered trips. The corridor shows where the middle half of prices sit each
month. Month-over-month and year-over-year bars compare each month's median with the month before and the
same month a year earlier; months without a comparison are empty. This tab always shows the full history:
the Month filter does not apply here, the other filters do.
""",
    "forecast": """### Fact vs forecast
**Historical** deals have a realized price, so the error is known: absolute error in RUB and percent
(error % above zero means the model predicted too high). **Future** deals have predictions only; a price on a
future record is never treated as the realized price. The reliability filter does not apply to this tab.
""",
}

TABS = [
    ("overview", "Market Overview", [
        [("kpi_trips", 2, 24), ("kpi_carriers", 2, 24), ("kpi_median", 2, 24), ("kpi_avg", 2, 24),
         ("kpi_km", 2, 24), ("kpi_reliable", 2, 24)],
        [("overview_trend", 6, 60), ("overview_body", 3, 60), ("overview_reliability", 3, 60)],
    ]),
    ("route", "Route Pricing", [[("route_table", 12, 110)]]),
    ("dynamics", "Market Dynamics", [
        [("dyn_median", 6, 55), ("dyn_corridor", 6, 55)],
        [("dyn_mom", 6, 50), ("dyn_yoy", 6, 50)],
        [("dyn_season", 12, 60)],
    ]),
    ("forecast", "Fact vs Forecast", [
        [("fc_mae", 2, 26), ("fc_median_ape", 2, 26), ("fc_hist_count", 2, 26), ("fc_future_count", 2, 26),
         ("fc_models", 4, 26)],
        [("fc_hist_trend", 8, 55), ("fc_ape_band", 4, 55)],
        [("fc_hist_table", 12, 70)],
        [("fc_future_trend", 5, 60), ("fc_future_table", 7, 60)],
    ]),
]

# (key, label, column, multi-select, sort ascending, parent filter key, excluded tabs)
FILTERS = [
    ("month", "Month", "month", True, False, None, ("dynamics",)),
    ("origin", "Origin city", "origin_city", True, True, None, ()),
    ("destination", "Destination city", "destination_city", True, True, "origin", ()),
    ("body", "Body group", "body_group", True, True, None, ()),
    ("weight", "Weight category", "weight_category", True, True, None, ()),
    ("reliability", "Reliability", "reliability_level", True, True, None, ("forecast",)),
]


# ---------------------------------------------------------------------------------------------------------------
# Query context
# ---------------------------------------------------------------------------------------------------------------


def query_filters(adhoc: list[dict]) -> list[dict]:
    return [{"col": f["subject"], "op": f["operator"], "val": f["comparator"]} for f in adhoc]


def query_context(spec: dict, dataset_id: int, form_data: dict) -> dict:
    query: dict[str, Any] = {
        "filters": query_filters(spec["filters"]),
        "extras": {"having": "", "where": ""},
        "applied_time_extras": {},
        "columns": [],
        "metrics": [],
        "orderby": [],
        "annotation_layers": [],
        "row_limit": spec.get("row_limit", 10000),
        "series_limit": 0,
        "order_desc": True,
        "url_params": {},
        "custom_params": {},
        "custom_form_data": {},
    }
    viz = spec["viz_type"]
    if viz == "table" and spec.get("columns"):
        query["columns"] = spec["columns"]
        query["orderby"] = [[col, asc] for col, asc in spec["order_by"]]
    elif viz == "table":
        query["columns"] = spec["groupby"]
        query["metrics"] = spec["metrics"]
        query["orderby"] = [[col, asc] for col, asc in spec["order_by"]]
    elif "x_axis" in spec:
        axis: dict[str, Any] = {"columnType": "BASE_AXIS", "sqlExpression": spec["x_axis"], "label": spec["x_axis"],
                                "expressionType": "SQL"}
        if spec["time_grain"]:
            axis["timeGrain"] = spec["time_grain"]
        query["columns"] = [axis]
        query["metrics"] = spec["metrics"]
        query["orderby"] = [[spec["metrics"][0], False]]
        query["time_offsets"] = spec["time_offsets"]
    elif spec.get("groupby"):
        query["columns"] = spec["groupby"]
        query["metrics"] = spec["metrics"]
        query["orderby"] = [[spec["metrics"][0], False]]
    else:
        query["metrics"] = spec["metrics"]
    return {"datasource": {"id": dataset_id, "type": "table"}, "force": False, "queries": [query],
            "form_data": form_data, "result_format": "json", "result_type": "full"}


# ---------------------------------------------------------------------------------------------------------------
# REST client
# ---------------------------------------------------------------------------------------------------------------


class Superset:
    def __init__(self, url: str, username: str, password: str):
        import requests  # Superset's image has it; the platform tests import this module without it.

        self.url = url.rstrip("/")
        self.session = requests.Session()
        self.session.headers["Referer"] = self.url
        token = self._call("POST", "/api/v1/security/login",
                           json={"username": username, "password": password, "provider": "db", "refresh": True})
        self.session.headers["Authorization"] = f"Bearer {token['access_token']}"
        self.session.headers["X-CSRFToken"] = self._call("GET", "/api/v1/security/csrf_token/")["result"]

    def _call(self, method: str, path: str, **kwargs: Any) -> dict:
        response = self.session.request(method, self.url + path, timeout=300, **kwargs)
        if response.status_code >= 400:
            raise RuntimeError(f"{method} {path} -> {response.status_code}: {response.text[:2000]}")
        return response.json() if response.content else {}

    def get(self, path: str) -> dict:
        return self._call("GET", path)

    def post(self, path: str, body: dict) -> dict:
        return self._call("POST", path, json=body)

    def put(self, path: str, body: dict) -> dict:
        return self._call("PUT", path, json=body)

    def find(self, resource: str, column: str, value: str) -> dict | None:
        escaped = value.replace("!", "!!").replace("'", "!'")
        rison = f"(filters:!((col:{column},opr:eq,value:'{escaped}')),page_size:100)"
        rows = self.get(f"/api/v1/{resource}/?q={quote(rison)}")["result"]
        return rows[0] if rows else None


def wait_for(url: str, timeout_s: int) -> None:
    import requests

    deadline = time.monotonic() + timeout_s
    while True:
        try:
            if requests.get(f"{url.rstrip('/')}/health", timeout=5).status_code == 200:
                return
        except requests.RequestException:
            pass
        if time.monotonic() > deadline:
            raise RuntimeError(f"Superset at {url} is not healthy after {timeout_s}s")
        time.sleep(3)


# ---------------------------------------------------------------------------------------------------------------
# Provisioning steps
# ---------------------------------------------------------------------------------------------------------------


def ensure_database(api: Superset, env: dict[str, str]) -> int:
    uri = (f"postgresql+psycopg2://{quote(env['BI_DB_USER'], safe='')}:{quote(env['BI_DB_PASSWORD'], safe='')}"
           f"@{env['BI_DB_HOST']}:{env['BI_DB_PORT']}/{env['DWH_DB_NAME']}")
    body = {"database_name": DATABASE_NAME, "sqlalchemy_uri": uri, "expose_in_sqllab": True, "allow_dml": False,
            "allow_ctas": False, "allow_cvas": False, "allow_file_upload": False, "allow_run_async": False,
            "cache_timeout": 3600,
            "extra": json.dumps({"schemas_allowed_for_file_upload": [], "allows_virtual_table_explore": True,
                                 "metadata_cache_timeout": {"schema_cache_timeout": 3600,
                                                            "table_cache_timeout": 3600}})}
    existing = api.find("database", "database_name", DATABASE_NAME)
    if existing:
        api.put(f"/api/v1/database/{existing['id']}", body)
        log.info("database updated: id=%s", existing["id"])
        return existing["id"]
    created = api.post("/api/v1/database/", body)
    log.info("database created: id=%s", created["id"])
    return created["id"]


def ensure_dataset(api: Superset, database_id: int, table_name: str, spec: dict) -> int:
    rison = (f"(filters:!((col:table_name,opr:eq,value:'{table_name}'),(col:schema,opr:eq,value:'{SCHEMA}')),"
             "page_size:100)")
    rows = [r for r in api.get(f"/api/v1/dataset/?q={quote(rison)}")["result"]
            if r["database"]["id"] == database_id]
    if rows:
        dataset_id = rows[0]["id"]
        api.put(f"/api/v1/dataset/{dataset_id}/refresh", {})
    else:
        dataset_id = api.post("/api/v1/dataset/", {"database": database_id, "schema": SCHEMA,
                                                    "table_name": table_name})["id"]
    current = api.get(f"/api/v1/dataset/{dataset_id}")["result"]
    columns = []
    for col in current["columns"]:
        name = col["column_name"]
        if name not in spec["columns"]:
            raise RuntimeError(f"{table_name}.{name} has no verbose name in provision.DATASETS")
        columns.append({"id": col["id"], "column_name": name, "verbose_name": spec["columns"][name],
                        "type": col.get("type"), "is_dttm": name == spec["main_dttm_col"] or bool(col.get("is_dttm")),
                        "filterable": True, "groupby": True, "is_active": True})
    missing = set(spec["columns"]) - {c["column_name"] for c in columns}
    if missing:
        raise RuntimeError(f"{table_name} lacks columns {sorted(missing)}; run python -m etl.migrate")
    metric_ids = {m["metric_name"]: m["id"] for m in current["metrics"]}
    metrics = []
    for name, (expression, verbose, fmt, description) in spec["metrics"].items():
        metric = {"metric_name": name, "expression": expression, "verbose_name": verbose, "d3format": fmt,
                  "description": description or None, "metric_type": None}
        if name in metric_ids:
            metric["id"] = metric_ids[name]
        metrics.append(metric)
    api.put(f"/api/v1/dataset/{dataset_id}?override_columns=false", {
        "description": spec["description"], "main_dttm_col": spec["main_dttm_col"], "cache_timeout": 3600,
        "columns": columns, "metrics": metrics})
    log.info("dataset %s.%s: id=%s, %d columns, %d metrics", SCHEMA, table_name, dataset_id, len(columns),
             len(metrics))
    return dataset_id


def ensure_dashboard(api: Superset) -> int:
    existing = api.find("dashboard", "slug", DASHBOARD_SLUG)
    if existing:
        return existing["id"]
    created = api.post("/api/v1/dashboard/", {"dashboard_title": DASHBOARD_TITLE, "slug": DASHBOARD_SLUG,
                                              "published": True})
    log.info("dashboard created: id=%s", created["id"])
    return created["id"]


def ensure_chart(api: Superset, key: str, spec: dict, dataset_id: int, dashboard_id: int) -> int:
    form_data = {"datasource": f"{dataset_id}__table", "viz_type": spec["viz_type"],
                 "adhoc_filters": spec["filters"], **spec["params"]}
    body = {"slice_name": spec["name"], "viz_type": spec["viz_type"], "datasource_id": dataset_id,
            "datasource_type": "table", "params": json.dumps(form_data),
            "query_context": json.dumps(query_context(spec, dataset_id, form_data)),
            "query_context_generation": False, "dashboards": [dashboard_id], "cache_timeout": 3600,
            "description": f"Smart Logistics dashboard chart '{key}'."}
    rison = (f"(filters:!((col:slice_name,opr:eq,value:'{spec['name']}'),"
             f"(col:dashboards,opr:rel_m_m,value:{dashboard_id})),page_size:100)")
    rows = api.get(f"/api/v1/chart/?q={quote(rison)}")["result"]
    if rows:
        chart_id = rows[0]["id"]
        api.put(f"/api/v1/chart/{chart_id}", body)
    else:
        chart_id = api.post("/api/v1/chart/", body)["id"]
    form_data["slice_id"] = chart_id
    api.put(f"/api/v1/chart/{chart_id}", {"params": json.dumps(form_data)})
    return chart_id


def layout(chart_ids: dict[str, int]) -> tuple[dict, dict[str, list[int]]]:
    position: dict[str, Any] = {
        "DASHBOARD_VERSION_KEY": "v2",
        "ROOT_ID": {"type": "ROOT", "id": "ROOT_ID", "children": ["TABS-main"]},
        "HEADER_ID": {"type": "HEADER", "id": "HEADER_ID", "meta": {"text": DASHBOARD_TITLE}},
        "TABS-main": {"type": "TABS", "id": "TABS-main", "children": [], "parents": ["ROOT_ID"], "meta": {}},
    }
    tab_charts: dict[str, list[int]] = {}
    for tab_key, title, rows in TABS:
        tab_id = f"TAB-{tab_key}"
        parents = ["ROOT_ID", "TABS-main", tab_id]
        position["TABS-main"]["children"].append(tab_id)
        position[tab_id] = {"type": "TAB", "id": tab_id, "children": [], "parents": ["ROOT_ID", "TABS-main"],
                            "meta": {"text": title, "defaultText": "Tab title", "placeholder": "Tab title"}}
        help_row = f"ROW-{tab_key}-help"
        position[tab_id]["children"].append(help_row)
        position[help_row] = {"type": "ROW", "id": help_row, "children": [f"MARKDOWN-{tab_key}"], "parents": parents,
                              "meta": {"background": "BACKGROUND_TRANSPARENT"}}
        position[f"MARKDOWN-{tab_key}"] = {"type": "MARKDOWN", "id": f"MARKDOWN-{tab_key}", "children": [],
                                           "parents": [*parents, help_row],
                                           "meta": {"code": HELP[tab_key], "width": 12, "height": 20}}
        tab_charts[tab_key] = []
        for index, row in enumerate(rows):
            row_id = f"ROW-{tab_key}-{index}"
            position[tab_id]["children"].append(row_id)
            position[row_id] = {"type": "ROW", "id": row_id, "children": [], "parents": parents,
                                "meta": {"background": "BACKGROUND_TRANSPARENT"}}
            for key, width, height in row:
                chart_id = chart_ids[key]
                node_id = f"CHART-{key}"
                position[row_id]["children"].append(node_id)
                position[node_id] = {"type": "CHART", "id": node_id, "children": [], "parents": [*parents, row_id],
                                     "meta": {"chartId": chart_id, "width": width, "height": height,
                                              "sliceName": CHARTS[key]["name"]}}
                tab_charts[tab_key].append(chart_id)
    return position, tab_charts


def native_filters(source_dataset_id: int, tab_charts: dict[str, list[int]]) -> list[dict]:
    all_charts = [c for charts in tab_charts.values() for c in charts]
    configs = []
    for key, label, column, multi, ascending, parent, excluded_tabs in FILTERS:
        excluded = [c for tab in excluded_tabs for c in tab_charts[tab]]
        configs.append({
            "id": f"NATIVE_FILTER-{key}",
            "name": label,
            "filterType": "filter_select",
            "type": "NATIVE_FILTER",
            "description": "",
            "targets": [{"datasetId": source_dataset_id, "column": {"name": column}}],
            "controlValues": {"enableEmptyFilter": False, "defaultToFirstItem": False, "multiSelect": multi,
                              "searchAllOptions": False, "inverseSelection": False, "creatable": False},
            "defaultDataMask": {"extraFormData": {}, "filterState": {}, "ownState": {}},
            "cascadeParentIds": [f"NATIVE_FILTER-{parent}"] if parent else [],
            "scope": {"rootPath": ["ROOT_ID"], "excluded": excluded},
            "chartsInScope": [c for c in all_charts if c not in excluded],
            "tabsInScope": [f"TAB-{tab}" for tab in tab_charts if tab not in excluded_tabs],
            "sortAscending": ascending,
            "requiredFirst": False,
        })
    return configs


def provision(env: dict[str, str]) -> dict[str, Any]:
    wait_for(env["SUPERSET_URL"], int(env.get("SUPERSET_WAIT_S", "300")))
    api = Superset(env["SUPERSET_URL"], env["SUPERSET_ADMIN_USERNAME"], env["SUPERSET_ADMIN_PASSWORD"])
    database_id = ensure_database(api, env)
    dataset_ids = {name: ensure_dataset(api, database_id, name, spec) for name, spec in DATASETS.items()}
    dashboard_id = ensure_dashboard(api)
    chart_ids = {key: ensure_chart(api, key, spec, dataset_ids[spec["dataset"]], dashboard_id)
                 for key, spec in CHARTS.items()}
    position, tab_charts = layout(chart_ids)
    metadata = {
        "native_filter_configuration": native_filters(dataset_ids["v_route_pricing"], tab_charts),
        "chart_configuration": {},
        "cross_filters_enabled": False,
        "color_scheme": "supersetColors",
        "refresh_frequency": 0,
        "timed_refresh_immune_slices": [],
        "expanded_slices": {},
        "label_colors": {"HIGH": "#5AC189", "MEDIUM": "#FCC700", "LOW_DATA": "#E04355"},
        "shared_label_colors": [],
        "default_filters": "{}",
    }
    api.put(f"/api/v1/dashboard/{dashboard_id}", {
        "dashboard_title": DASHBOARD_TITLE, "slug": DASHBOARD_SLUG, "published": True,
        "position_json": json.dumps(position), "json_metadata": json.dumps(metadata),
        "css": ".dashboard-markdown p, .dashboard-markdown li { font-size: 13px; }"})
    log.info("dashboard %r: id=%s, %d charts, %d filters", DASHBOARD_TITLE, dashboard_id, len(chart_ids),
             len(FILTERS))
    return {"database_id": database_id, "dataset_ids": dataset_ids, "dashboard_id": dashboard_id,
            "chart_ids": chart_ids, "url": f"{env['SUPERSET_URL'].rstrip('/')}/superset/dashboard/{DASHBOARD_SLUG}/"}


REQUIRED_ENV = ("SUPERSET_URL", "SUPERSET_ADMIN_USERNAME", "SUPERSET_ADMIN_PASSWORD", "BI_DB_HOST", "BI_DB_PORT",
                "DWH_DB_NAME", "BI_DB_USER", "BI_DB_PASSWORD")


def main() -> int:
    logging.basicConfig(level=os.environ.get("LOG_LEVEL", "INFO"), format="%(asctime)s %(levelname)s %(message)s")
    missing = [name for name in REQUIRED_ENV if not os.environ.get(name)]
    if missing:
        log.error("missing environment variables: %s", ", ".join(missing))
        return 2
    result = provision(dict(os.environ))
    print(json.dumps(result, indent=2))
    return 0


if __name__ == "__main__":
    sys.exit(main())
