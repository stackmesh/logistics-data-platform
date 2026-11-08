"""BI layer for Superset: the mart.v_* views, the bi_reader role, and the dashboard specification."""

from __future__ import annotations

import importlib.util
import json
import os
from datetime import date
from pathlib import Path

import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.exc import DBAPIError

from etl.mart.refresh import refresh_market_corridor

SNAPSHOT_AS_OF = date(2026, 10, 26)
FILTER_COLUMNS = ("month", "origin_city", "destination_city", "body_group", "weight_category")


def _load_provision():
    path = Path(__file__).resolve().parents[1] / "superset" / "provision.py"
    spec = importlib.util.spec_from_file_location("superset_provision", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


provision = _load_provision()


# --- Dashboard specification (no database) ------------------------------------------------------


def test_every_chart_uses_known_dataset_metrics_and_columns():
    for key, chart in provision.CHARTS.items():
        dataset = provision.DATASETS[chart["dataset"]]
        for metric in chart.get("metrics", []):
            assert metric in dataset["metrics"], (key, metric)
        referenced = [*chart.get("columns", []), *chart.get("groupby", []),
                      *[f["subject"] for f in chart["filters"]]]
        if chart.get("x_axis"):
            referenced.append(chart["x_axis"])
        for column in referenced:
            assert column in dataset["columns"], (key, column)


def test_filter_columns_exist_in_every_dataset_they_reach():
    tab_datasets = {tab: {provision.CHARTS[key]["dataset"] for row in rows for key, _, _ in row}
                    for tab, _, rows in provision.TABS}
    for _, label, column, _, _, _, excluded_tabs in provision.FILTERS:
        for tab, datasets in tab_datasets.items():
            if tab in excluded_tabs:
                continue
            for dataset in datasets:
                assert column in provision.DATASETS[dataset]["columns"], (label, tab, dataset)


def test_layout_places_every_chart_once_and_rows_fit_the_grid():
    placed = [key for _, _, rows in provision.TABS for row in rows for key, _, _ in row]
    assert sorted(placed) == sorted(provision.CHARTS)
    for _, _, rows in provision.TABS:
        for row in rows:
            assert sum(width for _, width, _ in row) <= 12


def test_layout_and_filters_reference_real_chart_ids():
    chart_ids = {key: index for index, key in enumerate(provision.CHARTS, start=100)}
    position, tab_charts = provision.layout(chart_ids)
    in_layout = {node["meta"]["chartId"] for node in position.values()
                 if isinstance(node, dict) and node.get("type") == "CHART"}
    assert in_layout == set(chart_ids.values())
    for node_id, node in position.items():
        if isinstance(node, dict):
            for child in node.get("children", []):
                assert child in position, (node_id, child)
    filters = {f["name"]: f for f in provision.native_filters(7, tab_charts)}
    assert set(filters) == {"Month", "Origin city", "Destination city", "Body group", "Weight category",
                            "Reliability"}
    assert set(filters["Reliability"]["scope"]["excluded"]) == set(tab_charts["forecast"])
    assert set(filters["Month"]["scope"]["excluded"]) == set(tab_charts["dynamics"])
    assert filters["Destination city"]["cascadeParentIds"] == ["NATIVE_FILTER-origin"]


def test_query_context_mirrors_chart_spec():
    for key, chart in provision.CHARTS.items():
        form_data = {"viz_type": chart["viz_type"], **chart["params"]}
        context = provision.query_context(chart, 1, form_data)
        query = context["queries"][0]
        assert query["metrics"] == chart.get("metrics", []) or chart["viz_type"] == "table", key
        assert len(query["filters"]) == len(chart["filters"]), key
        json.dumps(context)
    mom = provision.query_context(provision.CHARTS["dyn_mom"], 1, {})["queries"][0]
    assert mom["time_offsets"] == ["1 month ago"]
    assert {"col": "price_month", "op": "TEMPORAL_RANGE", "val": provision.ALL_TIME} in mom["filters"]


def test_forecast_charts_split_historical_and_future():
    for key in ("fc_mae", "fc_median_ape", "fc_hist_count", "fc_hist_trend", "fc_ape_band", "fc_hist_table"):
        assert provision.HISTORICAL in provision.CHARTS[key]["filters"], key
    for key in ("fc_future_count", "fc_future_trend", "fc_future_table"):
        assert provision.FUTURE in provision.CHARTS[key]["filters"], key
    assert "actual_price_rub" not in provision.CHARTS["fc_future_table"]["columns"]


# --- BI views (warehouse) -----------------------------------------------------------------------


@pytest.fixture(scope="module")
def mart_ready(loaded_warehouse, dwh_engine):
    return refresh_market_corridor(dwh_engine, SNAPSHOT_AS_OF)


@pytest.mark.integration
def test_observation_view_reproduces_every_mart_cell(mart_ready, dwh_engine):
    with dwh_engine.connect() as conn:
        row = conn.execute(text("""
            WITH cell AS (
                SELECT price_month, origin_city, destination_city, body_group, weight_category,
                       count(*) AS trips, count(DISTINCT carrier_id) AS carriers,
                       percentile_cont(0.5) WITHIN GROUP (ORDER BY price_rub) AS median_trip,
                       percentile_cont(0.25) WITHIN GROUP (ORDER BY price_km) AS p25_km
                FROM mart.v_market_observation GROUP BY 1, 2, 3, 4, 5)
            SELECT (SELECT count(*) FROM mart.mart_market_corridor) AS mart_cells, count(*) AS matched,
                   count(*) FILTER (WHERE m.trips_count <> c.trips OR m.carriers_count <> c.carriers
                                    OR abs(m.p50_trip - c.median_trip::numeric) > 0.01
                                    OR abs(m.p25_km - c.p25_km::numeric) > 0.01) AS mismatched
            FROM cell AS c
            JOIN mart.v_route_pricing AS m USING (price_month, origin_city, destination_city, body_group,
                                                  weight_category)
        """)).one()
    assert row.mart_cells > 0
    assert row.matched == row.mart_cells
    assert row.mismatched == 0


@pytest.mark.integration
def test_route_pricing_is_the_mart_renamed(mart_ready, dwh_engine):
    with dwh_engine.connect() as conn:
        differing = conn.execute(text("""
            SELECT count(*) FROM mart.mart_market_corridor AS m
            FULL JOIN mart.v_route_pricing AS v
              ON (v.price_month, v.origin_city, v.destination_city, v.body_group, v.weight_category)
               = (m.price_month, m.origin_city_name, m.destination_city_name, m.body_group_name, m.weight_category)
            WHERE v.price_month IS NULL OR m.price_month IS NULL
               OR (v.p25_trip, v.p50_trip, v.p75_trip, v.p25_km, v.p50_km, v.p75_km, v.trips_count,
                   v.carriers_count, v.reliability_level)
                  IS DISTINCT FROM (m.p25_trip, m.median_trip, m.p75_trip, m.p25_km, m.median_km, m.p75_km,
                                    m.trips_count, m.carriers_count, m.reliability_level)
               OR v.mom_pct IS DISTINCT FROM m.mom_pct OR v.yoy_pct IS DISTINCT FROM m.yoy_pct
        """)).scalar_one()
    assert differing == 0


@pytest.mark.integration
def test_fact_vs_forecast_view_matches_ml_view(dwh_engine):
    with dwh_engine.connect() as conn:
        row = conn.execute(text("""
            SELECT (SELECT count(*) FROM ml.v_fact_vs_forecast) AS ml_rows,
                   count(*) AS bi_rows,
                   count(*) FILTER (WHERE record_type = 'future' AND actual_price_rub IS NOT NULL) AS future_actuals,
                   count(*) FILTER (WHERE record_type = 'historical' AND abs_error_rub IS NULL) AS hist_no_error
            FROM mart.v_fact_vs_forecast
        """)).one()
    assert row.bi_rows == row.ml_rows
    assert row.future_actuals == 0
    assert row.hist_no_error == 0


@pytest.mark.integration
@pytest.mark.parametrize("view", ["v_market_observation", "v_route_pricing", "v_fact_vs_forecast"])
def test_bi_views_share_filter_column_names(dwh_engine, view):
    with dwh_engine.connect() as conn:
        columns = set(conn.execute(text("""
            SELECT column_name FROM information_schema.columns WHERE table_schema = 'mart' AND table_name = :v
        """), {"v": view}).scalars())
    assert set(FILTER_COLUMNS) <= columns
    assert ("reliability_level" in columns) == (view != "v_fact_vs_forecast")


# --- bi_reader role -----------------------------------------------------------------------------


@pytest.fixture(scope="module")
def bi_engine(dwh_engine):
    password = os.environ.get("BI_DB_PASSWORD")
    with dwh_engine.connect() as conn:
        exists = conn.execute(text("SELECT 1 FROM pg_roles WHERE rolname = 'bi_reader'")).first()
    if not exists or not password:
        pytest.skip("bi_reader role or BI_DB_PASSWORD not available (docker/postgres/03_bi_reader.sh)")
    engine = create_engine(dwh_engine.url.set(username="bi_reader", password=password))
    yield engine
    engine.dispose()


@pytest.mark.integration
def test_bi_reader_reads_bi_views(mart_ready, bi_engine):
    with bi_engine.connect() as conn:
        for view in ("v_market_observation", "v_route_pricing", "v_fact_vs_forecast"):
            conn.execute(text(f"SELECT * FROM mart.{view} LIMIT 1")).all()


@pytest.mark.integration
@pytest.mark.parametrize("statement", [
    "SELECT 1 FROM core.fact_deal LIMIT 1",
    "SELECT 1 FROM ml.ml_price_predictions LIMIT 1",
    "CREATE TABLE mart.bi_reader_probe (i int)",
    "DELETE FROM mart.mart_market_corridor",
])
def test_bi_reader_cannot_read_core_or_write(bi_engine, statement):
    with bi_engine.connect() as conn, pytest.raises(DBAPIError):
        conn.execute(text(statement))
