"""Feature dataset built from the real warehouse."""

from __future__ import annotations

import json
from datetime import date

import polars as pl
import pytest
from sqlalchemy import text

from ml.features.dataset import build_dataset, frame_digest, write_dataset
from ml.features.spec import DEFAULT_SPLIT, FEATURE_COLUMNS, FORBIDDEN_COLUMNS, MISSING, TARGET, UNKNOWN

pytestmark = pytest.mark.integration


@pytest.fixture(scope="module")
def dataset(loaded_warehouse, dwh_engine):
    with dwh_engine.connect() as conn:
        return build_dataset(conn)


def _scalar(conn, sql: str, **params):
    return conn.execute(text(sql), params).scalar_one()


ELIGIBLE = "is_fact AND NOT complex AND NOT outlier_mark AND price_rub > 0 AND distance_km > 0"


def test_row_counts_match_an_independent_count(dataset, dwh_tx):
    train = _scalar(dwh_tx, f"SELECT count(*) FROM core.fact_deal WHERE {ELIGIBLE} "
                            "AND loading_date >= DATE '2026-07-26' AND loading_date < DATE '2026-10-05'")
    test = _scalar(dwh_tx, f"SELECT count(*) FROM core.fact_deal WHERE {ELIGIBLE} "
                           "AND loading_date BETWEEN DATE '2026-10-05' AND DATE '2026-10-26'")
    assert (dataset.X["train"].height, dataset.X["test"].height) == (train, test)
    assert train > 0 and test > 0


def test_boundary_days_land_in_the_right_split(dataset, dwh_tx):
    def ids_on(day):
        return set(dwh_tx.execute(text(f"SELECT price_id FROM core.fact_deal WHERE {ELIGIBLE} AND loading_date = :d"),
                                  {"d": day}).scalars())

    train_ids = set(dataset.ids["train"]["price_id"].to_list())
    test_ids = set(dataset.ids["test"]["price_id"].to_list())
    for day, expected in ((date(2026, 7, 26), train_ids), (date(2026, 10, 4), train_ids),
                          (date(2026, 10, 5), test_ids), (date(2026, 10, 26), test_ids)):
        on_day = ids_on(day)
        assert on_day, day
        assert on_day <= expected, day
    for day in (date(2026, 7, 25), date(2026, 10, 27)):
        assert ids_on(day).isdisjoint(train_ids | test_ids), day
    assert ids_on(date(2026, 10, 5)).isdisjoint(train_ids)


def test_filtered_deals_are_absent(dataset, dwh_tx):
    included = set(dataset.ids["train"]["price_id"].to_list()) | set(dataset.ids["test"]["price_id"].to_list())
    for predicate in ("outlier_mark", "complex", "NOT is_fact"):
        excluded = set(dwh_tx.execute(text(
            f"SELECT price_id FROM core.fact_deal WHERE {predicate} "
            "AND loading_date BETWEEN DATE '2026-07-26' AND DATE '2026-10-26'"
        )).scalars())
        assert excluded, predicate
        assert excluded.isdisjoint(included), predicate


def test_target_is_separate_and_equals_core(dataset, dwh_tx):
    core = pl.DataFrame(
        [tuple(r) for r in dwh_tx.execute(text(
            "SELECT price_id, price_rub::float8, body_group_code, loading_city_gcid FROM core.fact_deal "
            "WHERE loading_date BETWEEN DATE '2026-07-26' AND DATE '2026-10-26'"))],
        schema=["price_id", "core_price_rub", "core_body_group", "loading_city_gcid"], orient="row",
    )
    for part in ("train", "test"):
        X, y, ids = dataset.X[part], dataset.y[part], dataset.ids[part]
        assert tuple(X.columns) == FEATURE_COLUMNS
        assert not set(X.columns) & (FORBIDDEN_COLUMNS | {TARGET, "price_id", "loading_date"})
        assert y.name == TARGET
        joined = ids.with_columns(y, X["body_group"]).join(core, on="price_id", how="left")
        assert joined["core_price_rub"].null_count() == 0
        assert (joined[TARGET] == joined["core_price_rub"]).all()
        assert (joined["body_group"] == joined["core_body_group"]).all()


def test_geography_matches_the_deduplicated_city_dimension(dataset, dwh_tx):
    cities = {row.name: row.region_name for row in dwh_tx.execute(text("SELECT name, region_name FROM core.dim_city"))}
    assert _scalar(dwh_tx, "SELECT count(*) FROM core.dim_city") == len(cities)
    for part in ("train", "test"):
        X = dataset.X[part]
        for city_col, region_col in (("origin_city", "origin_region"), ("destination_city", "destination_region")):
            pairs = set(X.select(city_col, region_col).unique().rows())
            assert all(cities[city] == region for city, region in pairs)
        for name in ("origin_city", "destination_city", "origin_region", "destination_region"):
            assert not {MISSING, UNKNOWN} & set(X[name].unique().to_list())
    moscow = int((dataset.X["train"]["origin_city"] == "Москва").sum())
    direct = _scalar(dwh_tx, f"SELECT count(*) FROM core.fact_deal WHERE {ELIGIBLE} AND loading_city_gcid = 1001 "
                             "AND loading_date >= DATE '2026-07-26' AND loading_date < DATE '2026-10-05'")
    assert moscow == direct, "duplicate source city rows must not multiply deals"


def test_build_is_reproducible(dataset, dwh_engine, tmp_path):
    with dwh_engine.connect() as conn:
        again = build_dataset(conn)
    for part in ("train", "test"):
        assert frame_digest(again.X[part]) == frame_digest(dataset.X[part])
        assert frame_digest(again.y[part]) == frame_digest(dataset.y[part])
        assert again.ids[part].equals(dataset.ids[part])
    first = write_dataset(dataset, tmp_path / "a")
    second = write_dataset(again, tmp_path / "b")
    assert first == second
    for name in ("metadata.json", "profile.json"):
        assert (tmp_path / "a" / name).read_text() == (tmp_path / "b" / name).read_text()
    stored = pl.read_parquet(tmp_path / "a" / "X_test.parquet")
    assert stored.equals(dataset.X["test"])
    assert pl.read_parquet(tmp_path / "a" / "y_train.parquet").columns == [TARGET]
    meta = json.loads((tmp_path / "a" / "metadata.json").read_text())
    assert meta["feature_columns"] == list(FEATURE_COLUMNS)
    assert meta["split"] == DEFAULT_SPLIT.to_dict()
    profile = json.loads((tmp_path / "a" / "profile.json").read_text())
    failed = [c for c in profile["leakage_audit"] if not c["passed"]]
    assert not failed, failed
