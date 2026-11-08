"""Catalog and constraint checks for the core warehouse. No ETL data is required."""

from __future__ import annotations

import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.exc import IntegrityError
from sqlalchemy.pool import NullPool

from logistics.settings import load_settings

pytestmark = pytest.mark.integration

BODY_GROUP_CASES = (
    *((code, "TENT") for code in (56, 57, 58, 59, 60, 61)),
    *((code, "REF") for code in (4, 46, 47, 48)),
    (2, "ISO"),
    *((code, "BORT") for code in (1, 15)),
    *((code, "OTHER") for code in (3, 5, 26, 50, 65, 68)),
    (None, "UNSPECIFIED"),
)


@pytest.fixture(scope="module")
def engine():
    settings = load_settings()
    target = settings.warehouse
    db = create_engine(target.url(settings.host, settings.port), poolclass=NullPool, **settings.engine_kwargs())
    yield db
    db.dispose()


@pytest.fixture
def conn(engine):
    connection = engine.connect()
    transaction = connection.begin()
    try:
        yield connection
    finally:
        transaction.rollback()
        connection.close()


def _sqlstate(exc: IntegrityError) -> str | None:
    return getattr(exc.orig, "sqlstate", None)


def _batch_id(conn) -> int:
    return int(
        conn.execute(
            text(
                """
                INSERT INTO audit.etl_batch (source_system, mode, status, completed_at)
                VALUES ('deals_bigdata', 'full', 'succeeded', clock_timestamp())
                RETURNING batch_id
                """
            )
        ).scalar_one()
    )


def _city(conn, batch_id: int, gc_id: int, source_city_id: int) -> None:
    conn.execute(
        text(
            """
            INSERT INTO core.dim_city (
                gc_id, source_city_id, name, full_name_local, region_name,
                duplicate_count, source_system, extracted_at, batch_id
            ) VALUES (
                :gc_id, :source_city_id, :name, :full_name, 'Region',
                1, 'deals_bigdata', now(), :batch_id
            )
            """
        ),
        {
            "gc_id": gc_id,
            "source_city_id": source_city_id,
            "name": f"City {gc_id}",
            "full_name": f"City {gc_id}, Region",
            "batch_id": batch_id,
        },
    )


def _deal(conn, batch_id: int, price_id: int, **overrides) -> None:
    values = {
        "price_id": price_id,
        "loading_city_gcid": 910001,
        "unloading_city_gcid": 910002,
        "body_type_id": 56,
        "body_group_code": "TENT",
        "weight_category": "FTL",
        "volume_category": "FTL",
        "currency_code": "643",
        "batch_id": batch_id,
    }
    values.update(overrides)
    conn.execute(
        text(
            """
            INSERT INTO core.fact_deal (
                price_id, loading_date, loading_city_gcid, unloading_city_gcid,
                distance_km, total_weight_t, total_volume_m3,
                body_type_id, body_group_code, weight_category, volume_category,
                weight_category_imputed, volume_category_imputed,
                price_src, currency_code, fx_rate, nearest_rate_date, price_rub,
                price_type, is_fact, complex, outlier_mark,
                organization_id, subscriber_id, created_at,
                source_system, extracted_at, batch_id
            ) VALUES (
                :price_id, DATE '2026-09-15', :loading_city_gcid, :unloading_city_gcid,
                897, 20, 80,
                :body_type_id, :body_group_code, :weight_category, :volume_category,
                false, false,
                84500, :currency_code, 1, NULL, 84500,
                3, true, false, false,
                1001, 101, TIMESTAMP '2026-09-10 08:00:00',
                'deals_bigdata', now(), :batch_id
            )
            """
        ),
        values,
    )


def test_required_relations_and_reserved_schemas(conn):
    rows = conn.execute(
        text(
            """
            SELECT n.nspname, c.relname
            FROM pg_class AS c
            JOIN pg_namespace AS n ON n.oid = c.relnamespace
            WHERE n.nspname IN ('raw', 'core', 'mart', 'ml', 'audit')
              AND c.relkind = 'r'
            """
        )
    ).all()
    present = {(schema, name) for schema, name in rows}
    required = {
        ("audit", "etl_batch"),
        ("audit", "dq_statistic"),
        ("audit", "quarantine_price"),
        ("raw", "cities"),
        ("raw", "body_types"),
        ("raw", "exchange_rates"),
        ("raw", "prices"),
        ("raw", "price_body_types"),
        ("raw", "price_dates"),
        ("raw", "price_group_stats"),
        ("core", "dim_city"),
        ("core", "dim_body_group"),
        ("core", "dim_body_type"),
        ("core", "dim_exchange_rate"),
        ("core", "fact_deal"),
        ("core", "fact_deal_date"),
        ("core", "bridge_deal_body_group"),
    }
    missing = required - present
    assert not missing, missing
    assert not any(schema in {"mart", "ml"} for schema, _ in present)


def test_fact_preserves_source_deal_columns(conn):
    columns = set(
        conn.execute(
            text(
                """
                SELECT column_name
                FROM information_schema.columns
                WHERE table_schema = 'core' AND table_name = 'fact_deal'
                """
            )
        ).scalars()
    )
    assert {
        "price_id",
        "loading_date",
        "loading_city_gcid",
        "unloading_city_gcid",
        "distance_km",
        "total_weight_t",
        "total_volume_m3",
        "body_group_code",
        "weight_category",
        "volume_category",
        "price_src",
        "currency_code",
        "fx_rate",
        "price_rub",
        "price_type",
        "is_fact",
        "complex",
        "outlier_mark",
        "organization_id",
        "subscriber_id",
        "created_at",
        "nearest_rate_date",
        "source_system",
        "extracted_at",
        "batch_id",
    } <= columns
    assert "price_group_id" not in columns


def test_source_price_id_is_unique(conn):
    batch_id = _batch_id(conn)
    _city(conn, batch_id, 910001, 910001)
    _city(conn, batch_id, 910002, 910002)
    _deal(conn, batch_id, 9100001)
    with pytest.raises(IntegrityError) as caught:
        with conn.begin_nested():
            _deal(conn, batch_id, 9100001)
    assert _sqlstate(caught.value) == "23505"


def test_city_gc_id_is_unique(conn):
    batch_id = _batch_id(conn)
    _city(conn, batch_id, 910001, 910001)
    with pytest.raises(IntegrityError) as caught:
        with conn.begin_nested():
            _city(conn, batch_id, 910001, 910099)
    assert _sqlstate(caught.value) == "23505"


def test_source_city_id_cannot_be_reused(conn):
    batch_id = _batch_id(conn)
    _city(conn, batch_id, 910001, 910001)
    with pytest.raises(IntegrityError) as caught:
        with conn.begin_nested():
            _city(conn, batch_id, 910003, 910001)
    assert _sqlstate(caught.value) == "23505"


def test_foreign_keys_reject_unknown_city_and_batch(conn):
    batch_id = _batch_id(conn)
    _city(conn, batch_id, 910001, 910001)
    _city(conn, batch_id, 910002, 910002)
    with pytest.raises(IntegrityError) as unknown_city:
        with conn.begin_nested():
            _deal(conn, batch_id, 9100002, loading_city_gcid=1)
    assert _sqlstate(unknown_city.value) == "23503"
    with pytest.raises(IntegrityError) as unknown_batch:
        with conn.begin_nested():
            _deal(conn, 2_000_000_000, 9100003)
    assert _sqlstate(unknown_batch.value) == "23503"


def test_currency_codes_are_restricted(conn):
    batch_id = _batch_id(conn)
    _city(conn, batch_id, 910001, 910001)
    _city(conn, batch_id, 910002, 910002)
    with pytest.raises(IntegrityError) as caught:
        with conn.begin_nested():
            _deal(conn, batch_id, 9100004, currency_code="999")
    assert _sqlstate(caught.value) == "23514"


def test_category_values_are_restricted(conn):
    batch_id = _batch_id(conn)
    _city(conn, batch_id, 910001, 910001)
    _city(conn, batch_id, 910002, 910002)
    with pytest.raises(IntegrityError) as weight:
        with conn.begin_nested():
            _deal(conn, batch_id, 9100005, weight_category="BAD")
    assert _sqlstate(weight.value) == "23514"
    with pytest.raises(IntegrityError) as volume:
        with conn.begin_nested():
            _deal(conn, batch_id, 9100006, volume_category="LTL")
    assert _sqlstate(volume.value) == "23514"


def test_body_group_mapping_and_single_group_per_type(conn):
    rows = conn.execute(
        text(
            """
            SELECT body_type_id, body_group_code, core.body_group_for_type(body_type_id)
            FROM core.dim_body_type
            ORDER BY body_type_id
            """
        )
    ).all()
    mapped = {body_type_id: (stored, computed) for body_type_id, stored, computed in rows}
    assert len(mapped) == len(rows)
    for body_type_id, expected in BODY_GROUP_CASES:
        if body_type_id is None:
            assert conn.execute(text("SELECT core.body_group_for_type(NULL)")).scalar_one() == expected
            continue
        stored, computed = mapped[body_type_id]
        assert stored == expected
        assert computed == expected
    groups = set(
        conn.execute(text("SELECT body_group_code FROM core.dim_body_group")).scalars()
    )
    assert groups == {"TENT", "REF", "ISO", "BORT", "OTHER", "UNSPECIFIED"}


def test_same_body_group_cannot_duplicate_a_deal(conn):
    batch_id = _batch_id(conn)
    _city(conn, batch_id, 910001, 910001)
    _city(conn, batch_id, 910002, 910002)
    _deal(conn, batch_id, 9100007)
    conn.execute(
        text(
            """
            INSERT INTO core.bridge_deal_body_group (price_id, body_group_code, resolution)
            VALUES (9100007, 'TENT', 'links')
            """
        )
    )
    with pytest.raises(IntegrityError) as duplicate_group:
        with conn.begin_nested():
            conn.execute(
                text(
                    """
                    INSERT INTO core.bridge_deal_body_group (price_id, body_group_code, resolution)
                    VALUES (9100007, 'TENT', 'links')
                    """
                )
            )
    assert _sqlstate(duplicate_group.value) == "23505"
    conn.execute(
        text(
            """
            INSERT INTO core.bridge_deal_body_group (price_id, body_group_code, resolution)
            VALUES (9100007, 'BORT', 'links')
            """
        )
    )
    count = conn.execute(
        text("SELECT COUNT(*) FROM core.bridge_deal_body_group WHERE price_id = 9100007")
    ).scalar_one()
    assert count == 2


def test_declared_body_group_must_match_the_source_type(conn):
    batch_id = _batch_id(conn)
    _city(conn, batch_id, 910001, 910001)
    _city(conn, batch_id, 910002, 910002)
    with pytest.raises(IntegrityError) as caught:
        with conn.begin_nested():
            _deal(conn, batch_id, 9100008, body_type_id=56, body_group_code="BORT")
    assert _sqlstate(caught.value) == "23514"


def test_price_group_stats_has_no_downstream_foreign_key(conn):
    references = conn.execute(
        text(
            """
            SELECT COUNT(*)
            FROM pg_constraint
            WHERE confrelid = 'raw.price_group_stats'::regclass
            """
        )
    ).scalar_one()
    assert references == 0
    core_columns = set(
        conn.execute(
            text(
                """
                SELECT column_name
                FROM information_schema.columns
                WHERE table_schema = 'core'
                """
            )
        ).scalars()
    )
    assert "price_group_id" not in core_columns
    assert "avg_price" not in core_columns
