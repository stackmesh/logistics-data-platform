"""Set-based merge of one snapshot into raw, core, and audit.

Everything runs in the caller's transaction, so a batch is applied completely or not at all.
Rows are written only when their values change, so a repeated run leaves lineage untouched.
"""

from __future__ import annotations

from collections.abc import Sequence

from sqlalchemy import text
from sqlalchemy.engine import Connection

from etl.config import DateWindow
from etl.extract.source import (
    BODY_TYPE_SCHEMA,
    CITY_SCHEMA,
    EXCHANGE_RATE_SCHEMA,
    PRICE_BODY_TYPE_SCHEMA,
    PRICE_DATE_SCHEMA,
    PRICE_SCHEMA,
    SourceSnapshot,
)
from etl.load.copy import copy_frame
from etl.transform.normalize import (
    BODY_TYPE_COLUMNS,
    CITY_COLUMNS,
    DEAL_COLUMNS,
    QUARANTINE_COLUMNS,
    RATE_COLUMNS,
    TransformResult,
)
from etl.transform.rules import REASON_CODES

ADVISORY_LOCK_KEY = "logistics_dwh.core_etl"

LINEAGE_COLUMNS = ("source_system", "extracted_at", "batch_id")
LINEAGE_SELECT = (
    "CAST(:source_system AS text), CAST(:extracted_at AS timestamptz), CAST(:batch_id AS bigint)"
)
WINDOW_MATCH = (
    "(CAST(:full AS boolean) OR {alias}.loading_date BETWEEN CAST(:start_date AS date) AND CAST(:end_date AS date))"
)

RAW_TABLES = {
    "prices": tuple(PRICE_SCHEMA),
    "price_body_types": tuple(PRICE_BODY_TYPE_SCHEMA),
    "price_dates": tuple(PRICE_DATE_SCHEMA),
    "cities": tuple(CITY_SCHEMA),
    "body_types": tuple(BODY_TYPE_SCHEMA),
    "exchange_rates": tuple(EXCHANGE_RATE_SCHEMA),
}

CORE_STAGE_DDL = {
    "stg_dim_exchange_rate": """
        currency_code text NOT NULL,
        rate_date date NOT NULL,
        rate_micro bigint NOT NULL,
        source_rate_id bigint NOT NULL""",
    "stg_dim_body_type": """
        body_type_id integer NOT NULL,
        name text NOT NULL,
        id_from_1c integer NOT NULL,
        source_std_id integer,
        body_group_code text NOT NULL,
        source_system text NOT NULL""",
    "stg_dim_city": """
        gc_id integer NOT NULL,
        source_city_id bigint NOT NULL,
        name text NOT NULL,
        full_name_local text NOT NULL,
        region_name text,
        latitude double precision NOT NULL,
        longitude double precision NOT NULL,
        duplicate_count integer NOT NULL""",
    "stg_deal": """
        price_id bigint NOT NULL,
        loading_date date NOT NULL,
        end_loading_date date,
        loading_city_gcid integer NOT NULL,
        unloading_city_gcid integer NOT NULL,
        distance_km double precision NOT NULL,
        total_weight_t double precision NOT NULL,
        total_volume_m3 double precision NOT NULL,
        body_type_id integer,
        body_group_code text NOT NULL,
        weight_category text NOT NULL,
        volume_category text NOT NULL,
        weight_category_imputed boolean NOT NULL,
        volume_category_imputed boolean NOT NULL,
        price_src_cents bigint NOT NULL,
        currency_code text NOT NULL,
        fx_rate_micro bigint NOT NULL,
        nearest_rate_date date,
        fx_rate_date date,
        price_rub_cents bigint NOT NULL,
        price_type smallint NOT NULL,
        is_fact boolean NOT NULL,
        complex boolean NOT NULL,
        outlier_mark boolean NOT NULL,
        organization_id bigint,
        subscriber_id bigint,
        created_at timestamp,
        updated_at timestamp,
        source_name text,
        item_id bigint,
        meta_item_id bigint,
        external_item_uuid text""",
    "stg_deal_date": """
        price_id bigint NOT NULL,
        price_date date NOT NULL""",
    "stg_bridge": """
        price_id bigint NOT NULL,
        body_group_code text NOT NULL,
        resolution text NOT NULL""",
    "stg_quarantine": """
        price_id bigint NOT NULL,
        reason_code text NOT NULL,
        reason_detail text NOT NULL,
        created_at timestamp,
        loading_date date,
        loading_city_gcid integer,
        unloading_city_gcid integer,
        distance double precision,
        total_weight double precision,
        total_volume double precision,
        price double precision,
        currency_code text,
        nearest_rate_date date,
        price_type smallint,
        is_fact boolean,
        source_record text NOT NULL""",
}

FACT_COLUMNS = (
    "price_id",
    "loading_date",
    "end_loading_date",
    "loading_city_gcid",
    "unloading_city_gcid",
    "distance_km",
    "total_weight_t",
    "total_volume_m3",
    "body_type_id",
    "body_group_code",
    "weight_category",
    "volume_category",
    "weight_category_imputed",
    "volume_category_imputed",
    "price_src",
    "currency_code",
    "fx_rate",
    "nearest_rate_date",
    "fx_rate_date",
    "price_rub",
    "price_type",
    "is_fact",
    "complex",
    "outlier_mark",
    "organization_id",
    "subscriber_id",
    "created_at",
    "updated_at",
    "source_name",
    "item_id",
    "meta_item_id",
    "external_item_uuid",
)

FACT_SELECT_EXPRESSIONS = {
    "price_src": "price_src_cents::numeric / 100",
    "fx_rate": "fx_rate_micro::numeric / 1000000",
    "price_rub": "price_rub_cents::numeric / 100",
    "external_item_uuid": "external_item_uuid::uuid",
}

QUARANTINE_SNAPSHOT_COLUMNS = QUARANTINE_COLUMNS[3:-1]

STAGE_KEYS = {
    **{f"stg_raw_{table}": ("id",) for table in RAW_TABLES},
    "stg_dim_exchange_rate": ("currency_code", "rate_date"),
    "stg_dim_body_type": ("body_type_id",),
    "stg_dim_city": ("gc_id",),
    "stg_deal": ("price_id",),
    "stg_deal_date": ("price_id", "price_date"),
    "stg_bridge": ("price_id", "body_group_code"),
    "stg_quarantine": ("price_id", "reason_code"),
}

# Merges anti-join large targets against stages. Planner statistics left over from a
# previous bulk load can be far off before autovacuum catches up, so refresh them first.
TARGET_TABLES = (
    "raw.prices",
    "raw.price_body_types",
    "raw.price_dates",
    "core.fact_deal",
    "core.fact_deal_date",
    "core.bridge_deal_body_group",
    "audit.quarantine_price",
)


def _upsert(
    conn: Connection,
    *,
    target: str,
    insert_columns: Sequence[str],
    select_sql: str,
    key: Sequence[str],
    compare_columns: Sequence[str],
    update_columns: Sequence[str],
    params: dict,
) -> dict[str, int]:
    assignments = ", ".join(f"{column} = EXCLUDED.{column}" for column in update_columns)
    current = ", ".join(f"t.{column}" for column in compare_columns)
    proposed = ", ".join(f"EXCLUDED.{column}" for column in compare_columns)
    sql = f"""
        WITH upserted AS (
            INSERT INTO {target} AS t ({", ".join(insert_columns)})
            {select_sql}
            ON CONFLICT ({", ".join(key)}) DO UPDATE SET {assignments}
            WHERE ({current}) IS DISTINCT FROM ({proposed})
            RETURNING (xmax = 0) AS inserted
        )
        SELECT count(*) FILTER (WHERE inserted) AS inserted,
               count(*) FILTER (WHERE NOT inserted) AS updated
        FROM upserted
    """
    row = conn.execute(text(sql), params).one()
    return {"inserted": int(row.inserted), "updated": int(row.updated)}


def _execute_count(conn: Connection, sql: str, params: dict | None = None) -> int:
    return int(conn.execute(text(sql), params or {}).rowcount)


def _create_stages(conn: Connection) -> None:
    for table, columns in RAW_TABLES.items():
        stage = f"stg_raw_{table}"
        conn.execute(text(f"DROP TABLE IF EXISTS {stage}"))
        conn.execute(
            text(
                f"CREATE TEMP TABLE {stage} ON COMMIT DROP AS "
                f"SELECT {', '.join(columns)} FROM raw.{table} WITH NO DATA"
            )
        )
    for stage, ddl in CORE_STAGE_DDL.items():
        conn.execute(text(f"DROP TABLE IF EXISTS {stage}"))
        conn.execute(text(f"CREATE TEMP TABLE {stage} ({ddl}) ON COMMIT DROP"))


def _stage(conn: Connection, snapshot: SourceSnapshot, result: TransformResult) -> dict[str, int]:
    raw_frames = {
        "prices": snapshot.prices,
        "price_body_types": snapshot.price_body_types,
        "price_dates": snapshot.price_dates,
        "cities": snapshot.cities,
        "body_types": snapshot.body_types,
        "exchange_rates": snapshot.exchange_rates,
    }
    staged = {}
    for table, frame in raw_frames.items():
        staged[f"raw_{table}"] = copy_frame(conn, f"stg_raw_{table}", RAW_TABLES[table], frame)
    core_frames = {
        "stg_dim_exchange_rate": (RATE_COLUMNS, result.dim_exchange_rate),
        "stg_dim_body_type": (BODY_TYPE_COLUMNS, result.dim_body_type),
        "stg_dim_city": (CITY_COLUMNS, result.dim_city),
        "stg_deal": (DEAL_COLUMNS, result.deals),
        "stg_deal_date": (("price_id", "price_date"), result.deal_dates),
        "stg_bridge": (("price_id", "body_group_code", "resolution"), result.bridge),
        "stg_quarantine": (QUARANTINE_COLUMNS, result.quarantine),
    }
    for stage, (columns, frame) in core_frames.items():
        staged[stage.removeprefix("stg_")] = copy_frame(conn, stage, columns, frame)
    for stage, key in STAGE_KEYS.items():
        conn.execute(text(f"ALTER TABLE {stage} ADD PRIMARY KEY ({', '.join(key)})"))
        conn.execute(text(f"ANALYZE {stage}"))
    return staged


def _load_raw(conn: Connection, params: dict) -> dict[str, dict[str, int]]:
    counts: dict[str, dict[str, int]] = {}

    for table in ("cities", "body_types", "exchange_rates"):
        deleted = _execute_count(
            conn,
            f"DELETE FROM raw.{table} AS t WHERE NOT EXISTS "
            f"(SELECT 1 FROM stg_raw_{table} AS s WHERE s.id = t.id)",
        )
        counts[table] = {"deleted": deleted}

    counts["prices"] = {
        "deleted": _execute_count(
            conn,
            f"DELETE FROM raw.prices AS t WHERE {WINDOW_MATCH.format(alias='t')} "
            "AND NOT EXISTS (SELECT 1 FROM stg_raw_prices AS s WHERE s.id = t.id)",
            params,
        )
    }
    for table in ("price_body_types", "price_dates"):
        counts[table] = {
            "deleted": _execute_count(
                conn,
                f"DELETE FROM raw.{table} AS t "
                "WHERE EXISTS (SELECT 1 FROM stg_raw_prices AS p WHERE p.id = t.price_id) "
                f"AND NOT EXISTS (SELECT 1 FROM stg_raw_{table} AS s WHERE s.id = t.id)",
            )
        }

    for table in ("cities", "body_types", "exchange_rates", "prices", "price_body_types", "price_dates"):
        columns = RAW_TABLES[table]
        compare = [column for column in columns if column != "id"]
        counts[table].update(
            _upsert(
                conn,
                target=f"raw.{table}",
                insert_columns=[*columns, *LINEAGE_COLUMNS],
                select_sql=f"SELECT {', '.join(columns)}, {LINEAGE_SELECT} FROM stg_raw_{table}",
                key=["id"],
                compare_columns=compare,
                update_columns=[*compare, *LINEAGE_COLUMNS],
                params=params,
            )
        )
    return counts


def _load_dimensions(conn: Connection, params: dict) -> dict[str, dict[str, int]]:
    counts = {}
    counts["dim_exchange_rate"] = _upsert(
        conn,
        target="core.dim_exchange_rate",
        insert_columns=["currency_code", "rate_date", "rate_value", "source_rate_id", *LINEAGE_COLUMNS],
        select_sql=(
            "SELECT currency_code, rate_date, rate_micro::numeric / 1000000, source_rate_id, "
            f"{LINEAGE_SELECT} FROM stg_dim_exchange_rate"
        ),
        key=["currency_code", "rate_date"],
        compare_columns=["rate_value", "source_rate_id"],
        update_columns=["rate_value", "source_rate_id", *LINEAGE_COLUMNS],
        params=params,
    )
    counts["dim_body_type"] = _upsert(
        conn,
        target="core.dim_body_type",
        insert_columns=[
            "body_type_id", "name", "id_from_1c", "source_std_id", "body_group_code",
            "source_system", "extracted_at", "batch_id",
        ],
        select_sql=(
            "SELECT body_type_id, name, id_from_1c, source_std_id, body_group_code, source_system, "
            "CAST(:extracted_at AS timestamptz), CAST(:batch_id AS bigint) FROM stg_dim_body_type"
        ),
        key=["body_type_id"],
        compare_columns=["name", "id_from_1c", "source_std_id", "body_group_code"],
        update_columns=[
            "name", "id_from_1c", "source_std_id", "body_group_code",
            "source_system", "extracted_at", "batch_id",
        ],
        params=params,
    )
    city_values = [column for column in CITY_COLUMNS if column != "gc_id"]
    counts["dim_city"] = _upsert(
        conn,
        target="core.dim_city",
        insert_columns=[*CITY_COLUMNS, *LINEAGE_COLUMNS],
        select_sql=f"SELECT {', '.join(CITY_COLUMNS)}, {LINEAGE_SELECT} FROM stg_dim_city",
        key=["gc_id"],
        compare_columns=city_values,
        update_columns=[*city_values, *LINEAGE_COLUMNS],
        params=params,
    )
    return counts


def _load_facts(conn: Connection, params: dict) -> dict[str, dict[str, int]]:
    deleted = _execute_count(
        conn,
        f"""
        DELETE FROM core.fact_deal AS f
        WHERE ({WINDOW_MATCH.format(alias='f')}
               OR EXISTS (SELECT 1 FROM stg_raw_prices AS s WHERE s.id = f.price_id))
          AND NOT EXISTS (SELECT 1 FROM stg_deal AS d WHERE d.price_id = f.price_id)
        """,
        params,
    )
    select_list = ", ".join(FACT_SELECT_EXPRESSIONS.get(column, column) for column in FACT_COLUMNS)
    fact_values = [column for column in FACT_COLUMNS if column != "price_id"]
    fact = _upsert(
        conn,
        target="core.fact_deal",
        insert_columns=[*FACT_COLUMNS, *LINEAGE_COLUMNS],
        select_sql=f"SELECT {select_list}, {LINEAGE_SELECT} FROM stg_deal",
        key=["price_id"],
        compare_columns=fact_values,
        update_columns=[*fact_values, *LINEAGE_COLUMNS],
        params=params,
    )
    fact["deleted"] = deleted

    dates = {
        "deleted": _execute_count(
            conn,
            """
            DELETE FROM core.fact_deal_date AS t
            WHERE EXISTS (SELECT 1 FROM stg_deal AS d WHERE d.price_id = t.price_id)
              AND NOT EXISTS (
                  SELECT 1 FROM stg_deal_date AS s
                  WHERE s.price_id = t.price_id AND s.price_date = t.price_date
              )
            """,
        ),
        "inserted": _execute_count(
            conn,
            "INSERT INTO core.fact_deal_date (price_id, price_date) "
            "SELECT price_id, price_date FROM stg_deal_date ON CONFLICT DO NOTHING",
        ),
    }

    bridge_deleted = _execute_count(
        conn,
        """
        DELETE FROM core.bridge_deal_body_group AS t
        WHERE EXISTS (SELECT 1 FROM stg_deal AS d WHERE d.price_id = t.price_id)
          AND NOT EXISTS (
              SELECT 1 FROM stg_bridge AS s
              WHERE s.price_id = t.price_id AND s.body_group_code = t.body_group_code
          )
        """,
    )
    bridge = _upsert(
        conn,
        target="core.bridge_deal_body_group",
        insert_columns=["price_id", "body_group_code", "resolution"],
        select_sql="SELECT price_id, body_group_code, resolution FROM stg_bridge",
        key=["price_id", "body_group_code"],
        compare_columns=["resolution"],
        update_columns=["resolution"],
        params=params,
    )
    bridge["deleted"] = bridge_deleted
    return {"fact_deal": fact, "fact_deal_date": dates, "bridge_deal_body_group": bridge}


def _load_quarantine(conn: Connection, params: dict) -> dict[str, int]:
    deleted = _execute_count(
        conn,
        f"""
        DELETE FROM audit.quarantine_price AS q
        WHERE ({WINDOW_MATCH.format(alias='q')}
               OR EXISTS (SELECT 1 FROM stg_raw_prices AS s WHERE s.id = q.price_id))
          AND NOT EXISTS (
              SELECT 1 FROM stg_quarantine AS r
              WHERE r.price_id = q.price_id AND r.reason_code = q.reason_code
          )
        """,
        params,
    )
    compare = ["reason_detail", "source_record", *QUARANTINE_SNAPSHOT_COLUMNS]
    counts = _upsert(
        conn,
        target="audit.quarantine_price",
        insert_columns=[
            "price_id", "reason_code", "reason_detail", "source_record",
            *QUARANTINE_SNAPSHOT_COLUMNS, "source_system", "batch_id", "rejected_at",
        ],
        select_sql=(
            "SELECT price_id, reason_code, reason_detail, source_record::jsonb, "
            f"{', '.join(QUARANTINE_SNAPSHOT_COLUMNS)}, "
            "CAST(:source_system AS text), CAST(:batch_id AS bigint), now() FROM stg_quarantine"
        ),
        key=["price_id", "reason_code"],
        compare_columns=compare,
        update_columns=[*compare, "source_system", "batch_id", "rejected_at"],
        params=params,
    )
    counts["deleted"] = deleted
    return counts


def _write_dq_statistics(conn: Connection, batch_id: int, reason_counts: dict[str, int]) -> None:
    conn.execute(
        text(
            """
            INSERT INTO audit.dq_statistic (batch_id, reason_code, rows_rejected)
            SELECT CAST(:batch_id AS bigint), code, cnt
            FROM unnest(CAST(:codes AS text[]), CAST(:counts AS bigint[])) AS u(code, cnt)
            ON CONFLICT (batch_id, reason_code) DO UPDATE SET rows_rejected = EXCLUDED.rows_rejected
            """
        ),
        {
            "batch_id": batch_id,
            "codes": list(REASON_CODES),
            "counts": [int(reason_counts.get(code, 0)) for code in REASON_CODES],
        },
    )


def load_snapshot(
    conn: Connection,
    *,
    batch_id: int,
    window: DateWindow,
    snapshot: SourceSnapshot,
    result: TransformResult,
    source_system: str,
) -> dict:
    """Apply one snapshot. The caller owns the transaction and commits or rolls back."""
    params = {
        **window.sql_params(),
        "batch_id": batch_id,
        "extracted_at": snapshot.extracted_at,
        "source_system": source_system,
    }
    conn.execute(text("SELECT pg_advisory_xact_lock(hashtext(:key))"), {"key": ADVISORY_LOCK_KEY})
    for table in TARGET_TABLES:
        conn.execute(text(f"ANALYZE {table}"))
    _create_stages(conn)
    staged = _stage(conn, snapshot, result)
    raw = _load_raw(conn, params)
    dimensions = _load_dimensions(conn, params)
    facts = _load_facts(conn, params)
    quarantine = _load_quarantine(conn, params)
    _write_dq_statistics(conn, batch_id, result.stats["reason_counts"])
    return {
        "staged": staged,
        "raw": raw,
        "core": {**dimensions, **facts},
        "quarantine": quarantine,
    }
