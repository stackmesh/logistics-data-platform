"""Synced tables: columns, PostgreSQL sources, partition keys, and the cross-system row fingerprint.

The fingerprint of a partition is (row count, sum of a 60-bit row hash). Each row is rendered to the same
canonical text in both systems, md5-hashed, and the first 15 hex digits are summed. Equal fingerprints mean
the same multiset of rows, independent of order, so they detect lost, extra, duplicated, and changed rows.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

CLICKHOUSE_SQL_DIR = Path(__file__).resolve().parents[2] / "clickhouse" / "sql"
NULL_TOKEN = r"\N"
ALL_PARTITION = "all"


@dataclass(frozen=True)
class Column:
    name: str
    kind: str  # int, text, date, decimal, bool, timestamp, timestamp_us, uuid, float
    scale: int | None = None
    nullable: bool = False
    pg_select: str | None = None

    def __post_init__(self) -> None:
        if self.kind not in _KINDS:
            raise ValueError(f"unknown column kind {self.kind}")
        if (self.kind == "decimal") != (self.scale is not None):
            raise ValueError(f"{self.name}: scale is required for decimals only")

    @property
    def fingerprinted(self) -> bool:
        # Floats are not compared bit-for-bit across systems; every float synced here is derived from
        # fingerprinted columns (price_km = price_rub / distance_km).
        return self.kind != "float"

    def pg_canonical(self) -> str:
        c = self.name
        expr = {
            "int": f"{c}::text",
            "text": f"{c}::text",
            "uuid": f"{c}::text",
            "date": f"({c} - DATE '1970-01-01')::text",
            "decimal": f"round({c}, {self.scale})::text",
            "bool": f"{c}::int::text",
            "timestamp": f"extract(epoch FROM {c})::bigint::text",
            "timestamp_us": f"(extract(epoch FROM {c}) * 1000000)::bigint::text",
        }[self.kind]
        return f"coalesce({expr}, '{NULL_TOKEN}')" if self.nullable else expr

    def ch_canonical(self) -> str:
        c = self.name
        expr = {
            "int": f"toString({c})",
            "text": f"toString({c})",
            "uuid": f"toString({c})",
            "date": f"toString(toInt32({c}))",
            "decimal": f"toDecimalString({c}, {self.scale})",
            "bool": f"toString(toUInt8({c}))",
            "timestamp": f"toString(toUnixTimestamp({c}))",
            "timestamp_us": f"toString(toUnixTimestamp64Micro({c}))",
        }[self.kind]
        return f"ifNull({expr}, '{NULL_TOKEN.replace(chr(92), chr(92) * 2)}')" if self.nullable else expr


_KINDS = {"int", "text", "date", "decimal", "bool", "timestamp", "timestamp_us", "uuid", "float"}


@dataclass(frozen=True)
class TableSpec:
    name: str
    columns: tuple[Column, ...]
    pg_source: str
    pg_partition: str
    ch_partition: str
    partition_type: str  # int or text
    pg_order: str = "price_id"  # unique, so a partition streams in a deterministic order
    ch_final: bool = False
    load_mode: str = "partition"  # partition: REPLACE PARTITION; exchange: whole-table EXCHANGE

    @property
    def column_names(self) -> list[str]:
        return [column.name for column in self.columns]

    def pg_select_list(self) -> str:
        return ", ".join(column.pg_select or column.name for column in self.columns)

    def pg_row_hash(self) -> str:
        parts = ", ".join(column.pg_canonical() for column in self.columns if column.fingerprinted)
        return f"('x' || substr(md5(concat_ws('|', {parts})), 1, 15))::bit(60)::bigint"

    def ch_row_hash(self) -> str:
        parts = ", ".join(column.ch_canonical() for column in self.columns if column.fingerprinted)
        return (
            "bitShiftRight(reinterpretAsUInt64(reverse(unhex(substring(hex(MD5("
            f"concatWithSeparator('|', {parts}))), 1, 16)))), 4)"
        )

    def pg_fingerprint_sql(self, where: str | None = None) -> str:
        """Fingerprint per partition. %(...)s placeholders in where are psycopg parameters."""
        condition = f"WHERE {where}" if where else ""
        return (
            f"SELECT ({self.pg_partition})::text AS part, count(*) AS rows, "
            f"coalesce(sum({self.pg_row_hash()}), 0)::text AS hash "
            f"FROM {self.pg_source} {condition} GROUP BY 1"
        )

    def ch_fingerprint_sql(self, database: str, table: str | None = None, where: str | None = None) -> str:
        final = " FINAL" if self.ch_final else ""
        condition = f"WHERE {where}" if where else ""
        return (
            f"SELECT toString({self.ch_partition}) AS part, count() AS rows, "
            f"toString(sum(toUInt128({self.ch_row_hash()}))) AS hash "
            f"FROM {database}.{table or self.name}{final} {condition} GROUP BY part"
        )

    def pg_partition_filter(self) -> str:
        return f"({self.pg_partition})::text = %(partition)s"

    def ch_partition_literal(self, partition: str) -> str:
        if self.load_mode == "exchange":
            raise ValueError(f"{self.name} is not partitioned")
        if self.partition_type == "int":
            if not partition.isdigit():
                raise ValueError(f"invalid partition {partition!r} for {self.name}")
            return partition
        return "'" + partition.replace("\\", "\\\\").replace("'", "\\'") + "'"


FACT_DEAL = TableSpec(
    name="fact_deal",
    columns=(
        Column("price_id", "int"),
        Column("loading_date", "date"),
        Column("end_loading_date", "date", nullable=True),
        Column("loading_city_gcid", "int"),
        Column("unloading_city_gcid", "int"),
        Column("distance_km", "decimal", 3),
        Column("total_weight_t", "decimal", 3),
        Column("total_volume_m3", "decimal", 3),
        Column("body_type_id", "int", nullable=True),
        Column("body_group_code", "text"),
        Column("weight_category", "text"),
        Column("volume_category", "text"),
        Column("weight_category_imputed", "bool"),
        Column("volume_category_imputed", "bool"),
        Column("price_src", "decimal", 2),
        Column("currency_code", "text"),
        Column("fx_rate", "decimal", 6),
        Column("nearest_rate_date", "date", nullable=True),
        Column("price_rub", "decimal", 2),
        Column("price_type", "int"),
        Column("is_fact", "bool"),
        Column("complex", "bool"),
        Column("outlier_mark", "bool"),
        Column("organization_id", "int", nullable=True),
        Column("subscriber_id", "int", nullable=True),
        Column("created_at", "timestamp", nullable=True, pg_select="created_at AT TIME ZONE 'UTC' AS created_at"),
        Column("updated_at", "timestamp", nullable=True, pg_select="updated_at AT TIME ZONE 'UTC' AS updated_at"),
        Column("source_name", "text", nullable=True),
        Column("item_id", "int", nullable=True),
        Column("meta_item_id", "int", nullable=True),
        Column("external_item_uuid", "uuid", nullable=True),
        Column("batch_id", "int"),
    ),
    pg_source="core.fact_deal",
    pg_partition="to_char(loading_date, 'YYYYMM')::int",
    ch_partition="toYYYYMM(loading_date)",
    partition_type="int",
)

# Materialized per sync from mart.market_corridor_observations(as_of) inside the sync's snapshot.
OBSERVATION_TEMP_TABLE = "ch_sync_observation"
OBSERVATION_TEMP_SQL = f"""
CREATE TEMP TABLE {OBSERVATION_TEMP_TABLE} ON COMMIT DROP AS
SELECT o.price_month, o.price_date, o.body_group_code, o.price_id, o.loading_city_gcid, o.unloading_city_gcid,
       o.weight_category, o.price_rub, f.distance_km, o.price_km::float8 AS price_km, o.organization_id,
       o.price_type, %(as_of)s::date AS as_of_date
FROM mart.market_corridor_observations(%(as_of)s::date) AS o
JOIN core.fact_deal AS f ON f.price_id = o.price_id
"""

FACT_PRICE_OBSERVATION = TableSpec(
    name="fact_price_observation",
    columns=(
        Column("price_month", "date"),
        Column("price_date", "date"),
        Column("body_group_code", "text"),
        Column("price_id", "int"),
        Column("loading_city_gcid", "int"),
        Column("unloading_city_gcid", "int"),
        Column("weight_category", "text"),
        Column("price_rub", "decimal", 2),
        Column("distance_km", "decimal", 3),
        Column("price_km", "float"),
        Column("organization_id", "int", nullable=True),
        Column("price_type", "int"),
        Column("as_of_date", "date"),
    ),
    pg_source=OBSERVATION_TEMP_TABLE,
    pg_partition="to_char(price_month, 'YYYYMM')::int",
    ch_partition="toYYYYMM(price_month)",
    partition_type="int",
    pg_order="price_id, price_month, body_group_code",
)

ML_PRICE_PREDICTIONS = TableSpec(
    name="ml_price_predictions",
    columns=(
        Column("model_name", "text"),
        Column("model_version", "text"),
        Column("price_id", "int"),
        Column("split_role", "text"),
        Column("as_of_date", "date"),
        Column("loading_date", "date"),
        Column("loading_city_gcid", "int"),
        Column("unloading_city_gcid", "int"),
        Column("body_group", "text"),
        Column("actual_price_rub", "decimal", 2, nullable=True),
        Column("predicted_price_rub", "decimal", 2),
        Column("abs_error_rub", "decimal", 2, nullable=True),
        Column("ape_pct", "decimal", 4, nullable=True),
        Column("scored_at", "timestamp_us"),
        Column("scoring_batch_id", "int"),
    ),
    pg_source="ml.ml_price_predictions",
    pg_partition="model_version",
    ch_partition="model_version",
    partition_type="text",
    pg_order="model_name, price_id",
    ch_final=True,
)

DIM_CITY = TableSpec(
    name="dim_city",
    columns=(Column("gc_id", "int"), Column("name", "text")),
    pg_source="core.dim_city",
    pg_partition=f"'{ALL_PARTITION}'",
    ch_partition=f"'{ALL_PARTITION}'",
    partition_type="text",
    pg_order="gc_id",
    load_mode="exchange",
)

DIM_BODY_GROUP = TableSpec(
    name="dim_body_group",
    columns=(Column("body_group_code", "text"), Column("body_group_name", "text")),
    pg_source="core.dim_body_group",
    pg_partition=f"'{ALL_PARTITION}'",
    ch_partition=f"'{ALL_PARTITION}'",
    partition_type="text",
    pg_order="body_group_code",
    load_mode="exchange",
)

SYNCED_TABLES = (DIM_CITY, DIM_BODY_GROUP, FACT_DEAL, FACT_PRICE_OBSERVATION, ML_PRICE_PREDICTIONS)
TABLES_BY_NAME = {spec.name: spec for spec in SYNCED_TABLES}

# Tables carrying the sync run id. Dimensions do not.
RUN_ID_TABLES = {"fact_deal", "fact_price_observation", "ml_price_predictions"}
