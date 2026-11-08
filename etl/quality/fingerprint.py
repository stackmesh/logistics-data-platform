"""Deterministic fingerprints of the loaded dataset for a loading_date window.

logical_* hashes cover business values only. lineage_* hashes cover batch_id and
extracted_at, so they change whenever a row is rewritten, even with identical values.
"""

from __future__ import annotations

from sqlalchemy import text
from sqlalchemy.engine import Connection

from etl.config import DateWindow

_WINDOW = "(CAST(:full AS boolean) OR {column} BETWEEN CAST(:start_date AS date) AND CAST(:end_date AS date))"

FACT_LOGICAL = (
    "price_id, loading_date, end_loading_date, loading_city_gcid, unloading_city_gcid, distance_km, "
    "total_weight_t, total_volume_m3, body_type_id, body_group_code, weight_category, volume_category, "
    "weight_category_imputed, volume_category_imputed, price_src, currency_code, fx_rate, "
    "nearest_rate_date, fx_rate_date, price_rub, price_type, is_fact, complex, outlier_mark, "
    "organization_id, subscriber_id, created_at, updated_at, source_name, item_id, meta_item_id, "
    "external_item_uuid"
)

RAW_LOGICAL = (
    "id, created_at, updated_at, subscriber_id, organization_id, loading_date, loading_city_gcid, "
    "unloading_city_gcid, distance, total_weight, total_volume, body_type, price, currency_code, complex, "
    "price_type, item_id, is_fact, nearest_rate_date, meta_item_id, end_loading_date, source, "
    "external_item_uuid, outlier_mark, weight_category, volume_category, price_group_id"
)

FINGERPRINT_SQL = f"""
WITH fact AS (
    SELECT * FROM core.fact_deal WHERE {_WINDOW.format(column="loading_date")}
)
SELECT
    (SELECT count(*) FROM fact) AS fact_rows,
    (SELECT coalesce(sum(price_rub), 0)::text FROM fact) AS sum_price_rub,
    (SELECT coalesce(sum(distance_km), 0)::text FROM fact) AS sum_distance_km,
    (SELECT md5(coalesce(string_agg(md5(ROW({FACT_LOGICAL})::text), '' ORDER BY price_id), '')) FROM fact)
        AS logical_fact,
    (SELECT md5(coalesce(string_agg(md5(ROW(price_id, batch_id, extracted_at)::text), '' ORDER BY price_id), ''))
     FROM fact) AS lineage_fact,
    (SELECT count(*) FROM core.fact_deal_date d JOIN fact f USING (price_id)) AS deal_date_rows,
    (SELECT md5(coalesce(string_agg(md5(ROW(d.price_id, d.price_date)::text), ''
                ORDER BY d.price_id, d.price_date), ''))
     FROM core.fact_deal_date d JOIN fact f USING (price_id)) AS logical_deal_date,
    (SELECT count(*) FROM core.bridge_deal_body_group b JOIN fact f USING (price_id)) AS bridge_rows,
    (SELECT md5(coalesce(string_agg(md5(ROW(b.price_id, b.body_group_code, b.resolution)::text), ''
                ORDER BY b.price_id, b.body_group_code), ''))
     FROM core.bridge_deal_body_group b JOIN fact f USING (price_id)) AS logical_bridge,
    (SELECT count(*) FROM audit.quarantine_price q WHERE {_WINDOW.format(column="q.loading_date")})
        AS quarantine_rows,
    (SELECT md5(coalesce(string_agg(md5(ROW(price_id, reason_code, reason_detail, source_record)::text), ''
                ORDER BY price_id, reason_code), ''))
     FROM audit.quarantine_price q WHERE {_WINDOW.format(column="q.loading_date")}) AS logical_quarantine,
    (SELECT md5(coalesce(string_agg(md5(ROW(price_id, reason_code, batch_id, rejected_at)::text), ''
                ORDER BY price_id, reason_code), ''))
     FROM audit.quarantine_price q WHERE {_WINDOW.format(column="q.loading_date")}) AS lineage_quarantine,
    (SELECT count(*) FROM raw.prices r WHERE {_WINDOW.format(column="r.loading_date")}) AS raw_rows,
    (SELECT md5(coalesce(string_agg(md5(ROW({RAW_LOGICAL})::text), '' ORDER BY id), ''))
     FROM raw.prices r WHERE {_WINDOW.format(column="r.loading_date")}) AS logical_raw,
    (SELECT md5(coalesce(string_agg(md5(ROW(id, batch_id, extracted_at)::text), '' ORDER BY id), ''))
     FROM raw.prices r WHERE {_WINDOW.format(column="r.loading_date")}) AS lineage_raw
"""


def fingerprint(conn: Connection, window: DateWindow) -> dict:
    row = conn.execute(text(FINGERPRINT_SQL), window.sql_params()).mappings().one()
    return dict(row)


def logical(fp: dict) -> dict:
    return {key: value for key, value in fp.items() if not key.startswith("lineage_")}
