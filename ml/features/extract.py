"""Reads the model population from validated core tables."""

from __future__ import annotations

import polars as pl
from sqlalchemy import text
from sqlalchemy.engine import Connection

from ml.features.spec import TARGET, SplitWindows

# Every column the feature pipeline may read, with its source expression. The target is read
# under its own name and leaves the pipeline through split_target only.
BASE_COLUMNS: dict[str, str] = {
    "price_id": "f.price_id",
    "loading_date": "f.loading_date",
    "origin_full_name": "o.full_name_local",
    "destination_full_name": "d.full_name_local",
    "origin_latitude": "o.latitude",
    "origin_longitude": "o.longitude",
    "destination_latitude": "d.latitude",
    "destination_longitude": "d.longitude",
    "distance": "f.distance_km::float8",
    "total_weight": "f.total_weight_t::float8",
    "total_volume": "f.total_volume_m3::float8",
    "body_group": "f.body_group_code",
    "weight_category": "f.weight_category::text",
    "volume_category": "f.volume_category::text",
}
TARGET_EXPRESSION = "f.price_rub::float8"

POPULATION_PREDICATE = """
    f.is_fact
    AND NOT f.complex
    AND NOT f.outlier_mark
    AND f.price_rub > 0
    AND f.distance_km > 0
    AND f.loading_date >= :train_start
    AND f.loading_date < :test_end
"""

POPULATION_SQL = f"""
SELECT {", ".join(f"{expr} AS {name}" for name, expr in BASE_COLUMNS.items())},
       {TARGET_EXPRESSION} AS {TARGET}
FROM core.fact_deal AS f
JOIN core.dim_city AS o ON o.gc_id = f.loading_city_gcid
JOIN core.dim_city AS d ON d.gc_id = f.unloading_city_gcid
WHERE {POPULATION_PREDICATE}
ORDER BY f.loading_date, f.price_id
"""

FUNNEL_SQL = """
SELECT count(*) AS in_window,
       count(*) FILTER (WHERE f.is_fact) AS is_fact,
       count(*) FILTER (WHERE f.is_fact AND NOT f.complex) AS direct,
       count(*) FILTER (WHERE f.is_fact AND NOT f.complex AND f.price_rub > 0 AND f.distance_km > 0) AS valid_measures,
       count(*) FILTER (WHERE f.is_fact AND NOT f.complex AND f.price_rub > 0 AND f.distance_km > 0
                        AND NOT f.outlier_mark) AS not_outlier
FROM core.fact_deal AS f
WHERE f.loading_date >= :train_start AND f.loading_date < :test_end
"""

QUARANTINE_SQL = """
SELECT reason_code, count(DISTINCT price_id) AS deals
FROM audit.quarantine_price
WHERE loading_date >= :train_start AND loading_date < :test_end
GROUP BY reason_code
ORDER BY reason_code
"""


def _params(split: SplitWindows) -> dict:
    return {"train_start": split.train_start, "test_end": split.test_end}


def read_population(conn: Connection, split: SplitWindows) -> pl.DataFrame:
    result = conn.execute(text(POPULATION_SQL), _params(split))
    frame = pl.DataFrame([tuple(row) for row in result], schema=list(result.keys()), orient="row",
                         infer_schema_length=None)
    return frame.with_columns(pl.col("loading_date").cast(pl.Date))


def read_funnel(conn: Connection, split: SplitWindows) -> dict:
    funnel = dict(conn.execute(text(FUNNEL_SQL), _params(split)).mappings().one())
    funnel["quarantined_in_window"] = {
        row.reason_code: row.deals for row in conn.execute(text(QUARANTINE_SQL), _params(split))
    }
    return funnel
