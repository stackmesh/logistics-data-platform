"""ClickHouse-side builds of the derived tables from the synced base tables.

The corridor reproduces mart.build_market_corridor of PostgreSQL over the same observation rows; the
eligibility rules are not repeated here, they stay in mart.market_corridor_observations.
"""

from __future__ import annotations

ROUTE_KEYS = "price_month, loading_city_gcid, unloading_city_gcid, body_group_code, weight_category"
CELL_JOIN = (
    "{alias}.loading_city_gcid = c.loading_city_gcid AND {alias}.unloading_city_gcid = c.unloading_city_gcid "
    "AND {alias}.body_group_code = c.body_group_code AND {alias}.weight_category = c.weight_category"
)


def route_month_stats_insert(database: str, target: str, partition: str) -> str:
    return f"""
INSERT INTO {database}.{target}
    ({ROUTE_KEYS}, observations, price_rub_sum, price_km_sum, max_price_date,
     trips, carriers, price_quantiles, price_km_quantiles, price_stddev)
SELECT {ROUTE_KEYS},
       count(),
       sum(price_rub),
       sum(price_km),
       max(price_date),
       uniqExactState(price_id),
       uniqExactIfState(organization_id, toUInt8(price_type IN (3, 9))),
       quantilesExactInclusiveState(0.25, 0.5, 0.75)(toFloat64(price_rub)),
       quantilesExactInclusiveState(0.25, 0.5, 0.75)(price_km),
       stddevSampStableState(toFloat64(price_rub))
FROM {database}.fact_price_observation
WHERE toYYYYMM(price_month) = {int(partition)}
GROUP BY {ROUTE_KEYS}
"""


def corridor_cells_sql(database: str) -> str:
    """Mart cells from merged route_month_stats states. Any coarser grain is the same query with fewer keys."""
    return f"""
SELECT {ROUTE_KEYS},
       sum(observations) AS n,
       sum(price_rub_sum) AS rub_sum,
       max(max_price_date) AS max_date,
       uniqExactMerge(trips) AS trips_count,
       uniqExactIfMerge(carriers) AS carriers_count,
       quantilesExactInclusiveMerge(0.25, 0.5, 0.75)(price_quantiles) AS q,
       quantilesExactInclusiveMerge(0.25, 0.5, 0.75)(price_km_quantiles) AS qk,
       stddevSampStableMerge(price_stddev) AS sd
FROM {database}.route_month_stats
GROUP BY {ROUTE_KEYS}
"""


def market_corridor_insert(database: str, target: str) -> str:
    """{as_of:Date} is a server-side query parameter."""
    prev = CELL_JOIN.format(alias="prev")
    ly = CELL_JOIN.format(alias="ly")
    return f"""
INSERT INTO {database}.{target}
WITH cell AS ({corridor_cells_sql(database)})
SELECT c.price_month, c.loading_city_gcid, c.unloading_city_gcid, c.body_group_code, c.weight_category,
       oc.name, dc.name, g.body_group_name,
       c.q[1], c.q[2], c.q[3],
       toFloat64(c.rub_sum) / c.n,
       c.qk[1], c.qk[2], c.qk[3],
       c.trips_count, c.carriers_count,
       if(c.n < 2 OR c.rub_sum = 0, NULL, c.sd / (toFloat64(c.rub_sum) / c.n)),
       if(prev.median IS NULL OR prev.median = 0, NULL, (c.q[2] / prev.median - 1) * 100),
       if(ly.median IS NULL OR ly.median = 0, NULL, (c.q[2] / ly.median - 1) * 100),
       c.max_date,
       toInt32(dateDiff('day', c.max_date, {{as_of:Date}})),
       multiIf(c.trips_count >= 30 AND c.carriers_count >= 5 AND dateDiff('day', c.max_date, {{as_of:Date}}) <= 30,
               'HIGH', c.trips_count >= 10, 'MEDIUM', 'LOW_DATA'),
       {{as_of:Date}}
FROM cell AS c
INNER JOIN {database}.dim_city AS oc ON oc.gc_id = c.loading_city_gcid
INNER JOIN {database}.dim_city AS dc ON dc.gc_id = c.unloading_city_gcid
INNER JOIN {database}.dim_body_group AS g ON g.body_group_code = c.body_group_code
LEFT JOIN (SELECT addMonths(price_month, 1) AS next_month, loading_city_gcid, unloading_city_gcid,
                  body_group_code, weight_category, q[2] AS median FROM cell) AS prev
       ON prev.next_month = c.price_month AND {prev}
LEFT JOIN (SELECT addMonths(price_month, 12) AS next_month, loading_city_gcid, unloading_city_gcid,
                  body_group_code, weight_category, q[2] AS median FROM cell) AS ly
       ON ly.next_month = c.price_month AND {ly}
SETTINGS join_use_nulls = 1
"""


def prediction_daily_insert(database: str, target: str, partition_literal: str) -> str:
    error = "(predicted_price_rub - actual_price_rub)"
    return f"""
INSERT INTO {database}.{target}
SELECT model_name, model_version,
       if(split_role = 'test', 'historical', 'future') AS record_type,
       loading_date, loading_city_gcid, unloading_city_gcid, body_group,
       count(),
       count(actual_price_rub),
       sum(ifNull(actual_price_rub, 0)),
       sum(predicted_price_rub),
       sum(ifNull({error}, 0)),
       sum(ifNull(abs_error_rub, 0)),
       sum(ifNull(toDecimal128({error}, 2) * toDecimal128({error}, 2), 0)),
       sum(ifNull(ape_pct, 0))
FROM {database}.ml_price_predictions FINAL
WHERE model_version = {partition_literal}
GROUP BY model_name, model_version, record_type, loading_date, loading_city_gcid, unloading_city_gcid, body_group
"""
