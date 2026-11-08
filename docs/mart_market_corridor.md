# Market corridor mart

`mart.mart_market_corridor` is the market price band for one calendar month, one city pair, one body group, and one weight category. It is built only from validated core tables (`core.fact_deal`, `core.fact_deal_date`, `core.bridge_deal_body_group`, `core.dim_city`, `core.dim_body_group`). It does not read `raw`, the source database, or the source's precomputed `price_group_stats`.

The design rationale is in [mart_design.md](mart_design.md). That document calls the table `mart.route_card`. The implemented name is `mart.mart_market_corridor`.

## Objects

| Object | File | Purpose |
|---|---|---|
| `mart.mart_market_corridor` | `sql/mart/001_market_corridor.sql` | The published table |
| `mart.build_market_corridor(as_of date)` | same | The transformation query as a `STABLE` SQL function. It returns the rows without writing them |
| `mart.refresh_market_corridor(as_of, refresh_id, ...)` | same | Replaces the table contents with `build_market_corridor(as_of)` |
| `audit.mart_refresh` | same | One row per refresh attempt: status, row counts, error |
| `etl/mart/refresh.py` | | Python task and CLI around the procedure |
| `sql/mart/examples.sql` | | The example queries below |

`python -m etl.migrate` applies `sql/core/*` and then `sql/mart/*`. Only numbered files are migrations.

## Grain and key

Primary key: `(price_month, loading_city_gcid, unloading_city_gcid, body_group_code, weight_category)`.

- `price_month` is `date_trunc('month', price_date)` over `core.fact_deal_date`. A deal with price dates in two months contributes to both months.
- Cities are `core.dim_city.gc_id`. The dimension has one row per `gc_id`, so the duplicate source rows for Moscow (1001) and Kazan (1003) cannot multiply deals. Names come from the dimension row.
- Body group comes from `core.bridge_deal_body_group`. That table already prefers `price_body_types`, falls back to `prices.body_type`, and marks a missing body type as `UNSPECIFIED`. It holds one row per deal and group, so a deal with child types 56, 57 and 61 is one Тент observation, while a deal with 56 and 15 is one Тент and one Борт observation.
- One observation is one deal in one month in one body group. Two price dates of the same deal in the same month collapse to one observation, dated by the later date.

## Eligibility

A deal is an observation only if all of these hold:

- `is_fact` is true.
- `price_type` is in 1, 2, 3, 4, 5 or 9.
- `complex` is false.
- `outlier_mark` is false.
- `price_rub` > 0 and `distance_km` > 0.
- Its currency is 643, 840 or 978, with a positive FX rate.
- The price date is on or before `as_of`.

Price dates after `as_of` are excluded, so future planned months never appear.

## Metrics

| Column | Definition |
|---|---|
| `p25_trip`, `median_trip`, `p75_trip` | `percentile_cont(0.25 / 0.5 / 0.75)` of `price_rub`, rounded to 0.01 |
| `avg_trip` | `avg(price_rub)` |
| `p25_km`, `median_km`, `p75_km` | `percentile_cont` of `price_rub / distance_km` per observation, rounded to 0.0001 |
| `trips_count` | `count(DISTINCT price_id)` |
| `carriers_count` | `count(DISTINCT organization_id)` over observations with `price_type` 3 or 9 |
| `cv_rate` | `stddev_samp / avg` of `price_rub`. NULL with fewer than 2 observations or a zero mean |
| `mom_pct` | `(median / median of the same key in the previous calendar month − 1) × 100`. NULL if that exact cell does not exist or its median is 0 |
| `yoy_pct` | The same against the same calendar month one year earlier |
| `max_price_date` | Latest eligible price date in the cell |
| `freshness_days` | `as_of_date − max_price_date` |
| `reliability_level` | HIGH if trips ≥ 30, carriers ≥ 5 and freshness ≤ 30. Otherwise MEDIUM if trips ≥ 10. Otherwise LOW_DATA |

`mom_pct` and `yoy_pct` are exact joins on the shifted month. A gap is NULL. The last available month is never substituted.

LOW_DATA cells are stored, not dropped. Consumers filter on `reliability_level`.

## Refresh

```bash
python -m etl.mart.refresh                     # as_of = AS_OF_DATE (default 2026-10-26)
python -m etl.mart.refresh --as-of 2026-10-26
```

The compose `pipeline` service runs it after the full ETL load.

A refresh runs in four steps:

1. Insert a `running` row into `audit.mart_refresh` and commit it.
2. In a single transaction:
   - Take the same advisory lock as the core ETL, so a refresh never reads a half-loaded core.
   - `DELETE` every mart row.
   - `INSERT` the output of `build_market_corridor(as_of)`.
   - `ANALYZE` the mart.
   - Mark the audit row `succeeded` with the deleted and inserted counts.
3. Commit.
4. On any error the transaction rolls back, the previous mart contents stay visible, and the audit row is marked `failed` with the error.

Readers keep seeing the old rows until the commit, because the refresh uses `DELETE`, not `TRUNCATE`. The cost is that the heap briefly holds two versions of every row. After repeated refreshes the table settles at about 125 MB of heap plus 48 MB of indexes for 203,644 rows. Autovacuum reclaims the dead rows, and the space is reused rather than growing.

Every row carries `as_of_date` and the `refresh_id` that wrote it.

## Indexes

| Index | Serves |
|---|---|
| Primary key | Point lookup of a cell, and the MoM/YoY self-join in the build |
| `(loading_city_gcid, unloading_city_gcid, body_group_code, weight_category, price_month)` | One route's history (example 1b: 0.5 ms) |
| `(price_month, reliability_level)` | Network views of a month filtered by reliability (index-only count of one month: 2.6 ms) |

## Real warehouse results

Refreshed against the full core load: 299,797 deals and as_of 2026-10-26.

| Measure | Value |
|---|---|
| Cells | 203,644 |
| Months | 37 (2023-10 to 2026-10) |
| City pairs | 1,560 |
| Observations (sum of `trips_count`) | 291,020 |
| HIGH / MEDIUM / LOW_DATA | 10 / 924 / 202,710 |
| Cells with `mom_pct` / `yoy_pct` | 75,986 / 42,624 |
| Refresh time | 12.1 s |
| Repeated refresh | Same row count and content hash. Only `refresh_id` changes |

The market is thin at this grain. 159,375 cells (78%) hold a single deal. In 2026-10, 99.0% of cells are LOW_DATA, and only 7 cells are HIGH. 2026-10 is a partial month: it contains price dates up to 2026-10-26.

`carriers_count` counts only price types 3 and 9. A cell of type-1 deals has trips but zero carriers. For example, Moscow → Kazan, Изотерм, 5T has 6 trips and 0 carriers in 2026-10.

## Example queries

All six queries are in `sql/mart/examples.sql`. Run them with `psql -f sql/mart/examples.sql`. The results below come from the real warehouse. Long outputs are shortened here; the file returns 20 rows each.

### 1. Moscow → Kazan

1a lists every body group and weight category in 2026-10. It returns 28 rows; the top rows are:

| Body | Weight | Trips | Carriers | p25 | Median | p75 | Median / km | MoM % | YoY % | Reliability |
|---|---|---|---|---|---|---|---|---|---|---|
| Тент | FTL | 37 | 9 | 84,734.66 | 95,018.57 | 104,907.97 | 105.9293 | 1.2629 | 10.1330 | HIGH |
| Рефрижератор | FTL | 14 | 6 | 103,273.00 | 122,729.10 | 131,353.15 | 136.8217 | 3.3520 | 6.4753 | MEDIUM |
| Тент | L5T | 11 | 3 | 36,389.44 | 37,245.48 | 42,067.54 | 41.5223 | 3.8816 | 4.3290 | MEDIUM |
| Изотерм | FTL | 9 | 3 | 95,712.58 | 109,379.33 | 126,411.16 | 121.9391 | 8.9313 | −2.1667 | LOW_DATA |
| Борт | L5T | 5 | 1 | 36,126.64 | 37,234.48 | 37,245.48 | 41.5100 | −2.9737 | NULL | LOW_DATA |

Борт L5T has no 2025-10 cell, so `yoy_pct` is NULL. The Изотерм G20T and Рефрижератор G20T rows both show one trip at 148,781.77: that is one deal with child types in two groups.

1b is the Тент FTL history for 2025-10 to 2026-10:

| Month | Trips | Carriers | Median | MoM % | YoY % | Freshness | Reliability |
|---|---|---|---|---|---|---|---|
| 2025-10 | 15 | 9 | 86,276.22 | 3.5773 | 11.7641 | 360 | MEDIUM |
| 2026-01 | 22 | 12 | 101,149.19 | 12.1237 | 25.4528 | 268 | MEDIUM |
| 2026-04 | 19 | 8 | 82,552.93 | −11.9073 | 15.6882 | 180 | MEDIUM |
| 2026-07 | 25 | 10 | 87,712.22 | −0.0149 | 10.0509 | 87 | MEDIUM |
| 2026-09 | 29 | 10 | 93,833.58 | 5.1034 | 12.6501 | 27 | MEDIUM |
| 2026-10 | 37 | 9 | 95,018.57 | 1.2629 | 10.1330 | 0 | HIGH |

The query returns all 13 months. 2026-09 misses HIGH by one trip (29 < 30). Earlier months also fail the freshness rule.

### 2. Top 20 most expensive routes

These are 2026-10 cells with MEDIUM or HIGH reliability, ranked by median trip price.

| Route | Body | Weight | Median | p25 | p75 | Median / km | Trips | Reliability |
|---|---|---|---|---|---|---|---|---|
| Москва → Хабаровск | Рефрижератор | FTL | 981,035.12 | 881,895.23 | 1,079,518.92 | 127.8388 | 16 | MEDIUM |
| Москва → Владивосток | Тент | FTL | 838,237.47 | 776,020.77 | 918,665.63 | 104.5314 | 14 | MEDIUM |
| Москва → Хабаровск | Тент | FTL | 785,707.96 | 733,917.31 | 842,251.08 | 102.3857 | 23 | MEDIUM |
| Москва → Иркутск | Тент | FTL | 522,402.98 | 444,693.82 | 574,856.13 | 99.4296 | 17 | MEDIUM |
| Краснодар → Новосибирск | Тент | FTL | 430,350.03 | 375,696.73 | 443,591.63 | 105.3746 | 10 | MEDIUM |
| … | | | | | | | | |
| Москва → Новосибирск | Тент | FTL | 346,362.04 | 295,772.08 | 399,583.33 | 98.5103 | 38 | HIGH |
| Санкт-Петербург → Краснодар | Тент | FTL | 222,116.07 | 197,587.28 | 241,525.74 | 101.3303 | 23 | MEDIUM |

Rank 13 is Moscow → Novosibirsk and rank 20 is St Petersburg → Krasnodar. The ranking by trip price follows distance; the price per km stays around 96–128 RUB.

### 3. LOW_DATA routes

3a is the reliability mix in 2026-10:

| Reliability | Cells | Observations | % of cells |
|---|---|---|---|
| LOW_DATA | 9,854 | 15,560 | 99.0 |
| MEDIUM | 90 | 1,397 | 0.9 |
| HIGH | 7 | 351 | 0.1 |

3b lists the LOW_DATA cells closest to MEDIUM. All of the top 20 have 9 trips. Some already have 5 carriers and are fresh, so one more deal would make them MEDIUM:

| Route | Body | Weight | Trips | Carriers | Median | CV | Freshness |
|---|---|---|---|---|---|---|---|
| Москва → Самара | Тент | 10T | 9 | 5 | 76,734.26 | 0.196022 | 0 |
| Новосибирск → Уфа | Тент | FTL | 9 | 5 | 211,552.58 | 0.174078 | 1 |
| Уфа → Санкт-Петербург | Тент | FTL | 9 | 5 | 179,778.75 | 0.209895 | 0 |
| Екатеринбург → Казань | Тент | FTL | 9 | 4 | 94,159.11 | 0.162277 | 0 |
| Москва → Владивосток | Рефрижератор | FTL | 9 | 3 | 1,092,263.14 | 0.123835 | 0 |

### 4. Highest month-over-month increase

These are 2026-10 MEDIUM and HIGH cells with an exact 2026-09 cell. The previous-month median is derived from `mom_pct`.

| Route | Body | Weight | Median | Previous month | MoM % | YoY % | Trips | Reliability |
|---|---|---|---|---|---|---|---|---|
| Санкт-Петербург → Ижевск | Тент | FTL | 181,788.79 | 136,364.94 | 33.3105 | 17.8606 | 13 | MEDIUM |
| Москва → Нижний Новгород | Борт | FTL | 57,168.23 | 47,928.20 | 19.2789 | 20.0138 | 10 | MEDIUM |
| Москва → Рязань | Тент | FTL | 33,571.04 | 28,410.98 | 18.1622 | 39.3659 | 16 | MEDIUM |
| Барнаул → Москва | Тент | FTL | 410,151.68 | 347,430.71 | 18.0528 | 23.1873 | 10 | MEDIUM |
| Санкт-Петербург → Новосибирск | Тент | FTL | 415,240.54 | 359,583.80 | 15.4781 | 18.8533 | 12 | MEDIUM |
| Москва → Нижний Новгород | Тент | FTL | 61,686.41 | 53,874.55 | 14.5001 | 29.5781 | 34 | HIGH |

The HIGH cell, Moscow → Nizhny Novgorod Тент FTL, is rank 7. Most of the top rises are on 10–16 trips, so they are MEDIUM evidence.

### 5. Trip price versus kilometre price

These are 2026-10 MEDIUM and HIGH cells, ranked by median RUB per km and compared with their rank by trip price.

| Route | Body | Weight | Median trip | Median / km | Implied km | Trip rank | km rank | Trips |
|---|---|---|---|---|---|---|---|---|
| Москва → Рязань | Тент | FTL | 33,571.04 | 145.9610 | 230 | 95 | 1 | 16 |
| Москва → Тверь | Тент | FTL | 28,828.54 | 142.7155 | 202 | 96 | 2 | 11 |
| Екатеринбург → Челябинск | Тент | FTL | 33,999.37 | 140.4933 | 242 | 94 | 3 | 10 |
| Москва → Санкт-Петербург | Рефрижератор | FTL | 110,171.82 | 139.2817 | 791 | 60 | 5 | 19 |
| Москва → Хабаровск | Рефрижератор | FTL | 981,035.12 | 127.8388 | 7,674 | 1 | 9 | 16 |
| Москва → Тула | Тент | FTL | 27,739.10 | 127.8300 | 217 | 97 | 10 | 21 |

There are 97 ranked cells. The cheapest trips have the most expensive kilometres: short Тент hauls near Moscow rank 94–97 by trip price and 1–3 by price per km. On the 7 routes where both Тент FTL and Рефрижератор FTL are MEDIUM or better, Рефрижератор costs more per km on every one, for example 136.8 vs 105.9 RUB/km on Moscow → Kazan. The most expensive trip, Moscow → Khabarovsk, is only 9th by price per km.

### 6. Largest carrier competition

These are 2026-10 cells ranked by distinct carriers.

| Route | Body | Weight | Carriers | Trips | Carriers / trip | Median | CV | Reliability |
|---|---|---|---|---|---|---|---|---|
| Москва → Ростов-на-Дону | Тент | FTL | 39 | 112 | 0.35 | 126,991.51 | 0.171357 | HIGH |
| Москва → Санкт-Петербург | Тент | FTL | 18 | 47 | 0.38 | 88,023.20 | 0.401762 | HIGH |
| Москва → Ростов-на-Дону | Рефрижератор | FTL | 14 | 49 | 0.29 | 163,418.54 | 0.156141 | HIGH |
| Москва → Новосибирск | Тент | FTL | 14 | 38 | 0.37 | 346,362.04 | 0.203229 | HIGH |
| Москва → Краснодар | Тент | FTL | 11 | 26 | 0.42 | 152,112.53 | 0.167186 | MEDIUM |
| Москва → Казань | Тент | FTL | 9 | 37 | 0.24 | 95,018.57 | 0.175255 | HIGH |

Moscow → Rostov-on-Don Тент FTL is the busiest cell in the snapshot. Moscow → St Petersburg Тент FTL has the widest dispersion among the HIGH cells (CV 0.40).

## Tests

`tests/test_mart_market_corridor.py` (19 tests, integration). The full-mart, snapshot-shape and refresh tests run on the real warehouse. The fixture tests load deals through the real ETL (`transform_snapshot` and `load_snapshot`) with 2099 dates and gc_ids 960001+, call `build_market_corridor('2099-06-15')`, and roll back.

| Test | What it proves |
|---|---|
| Every cell matches an independent reference | A Polars recomputation from core matches the whole mart, all 203,644 cells: same key set, exact counts, freshness and reliability, percentiles within the rounding step, exact-month MoM/YoY |
| Snapshot shape | No month after 2026-10, no `max_price_date` after as_of, no duplicate keys, no reads from raw, the source, or `price_group_stats` |
| Hand-computed percentiles | Trips of 100k, 120k, 150k and 210k give p25 115,000, median 135,000, p75 165,000, and the matching per-km values and CV |
| numpy reference percentiles | 7 random prices match `numpy.percentile(method="linear")` |
| Duplicate gc_id | A second source row for gc 960001 yields one dimension row and does not multiply the 4 deals |
| Child body types | 56+57+61 is one Тент observation; 56+15 is Тент and Борт |
| Exact MoM/YoY | Feb vs Jan is +10% and vs the previous Feb +25%. April with no March and no previous April is NULL for both. A 10T cell in March does not feed FTL |
| Reliability boundaries | 30/5/30 days is HIGH. 31 days, 29 trips, 4 carriers or 10 trips is MEDIUM. 9 trips is LOW_DATA |
| Carriers | Types 3 and 9 only, distinct organizations |
| Future dates | The as-of day is included; the next day and later months are excluded |
| Filters | Outlier, complex and non-fact type 6 deals are dropped. A single observation has NULL CV |
| Two dates in one month | Count once, dated by the later date |
| Repeated refresh | Two more refreshes give the same content hash and row count, one `refresh_id` in the table, and audit rows that succeeded |
| Failed refresh | A refresh that fails after its DELETE rolls back and leaves the previous mart intact |

Not implemented in this stage: Superset and ClickHouse.
