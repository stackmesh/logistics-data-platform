# ClickHouse analytical copy

ClickHouse 26.8 LTS serves the heavy analytical reads: route pricing, monthly market corridors, percentiles over
long date ranges, and ML error analysis. PostgreSQL 17 remains the source and the authoritative Core DWH:
nothing is written to ClickHouse except by `etl.clickhouse.sync`, which rebuilds it from PostgreSQL, and every
ClickHouse result is checked against the PostgreSQL result it replaces.

| File | Purpose |
|---|---|
| `clickhouse/sql/001_schema.sql` | Database and tables; `{database}` is substituted. Idempotent |
| `etl/clickhouse/schema.py` | Synced tables: columns, PostgreSQL sources, partition keys, the cross-system row fingerprint |
| `etl/clickhouse/sync.py` | `python -m etl.clickhouse.sync`: incremental, full, and backfill loads |
| `etl/clickhouse/derived.py` | ClickHouse builds of route statistics, the market corridor, and daily prediction errors |
| `etl/clickhouse/check.py` | `python -m etl.clickhouse.check`: consistency against PostgreSQL with documented tolerances |
| `etl/clickhouse/migrate.py` | `python -m etl.clickhouse.migrate`: applies the DDL (the sync does this too) |
| `scripts/benchmark_clickhouse.py` | PostgreSQL vs ClickHouse on identical datasets and queries |
| `tests/test_clickhouse.py` | 13 unit and 8 integration tests |

## 1. What moved and what stayed

```
PostgreSQL (authoritative)                               ClickHouse (derived, rebuildable)
core.fact_deal ─────────────────────── partitions ─────▶ fact_deal
mart.market_corridor_observations(as_of) ── partitions ─▶ fact_price_observation ─▶ route_month_stats ─▶ market_corridor
ml.ml_price_predictions ──── watermark + partitions ────▶ ml_price_predictions ─▶ ml_prediction_daily
core.dim_city, core.dim_body_group ──── whole table ────▶ dim_city, dim_body_group
mart.mart_market_corridor, ml.v_prediction_error_summary ◀── compared by etl.clickhouse.check
```

Business rules are not duplicated. Which deals enter the corridor (completed, direct, not complex, not an outlier,
positive price and distance, supported currency, price date on or before the snapshot) is decided only by
`mart.market_corridor_observations` in PostgreSQL. ClickHouse receives its output and only aggregates it with
the same definitions as `mart.build_market_corridor`. The corridor's snapshot date is the `as_of_date` of the
current PostgreSQL mart, so both corridors describe the same snapshot. PostgreSQL keeps the mart, the ML
tables, the BI views, and Superset's connection unchanged.

## 2. Table engines

Engines were chosen by how each table changes and how it is aggregated.

| Table | Engine | Changes by | Why this engine | Rejected alternatives |
|---|---|---|---|---|
| `fact_deal` | MergeTree | partition replacement | A deal can change route or loading month. Each month is replaced atomically from a verified stage, so exactly one version of every deal exists and no query needs `FINAL` | ReplacingMergeTree collapses versions only on merge and only within a sorting key; a deal that moved to another route or month would keep its old row |
| `fact_price_observation` | MergeTree | partition replacement | Same as `fact_deal`; this is the raw population for exact percentiles | Same as above |
| `route_month_stats` | AggregatingMergeTree | partition rebuild from observations | Stores partial aggregate states per mart cell: `uniqExact` trips, `uniqExactIf` carriers, `quantilesExactInclusive` of trip and per-km price, `stddevSampStable`. States merge exactly, so a route across months or all body groups gets exact distinct counts and exact percentiles without the raw rows | SummingMergeTree can keep only additive columns: summed distinct counts double-count deals that span months, and percentiles cannot be summed. Plain MergeTree of final values cannot be rolled up |
| `market_corridor` | MergeTree | whole-table rebuild and `EXCHANGE TABLES` | A derived snapshot with the mart's columns. It is rebuilt in seconds from `route_month_stats` and swapped atomically | AggregatingMergeTree would push `-Merge` calls into every BI query; ReplacingMergeTree is not needed when the table is swapped as a whole |
| `ml_price_predictions` | ReplacingMergeTree(`scored_at`) | overlapping watermark inserts, then partition reconciliation | Incremental loads may re-read rows; a re-scored deal arrives with a later `scored_at`. The sorting key `(model_name, model_version, price_id)` is the PostgreSQL primary key, which never changes for a row, so a merge keeps exactly the latest version | MergeTree would keep every re-read row. CollapsingMergeTree needs sign rows, that is, the previous state of every changed row |
| `ml_prediction_daily` | SummingMergeTree | partition rebuild from predictions | Error components (counts, sums of actual, predicted, signed, absolute, squared error, APE) are additive, so MAE, RMSE, MAPE, and bias at any grain are ratios of sums | AggregatingMergeTree would also work, at higher storage and query cost for purely additive values |
| `dim_city`, `dim_body_group` | MergeTree | whole-table `EXCHANGE TABLES` | 40 and 6 rows | — |
| `sync_watermark` | ReplacingMergeTree(`updated_at`) | one row per run | The latest watermark per source wins | — |
| `sync_log` | MergeTree | append | Audit trail of partition actions | — |

Trade-offs that remain:

- ReplacingMergeTree only removes duplicates during background merges. `ml_price_predictions` must be read with
  `FINAL`, as the sync, the derived build, and the checks do. Without `FINAL` a query can count a row twice.
- SummingMergeTree adds up any rows with the same key, so inserting the same data twice would double the sums.
  `ml_prediction_daily` is safe only because its partitions are rebuilt and replaced, never appended to. Do not
  insert into it directly.
- AggregatingMergeTree states are opaque. Read them with the `-Merge` functions; `derived.corridor_cells_sql`
  is the reference query. `uniqExact` and `quantilesExact*` states hold the values themselves:
  `route_month_stats` (11.5 MiB) is larger than the observations it summarizes (7.8 MiB). It pays off in exact
  rollups, not in storage. Approximate states (`uniq`, `quantileTDigest`) would be smaller but would no longer
  match PostgreSQL exactly.

## 3. Partitioning

| Table | Partition key | Partitions now | Reason |
|---|---|---|---|
| `fact_deal` | `toYYYYMM(loading_date)` | 41 | The unit of change and verification; date-range queries prune whole months |
| `fact_price_observation`, `route_month_stats`, `market_corridor` | `toYYYYMM(price_month)` | 37 | The mart's month; a changed month is rebuilt alone |
| `ml_price_predictions`, `ml_prediction_daily` | `model_version` | 1 | Each scoring run replaces a model version as a unit; old versions can be dropped as partitions |

Monthly partitions keep parts large: about 7,000 rows per partition today. With 10× the data, partitions still hold
tens of thousands of rows. Daily partitions would create thousands of tiny parts and slow merges. Partitions are
not a query index in ClickHouse; the sorting key is.

## 4. Sorting keys

| Table | `ORDER BY` | Serves |
|---|---|---|
| `fact_deal` | `loading_city_gcid, unloading_city_gcid, body_group_code, weight_category, loading_date, price_id` | Route filtering: one route reads a narrow range of granules (see `route_filter` below) |
| `fact_price_observation`, `route_month_stats`, `market_corridor` | `loading_city_gcid, unloading_city_gcid, body_group_code, weight_category, price_month[, price_id]` | Route, then body group and weight category, then time, the corridor's own grain. In the aggregating table this is also the identity of a cell |
| `ml_price_predictions` | `model_name, model_version, price_id` | The PostgreSQL primary key; the dedup identity of ReplacingMergeTree |
| `ml_prediction_daily` | `model_name, model_version, record_type, loading_date, loading_city_gcid, unloading_city_gcid, body_group` | The summing identity; version and record-type filters first |

Low-cardinality columns come first (40 cities, 6 body groups, 6 weight categories), which also improves
compression. Strings with few values are `LowCardinality(String)`. Money and distances keep PostgreSQL's
decimal scale (`Decimal(18,2)`, `Decimal(12,3)`); per-km price is `Float64` because PostgreSQL computes its
percentiles in double precision as well.

## 5. Data ingestion

### 5.1 One run

1. Apply the DDL (idempotent).
2. Take the PostgreSQL advisory lock `hashtext('logistics_dwh.clickhouse_sync')`. A second concurrent sync exits
   with code 1 rather than waiting.
3. Open one `REPEATABLE READ` transaction. Every read of the run, including the consistency checks, sees the same
   snapshot, so a core load committing during the sync cannot produce a half-old, half-new copy. Inside the
   snapshot, materialize `mart.market_corridor_observations(as_of)` once into a temporary table.
4. For every table, compute a fingerprint per partition in both systems and plan:

   | PostgreSQL | ClickHouse | Action |
   |---|---|---|
   | present | missing | load (`missing_in_clickhouse`) |
   | present | different | load (`fingerprint_mismatch`) |
   | present | equal | skip, or load if forced (`--full`, `--months`) |
   | missing | present | drop the ClickHouse partition (`absent_in_postgres`) |

5. Load a partition:
   - Stream the partition from PostgreSQL with a server-side cursor, `CLICKHOUSE_INSERT_BATCH_ROWS` rows at a
     time (default 50,000), into `<table>_stage`. Rows stream in a deterministic order by primary key.
   - Verify the stage fingerprint against PostgreSQL. On mismatch, stop. The published partition is untouched.
   - Publish with `ALTER TABLE ... REPLACE PARTITION ... FROM <table>_stage`. This is atomic for readers: a query
     sees either the old or the new partition, never a mix or a gap. Dimensions are swapped with `EXCHANGE TABLES`.
   - Verify the published partition again, then truncate the stage. Every action is written to `sync_log` with
     both fingerprints.
6. Rebuild derived partitions: route statistics for changed observation months, the corridor when anything it
   reads changed, and daily prediction sums for changed model versions. Each rebuild also self-heals: a month
   whose statistic row count disagrees with the observations is rebuilt even if nothing was loaded.
7. Write the prediction watermark, run the consistency checks in the same snapshot, then release the lock.

### 5.2 The fingerprint

A partition's fingerprint is its row count plus the sum of a 60-bit row hash. Each row is rendered to the same
canonical text in both systems: integers in decimal, dates as days since 1970, decimals with their fixed scale,
booleans as 0/1, timestamps as epoch seconds or microseconds, NULL as `\N`, joined by `|`. The hash is the first
15 hex digits of the text's md5. The sum is order independent and detects a lost, extra, duplicated, or changed
row even when the count is unchanged. Floats are not hashed: the only float column, `price_km`, is derived from
hashed columns. Equality was verified on the full dataset before any loader was written: all 81 partitions and
608,562 rows hash identically in both systems.

### 5.3 Incremental, repeatable, backfill

- **Incremental (default):** only partitions whose fingerprint differs are transferred. A run with no source
  changes transfers nothing; comparing the 81 partition fingerprints takes about 3 s.
- **Repeatable:** a re-run converges to the same state. Loading a partition replaces it whole, so running twice
  never appends twice.
- **Full:** `--full` reloads every partition and rebuilds every derived table.
- **Backfill:** `--months 2024-01..2024-06` (or one month, `--months 2025-03`) forces those months of
  `fact_deal` and `fact_price_observation` and rebuilds their statistics and the corridor. Months outside the
  range are compared as usual.
- **Dry run:** `--plan` prints the plan and changes no data. It still applies the idempotent DDL.

### 5.4 Predictions: watermark plus reconciliation

`ml.upsert_price_predictions` rewrites `scored_at` only for changed rows, and deletes stale rows. A watermark on
`scored_at` alone would miss two cases: a scoring transaction that commits after a later one, whose `scored_at`
is its start time and therefore older than the watermark, and deletes, which leave no row behind. So:

1. Fingerprint the window `(watermark − CLICKHOUSE_WATERMARK_OVERLAP_S, watermark]` in both systems (default
   overlap 1 hour). If it differs, a late row exists, so re-read from the start of the window; otherwise read
   strictly after the watermark.
2. Insert those rows directly into `ml_price_predictions`. Re-read rows have the same key and version and
   collapse under `FINAL` and on merge.
3. Compare every model version's fingerprint (with `FINAL`) with PostgreSQL. A partition that still differs
   because of a delete, or a late commit older than the overlap, is reloaded through the stage like any other.
   Nothing that differs survives the run.

## 6. Deduplication behavior

| Where a duplicate could come from | What prevents it |
|---|---|
| A retried INSERT after a network error | Each batch has `insert_deduplication_token = run:table:partition:batch`. A stored block is not stored again (`non_replicated_deduplication_window` on the stage tables and on `ml_price_predictions`). Up to 3 attempts with backoff |
| Re-running a load | Partition replacement: the partition is replaced, not appended to |
| A deal changing route or month | The old month's fingerprint changes too, so both months are reloaded; the deal exists only in its new month |
| Overlapping prediction reads | ReplacingMergeTree on the PostgreSQL primary key; `FINAL` in every read |
| A row deleted in PostgreSQL | Partition fingerprints differ, so the partition is reloaded or dropped |
| Rows edited directly in ClickHouse | Same: detected on the next run and replaced from PostgreSQL. An integration test deletes, duplicates, and plants rows, then checks the repair |
| Summing the same rows twice in `ml_prediction_daily` | Partitions are rebuilt and replaced only, never appended |

Failure semantics: a run either publishes a partition that matches PostgreSQL, or fails with exit code 1 and
leaves the previous verified partition in place. Partitions published before a failure are each verified
individually; the next run picks up the rest. An integration test drops one row per batch during a load and
asserts that the sync refuses to publish, leaves the partition unchanged, and logs the failure. No record is lost
silently.

## 7. Consistency with PostgreSQL

`python -m etl.clickhouse.check`, also run at the end of every sync, compares in one snapshot:

| Check | Rule |
|---|---|
| Base tables (`fact_deal`, `fact_price_observation`, `ml_price_predictions`, both dimensions) | Every partition fingerprint equal. Exact |
| `route_month_stats` | Observation count per month equals the observations table. Exact |
| `market_corridor` vs `mart.mart_market_corridor` | Same 5-column keys; equal names, `trips_count`, `carriers_count`, `max_price_date`, `freshness_days`, `reliability_level`, `as_of_date`, and NULL-ness of `cv_rate`, `mom_pct`, `yoy_pct`; numeric values within the tolerances below |
| `ml_prediction_daily` vs `ml.v_prediction_error_summary` | Equal deals and first/last loading dates per model version and record type; means within the tolerances below |

Numerical tolerances. PostgreSQL stores corridor values rounded; ClickHouse keeps the unrounded double. The
tolerance is half a unit of the last stored decimal, plus `1e-9 × |value|` for floating-point rounding:

| Field | PostgreSQL type | Absolute tolerance | Largest observed difference |
|---|---|---|---|
| `p25_trip`, `median_trip`, `p75_trip`, `avg_trip` | numeric(18,2) | 0.005 | 0.0050000001 |
| `p25_km`, `median_km`, `p75_km` | numeric(18,4) | 0.00005 | 0.0000500000 |
| `cv_rate` | numeric(12,6) | 0.0000005 | 0.00000049999 |
| `mom_pct`, `yoy_pct` | numeric(12,4) | 0.00005 | 0.0000499999 |
| prediction means: actual, predicted, MAE, RMSE, bias | numeric, rounded to 2 | 0.005 | 0.0044 |
| MAPE | numeric, rounded to 4 | 0.00005 | 0.0000469 |

The largest differences sit exactly at the half-unit bound, as expected for rounded against unrounded values
(for example a median of two prices, `x.xx5`). The relative term covers the binary representation of such values.
A stricter test also passes: rounding each ClickHouse value the way PostgreSQL does (`float8` to `numeric` at
15 significant digits, half away from zero) reproduces the stored mart value in all 203,644 cells for all 10
numeric fields, with 0 differences (`rounding_flips` in the report). The percentile inputs are the same doubles in
both systems: `toFloat64(price_rub)` in ClickHouse equals PostgreSQL's `price_rub::float8` for all 291,020
observations, and `quantileExactInclusive` interpolates like `percentile_cont`.

Latest result (snapshot 2026-10-26, clean warehouse):

| Check | Result |
|---|---|
| Fingerprints | 5 tables, 81 partitions, 608,562 rows: all equal |
| Route statistics | 37 of 37 months equal |
| Market corridor | 203,644 of 203,644 cells; 0 missing, 0 extra, 0 exact-field differences, 0 out of tolerance |
| Prediction summary | 2 of 2 rows (historical, future); 0 out of tolerance |

## 8. Query performance

`scripts/benchmark_clickhouse.py` builds the same dataset in both systems: the corridor population at scale 1×
(291,020 rows) and 10× (2,910,200 rows: ten copies, with `price_id` offset so every copy is a distinct deal). Before
timing, the PostgreSQL and ClickHouse tables must have equal fingerprints. PostgreSQL gets an UNLOGGED table with
B-tree indexes on the route keys and on the price date, vacuumed and analyzed; ClickHouse gets a copy of
`fact_price_observation`, merged with `OPTIMIZE FINAL`. Each query runs once to warm caches, then 5 times; the
table shows the median wall-clock time, including the result transfer to the client. The results of every query
pair were equal (counts exact, floats within 1e-9 relative).

Environment: Intel Core i7-10700K (16 hardware threads), 31.3 GB RAM, local SSD, both servers on the same
machine. PostgreSQL 17.11 with `shared_buffers` 512MB, `work_mem` 64MB, `max_parallel_workers_per_gather` 2,
JIT on. ClickHouse 26.8.34.13 with `max_threads` 16.

| Query | Rows | PostgreSQL | PostgreSQL, 8 workers | ClickHouse | ClickHouse, 1 thread | Speedup |
|---|---|---|---|---|---|---|
| 1. Route pricing aggregation (trips, mean, median per route) | 291,020 | 0.304 s | 0.304 s | 0.016 s | 0.025 s | 19.5× |
| 2. Monthly market corridors (mart grain, all statistics) | 291,020 | 0.877 s | 0.860 s | 0.081 s | 0.138 s | 10.8× |
| 3. P25/P50/P75 by month and body group | 291,020 | 0.227 s | 0.227 s | 0.012 s | 0.023 s | 19.1× |
| 4. Two-year date range by month | 291,020 | 0.174 s | 0.173 s | 0.013 s | 0.020 s | 12.9× |
| 5. One route, monthly corridor | 291,020 | 0.015 s | 0.015 s | 0.014 s | 0.021 s | 1.1× |
| 1. Route pricing aggregation | 2,910,200 | 3.089 s | 3.111 s | 0.101 s | 0.177 s | 30.6× |
| 2. Monthly market corridors | 2,910,200 | 4.782 s | 4.792 s | 0.227 s | 0.505 s | 21.1× |
| 3. P25/P50/P75 by month and body group | 2,910,200 | 2.526 s | 2.575 s | 0.076 s | 0.134 s | 33.4× |
| 4. Two-year date range by month | 2,910,200 | 2.052 s | 2.047 s | 0.048 s | 0.091 s | 43.2× |
| 5. One route, monthly corridor | 2,910,200 | 0.152 s | 0.148 s | 0.016 s | 0.024 s | 9.7× |

Storage of the same rows: PostgreSQL table plus indexes 43.4 MB at 1× and 367 MB at 10×; ClickHouse compressed
8.1 MB and 25.1 MB. Full results: `docs/clickhouse_benchmark.json`.

Interpretation:

- PostgreSQL gains nothing from more parallel workers on these queries. `percentile_cont` and
  `count(DISTINCT ...)` have no partial aggregation, so the planner chooses a single-process sort and group
  (verified with `EXPLAIN`: a plain `avg` on the same table gets a parallel partial aggregate, `percentile_cont`
  does not). Every corridor statistic needs one of them.
- ClickHouse is faster even on one thread (6–23× at 10×). It reads only the referenced columns, compressed, and
  computes exact quantiles and distinct counts per group with vectorized hash tables instead of sorting all rows.
- The gap grows with data: PostgreSQL scales roughly linearly (×10 rows, ×5–12 time), ClickHouse sub-linearly
  (×1–6 time), because fixed per-query overhead dominates at 291k rows.
- Route-level filtering is the case PostgreSQL's B-tree handles well: equal at 1×. At 10× ClickHouse's sorting key
  reads a few granules of one route, while PostgreSQL fetches about 10× more index entries and heap rows.
- At the current data size, every query is well under one second in both systems. ClickHouse matters for larger
  history, finer drill-downs, and many concurrent dashboard users. The PostgreSQL mart stays the reference
  and the serving layer for Superset.

Reproduce: `python scripts/benchmark_clickhouse.py --scales 1,10 --runs 5 --output docs/clickhouse_benchmark.json`.
It needs a synced ClickHouse database and creates and drops `bench.*` in PostgreSQL and
`<db>.bench_price_observation_x*` in ClickHouse (`--keep` keeps them).

## 9. Operations

```bash
docker compose up -d --build                                   # PostgreSQL, pipeline
docker compose --profile clickhouse up -d clickhouse          # ClickHouse on 127.0.0.1:18123
docker compose --profile clickhouse run --rm clickhouse-sync  # sync + checks, exit 0 only when consistent
docker compose --profile clickhouse run --rm clickhouse-sync python -m etl.clickhouse.sync --months 2025-01..2025-03
docker compose --profile clickhouse run --rm clickhouse-sync python -m etl.clickhouse.check
```

Run the sync after each pipeline run; it is incremental, so frequent runs are cheap. Timings on the build machine:

| Run | Time |
|---|---|
| First sync into an empty database (81 partitions, derived tables, checks) | 37.6 s |
| `--full` without checks | 31.9 s |
| No source changes, with checks | 11.3 s (sync 3.6 s, checks about 8 s) |
| Backfill of one month, with checks | 12.7 s |
| `--plan` | 3.3 s |

Exit codes: 0 synced and consistent; 1 a load could not be verified, the checks found a difference, another
sync holds the lock, or a server is unreachable; 2 invalid arguments or missing settings. Logs are JSON lines;
`clickhouse_sync_finished` carries the plan, derived rebuilds, watermark, and the full check report.

History: `SELECT * FROM logistics.sync_log ORDER BY logged_at DESC`.

| Variable | Meaning |
|---|---|
| `CLICKHOUSE_HOST`, `CLICKHOUSE_PORT` | HTTP interface; `clickhouse:8123` in compose, `127.0.0.1` and `CLICKHOUSE_PUBLISH_PORT` from the host |
| `CLICKHOUSE_DB` | Target database, default `logistics` in `.env.example` |
| `CLICKHOUSE_USER`, `CLICKHOUSE_PASSWORD` | Login; the image creates it from these variables. No defaults in code |
| `CLICKHOUSE_SECURE` | `1` for HTTPS |
| `CLICKHOUSE_INSERT_BATCH_ROWS` | Rows per INSERT block, default 50,000 |
| `CLICKHOUSE_WATERMARK_OVERLAP_S` | Prediction overlap window, default 3,600 |

Without Docker: run any ClickHouse 26.8 server, set the variables, and run `python -m etl.clickhouse.sync`. The
integration tests create and drop a database `logistics_test_<random>`, and skip when ClickHouse is unreachable.

## 10. Limitations

- The ClickHouse image and the compose services were not started: Docker is unavailable on the build machine.
  Everything above ran against the official ClickHouse 26.8.34.13 binary (the same version as the image tag)
  with the same user and database settings. The compose file passes `docker compose config`, and the image tag
  exists on Docker Hub.
- One server, no replication. For ReplicatedMergeTree the stage-and-replace flow is unchanged, but the
  deduplication window setting becomes `replicated_deduplication_window`.
- The sync reads whole changed partitions. A change to one deal reloads its month (about 7,000 rows, under a
  second). For far larger months, partition by week or add a row-level change table in PostgreSQL.
- The checks compare ClickHouse with the stored PostgreSQL mart. If the mart is stale against the core, both
  describe the stale snapshot. Refresh the mart first; the pipeline does.
- Superset still reads PostgreSQL. Pointing charts at ClickHouse would need the `clickhouse-connect` SQLAlchemy
  dialect in the Superset image, and a decision on which system the BI users trust; that is outside this stage.
