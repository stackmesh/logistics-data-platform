# Data quality and reliability report: Stage 3 ETL

Audit date: 2026-11-08. Dataset: the supplied synthetic snapshot, 300,000 deals, seed `0.20261026`, as of 2026-10-26.

## Verdict

The ETL passed every check in this audit:

| Area | Result |
| --- | --- |
| Reconciliation | Every source deal has exactly one explained outcome. No record is missing, duplicated, or wrongly classified. |
| Idempotency | A repeated run leaves the dataset unchanged, down to row lineage. |
| Backfill | Overlapping windows do not multiply rows. Reloading an emptied range reproduces the full load exactly. |
| Transaction safety | Failed, killed, and concurrent loads leave no partial data. |

At the time of this audit the ETL suite had 103 tests and all of them passed. The whole platform suite, including the later mart, ML, and pipeline stages, now has 309 tests; see the README for the latest run.

**Environment caveat.** Results come from PostgreSQL 17.11 running locally, with the same two databases, roles, and migrations that Docker Compose creates. Docker is not installed on the audit machine, so `docker compose --profile test run --rm tests` has not been executed here. The suite uses only environment variables, so the container runs the same tests.

## 1. Validation rules

A deal is quarantined when any rule fails. Each failed rule is stored as its own row in `audit.quarantine_price`, together with:

- a readable detail,
- the full source row as JSONB,
- the batch ID,
- `rejected_at`.

| Code | Rule | Hits in snapshot |
| --- | --- | --- |
| `PRICE_NON_POSITIVE` | `price` NULL or `<= 0` | 164 |
| `RUB_PRICE_LE_1000` | currency 643 and `0 < price <= 1000` | 0 |
| `DISTANCE_NON_POSITIVE` | `distance` NULL or `<= 0` | 0 |
| `WEIGHT_NEGATIVE` | `total_weight` NULL or `< 0` | 0 |
| `VOLUME_NEGATIVE` | `total_volume` NULL or `< 0` | 0 |
| `ORIGIN_CITY_NOT_FOUND` | `loading_city_gcid` not in `cities` | 0 |
| `DESTINATION_CITY_NOT_FOUND` | `unloading_city_gcid` not in `cities` | 0 |
| `CURRENCY_UNSUPPORTED` | currency not 643, 840, or 978 | 0 |
| `FX_RATE_MISSING` | USD or EUR, and `nearest_rate_date` is NULL or has no rate on that exact date | 39 (all NULL dates) |

Normalization of accepted deals:

- **FX**
  - RUB uses rate 1.
  - USD and EUR use the rate whose `setting_date` equals `nearest_rate_date`. When the source has several rows for one currency and date, the greatest `id` wins.
  - `price_rub = ROUND(ROUND(price, 2) * ROUND(rate, 6), 2)`, rounding halves away from zero.
- **Cities**: one row per `gc_id`, taken from the smallest `cities.id`.
- **Categories**: valid source values are kept. Missing or invalid values are calculated with the business bands (weight rounded to 0.1 t; volume `FTL` when `0 < v <= 120`) and flagged `*_imputed`.
- **Kept as-is**: same-city routes, non-fact deals, future deals, and `outlier_mark`.

## 2. Source-to-core reconciliation

### Method

`etl/quality/reconcile.py` recomputes the expected outcome of every source deal in SQL on `deals_bigdata`. It shares no code with the Polars transform. It then full-joins the result with `raw.prices`, `core.fact_deal` (joined to `core.dim_city`), and `audit.quarantine_price`.

A deal counts as explained only when every condition holds:

- It is in raw.
- It is in exactly one of core and quarantine.
- Its quarantine reason codes equal the expected set.
- When accepted, these fields match the source: FX rate, FX rate date, `nearest_rate_date`, `price_rub`, distance, both city keys, both city resolutions (the smallest source `cities.id`), both categories, `outlier_mark`, and `is_fact`.

Amounts are compared as exact `numeric` text, so a one-kopeck difference is a failure.

To run it:

```bash
python -m etl.quality.reconcile --full --by-month
```

### Results

| Window (loading_date) | Source | Raw | Core | Quarantined | Unexplained | Duplicate IDs | Field mismatches |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| Full history | 300,000 | 300,000 | 299,797 | 203 | 0 | 0 | 0 |
| 2026-07-26..2026-10-26 | 46,678 | 46,678 | 46,634 | 44 | 0 | 0 | 0 |
| 2026-10-26 (as-of day) | tested | | | | 0 | 0 | 0 |
| 2026-10-27..2027-12-31 (future) | tested | | | | 0 | 0 | 0 |
| 2023 | tested | | | | 0 | 0 | 0 |

- **Total `SUM(price_rub)`**:
  - full history 51,312,682,850.42;
  - 2026-07-26..2026-10-26 window 8,997,878,564.12.
- **Mismatch counters, full history**: all 20 counters are 0. They cover `fx_rate`, `fx_rate_date`, `nearest_rate_date`, `price_rub`, `distance`, the origin and destination keys and resolutions, both categories, `outlier_mark`, `is_fact`, in core and quarantine at once, missing, not in source, not in raw, should be accepted, should be quarantined, and reason codes.
- **Months**: all 41 months from 2023-10 through 2027-02 balance as source = raw = accepted + quarantined, with 0 unexplained. The largest month is 2026-10: 18,519 = 18,497 + 22.
- **Batch audit**: `audit.etl_batch` and `audit.dq_statistic` agree with the independent counts (300,000 extracted, 299,797 accepted, 203 rejected, and the same per-reason counts).
- **Duplicate keys**: none in core, raw, deal dates, the bridge, quarantine, rates, or cities. No deal is in both core and quarantine.
- **Negative control**: inside a rolled-back transaction I added 0.01 to one `price_rub`, changed one weight category, and deleted one fact row. The oracle flagged all three as unexplained (`test_reconciliation_detects_tampering`).

## 3. Idempotency

The 2026-07-26..2026-10-26 window was run twice as normal committed batches 210 and 211. The fingerprint (`etl/quality/fingerprint.py`) was taken after each run.

| Measure | Run 1 | Run 2 |
| --- | --- | --- |
| Extracted / accepted / rejected | 46,678 / 46,634 / 44 | 46,678 / 46,634 / 44 |
| Fact inserted / updated / deleted | 0 / 0 / 0 | 0 / 0 / 0 |
| Raw and quarantine changes | 0 | 0 |
| `core.fact_deal` rows | 46,634 | 46,634 |
| `SUM(price_rub)` | 8,997,878,564.12 | 8,997,878,564.12 |
| `SUM(distance_km)` | 97,062,083.000 | 97,062,083.000 |
| Fact logical hash | `8fade3f9413705c0b974a4bc1347d7aa` | `8fade3f9413705c0b974a4bc1347d7aa` |
| Fact lineage hash (batch_id, extracted_at) | `d7ae7c4c9a3fa66a4b7d4acc7a73cdde` | `d7ae7c4c9a3fa66a4b7d4acc7a73cdde` |
| Deal-date rows / hash | 52,235 / `f88bea03…` | 52,235 / `f88bea03…` |
| Bridge rows / hash | 52,086 / `66c5d142…` | 52,086 / `66c5d142…` |
| Quarantine rows / hash | 44 / `c83d60b9…` | 44 / `c83d60b9…` |
| Raw rows / hash | 46,678 / `f53bff6e…` | 46,678 / `f53bff6e…` |

Each hash is an `md5` over the ordered per-row `md5`s of every business column.

The lineage hash does not change either, which means unchanged rows are not rewritten. The full-history fingerprint stays the same across window reruns (`test_same_window_twice_leaves_dataset_identical`).

## 4. Backfill

**Committed overlapping runs.** Window A (2026-08-01..2026-09-30, batch 212) and window B (2026-09-01..2026-10-31, batch 213) were run against the loaded warehouse:

- A: 26,067 extracted, 0 changes.
- B: 32,286 extracted, 0 changes.
- The 2026-09 overlap has 13,756 fact rows before and after, with an identical fingerprint.
- The full-history fingerprint is identical.

**Backfill into an empty range** (`test_backfill_into_empty_range_with_overlapping_windows`, rolled back):

1. Delete every core, quarantine, and raw row with a loading date in 2026-08-01..2026-10-31.
2. Load window A, then window B, from real extracts.

Results:

- The logical fingerprint of the whole range equals the full load's.
- The overlap's fingerprint equals the full load's.
- B inserts only the October deals and reports 0 updates for the 13,756 overlap deals.

## 5. Transaction safety

Every load step for a batch runs in one warehouse transaction:

- the raw, dimension, fact, and quarantine merges;
- the `dq_statistic` write;
- the `succeeded` status update.

A transaction-level advisory lock serializes loaders. To make a partial commit visible, the failure tests double every price before loading, then fail.

| Test | Simulation | Result |
| --- | --- | --- |
| `test_failure_after_all_writes_rolls_back_everything` | Exception after all merges, before commit | Full fingerprint unchanged. Batch `failed` with the error and `completed_at`. No rows carry that batch ID. |
| `test_failure_between_raw_and_core_rolls_back_raw` | Exception after the raw and dimension merges | Same |
| `test_killed_session_mid_load_leaves_core_untouched` | Load completed but not committed, then the server session is killed with `pg_terminate_backend` | Fingerprint unchanged. The batch row created in that transaction is gone too. |
| `test_concurrent_run_waits_for_the_advisory_lock` | Another session holds the ETL lock | Second loader waits on the `advisory` lock, finishes after release, and changes nothing |
| `test_reprocessing_after_failed_load_leaves_no_partial_rows` (Stage 3) | Check-constraint violation mid-load, then retry | No partial rows. The retry succeeds. |

## 6. Edge cases

`tests/test_dq_edge_cases.py` sends each case through the transform and the real warehouse merge, and asserts the resulting database rows.

| Case | Outcome |
| --- | --- |
| USD with NULL `nearest_rate_date` | Quarantined, `FX_RATE_MISSING` |
| RUB with NULL `nearest_rate_date` | Accepted, `fx_rate` 1, `fx_rate_date` NULL |
| EUR with no rate on the exact date (rates exist on other dates) | Quarantined, `FX_RATE_MISSING` |
| RUB price exactly 1000 | Quarantined, `RUB_PRICE_LE_1000` |
| RUB price 1000.01 | Accepted, `price_rub` 1000.01 |
| Distance 0 | Quarantined, `DISTANCE_NON_POSITIVE` |
| Weight -0.5 | Quarantined, `WEIGHT_NEGATIVE` |
| Weight 0, no source category | Accepted, `OTH`, imputed |
| Volume 0, no source category | Accepted, `OTH` |
| Volume exactly 120 | Accepted, `FTL` |
| Volume 120.5 | Accepted, `OTH` |
| Missing origin / destination city | Quarantined, `ORIGIN_CITY_NOT_FOUND` / `DESTINATION_CITY_NOT_FOUND` |
| Duplicate `gc_id` in cities | One `dim_city` row from the smallest ID, `duplicate_count` 2. A fact-to-city join returns 1 row. |
| Body types 56, 57, 61 on one deal | One bridge row, `TENT`. The three raw links are kept. |
| `outlier_mark = true` | Accepted, flag preserved |
| Future planned deal (`is_fact` false, `price_type` 6) | Accepted |

The unit tests in `tests/test_etl_transform.py` add:

- every weight band boundary (0.44/0.45, 2.44/2.45, 4.94/4.95, 9.94/9.95, 29.94/29.95);
- volume 0.001 and 120.001;
- duplicate rate rows;
- half-up conversion rounding;
- same-city routes;
- multi-reason rows.

## 7. Performance

All measurements are on the full snapshot (300,000 deals; 964,658 source rows including links, dates, and reference tables), PostgreSQL 17.11 on the same host. To benchmark:

```bash
python scripts/benchmark_etl.py
```

| Run | Extract | Transform | Load | Total | Throughput |
| --- | ---: | ---: | ---: | ---: | ---: |
| Cold full load into empty targets | 2.9 s | 0.2 s | 49.9 s | 53.0 s | 5,660 deals/s |
| Warm full rerun (no changes) | 2.9 s | 0.2 s | 12.5 s | 15.7 s | 19,136 deals/s |
| One month, 2026-10 (18,519 deals) | 0.8 s | 0.03 s | 2.0 s | 2.8 s | |
| One day, 2026-10-26 (1,538 deals) | 0.5 s | 0.02 s | 1.2 s | 1.7 s | |

Cold load breakdown:

| Step | Time |
| --- | ---: |
| COPY into staging | 3.8 s |
| Raw merge | 18.4 s |
| Core merge | 27.5 s |
| Dimensions | under 0.1 s |
| Quarantine | under 0.1 s |

`auto_explain` shows that every merge is one sequential scan of a stage feeding one insert. About 60% of merge time is per-row foreign-key triggers: the fact table alone fires six FK checks on each of its 299,797 rows. The rest is index maintenance.

The foreign keys are the Stage 2 integrity contract, so they stay. This is a one-time cost of the initial load. Daily windows load in under 2 seconds.

Changes made during this audit:

- **Extraction streams through a server-side cursor.** SQLAlchemy's `stream_results` path was buffering whole results. Peak RSS after extraction fell from 1,193 MB to 379 MB, and the whole cold run peaks at 675 MB (previously 1,204 MB).
- **COPY sends chunks of Polars-rendered CSV** instead of writing one Python tuple per row. Staging COPY went from 6.3 s to 3.8 s. Float values round-trip exactly: reruns afterwards reported 0 raw updates.
- **The load logs per-step timings** in each batch summary and in `audit.etl_batch.summary`.

**Indexes.** None were added. Most merge predicates already have an index; two deletes scan their table, and that is acceptable at this volume:

| Merge predicate | Access path |
| --- | --- |
| Staging keys | Primary key on each staging table |
| Window deletes on fact | Sequential scan of `core.fact_deal` (about 46 ms for 299,797 rows). The predicate is `loading_date in window OR deal in this extract`, and the `OR EXISTS` branch prevents use of `fact_deal_loading_date_idx`. Splitting it into two deletes would make it index-driven if the table grows by orders of magnitude |
| Window deletes on raw | `raw_prices_loading_date_idx` |
| Window deletes on quarantine | Sequential scan of `audit.quarantine_price` (203 rows), same `OR` shape as the fact delete |
| Child deletes on raw links | `raw_price_body_types_price_idx` |
| Child deletes on raw dates | `(price_id, price_date)` unique |
| Child deletes on core dates and bridge | Primary keys |
| Upserts | Each target's primary key |

The one plan regression found in Stage 3, a nested loop caused by stale statistics after bulk inserts, is prevented by `ANALYZE` on the targets at the start of each load.

**Memory.** The pipeline uses Polars, not Pandas. The source and transformed frames are about 59 MB each. Transformation adds about 186 MB of peak RSS, and load staging adds about 110 MB.

## 8. Test automation

| Module | Scope | Tests |
| --- | --- | ---: |
| `test_etl_transform.py` | Rules, no database | 47 |
| `test_etl_load.py` | Merge idempotency and window replacement, fixtures, rolled back | 4 |
| `test_etl_extract.py` | Window extraction from the real source | 1 |
| `test_dq_reconciliation.py` | Source-to-core oracle over 5 windows, months, audit tables, duplicates, negative control | 9 |
| `test_dq_idempotency_backfill.py` | Repeated and overlapping committed runs, empty-range backfill | 3 |
| `test_dq_transaction_safety.py` | Injected failures, killed session, advisory lock | 4 |
| `test_dq_edge_cases.py` | All requested edge cases through the real merge | 22 |
| `test_core_schema.py`, `test_db_smoke.py` | Stage 1 and 2 contracts | 13 |

Last full run of these modules during the audit: 103 passed in 2 min 42 s. The table covers the ETL modules only; the mart, ML, and pipeline modules are listed in the README.

To run:

```bash
# inside Docker: applies migrations, then runs the whole suite
docker compose --profile test run --rm tests

# from the host
pytest -q                        # everything
pytest -q -m "not integration"   # rule tests only, no database
```

The integration tests start with one committed full load, which is idempotent. The idempotency, backfill, and lock tests then run real committed batches. Every other test that writes to the warehouse rolls back.

## 9. Known limitations

- **Docker not run here.** Docker Compose was not executed on the audit machine. Everything ran against an equivalent local PostgreSQL 17.11.
- **Outlier marks on the as-of day.** The source marks outliers in a nightly job, and the generator only marks deals created before `2026-10-26 02:30:00`. Of the 5,925 deals created on 2026-10-26, only the 140 created before 02:30 could be marked (3 are). The other 5,785 keep `outlier_mark = false` even when extreme. The ETL preserves the flag and does not recalculate it.
- **Several rules are proven only by fixtures.** The snapshot never triggers the RUB ≤ 1000, distance, weight, volume, city, or currency rules, or a missing exact rate with a non-NULL date. Source categories are always present, so category imputation never happens on real data. All of these paths are covered by fixture tests through the real merge.
- **Shared rule interpretation.** The reconciliation oracle is independent code, but it follows the same reading of the rules. For example, `RUB_PRICE_LE_1000` excludes prices `<= 0`, which get `PRICE_NON_POSITIVE` instead. A misread rule would be wrong in both places.
- **Killed processes leave a `running` row.** If the pipeline process itself dies, its batch row stays `running`. No data is affected, because the data transaction rolls back. There is no automatic sweep for stale running batches.
- **Cold-load cost is foreign-key enforcement.** Cold-load time is dominated by FK checks and is linear in rows. Larger first loads should be run window by window.
- **Price precision.** `price_src` is stored with 2 decimals. Source prices with more decimals would be rounded; the original float stays in `raw.prices`.
- **Tests write to the warehouse they target.** The integration suite commits idempotent batches and leaves `audit.etl_batch` rows, and rolled-back tests leave gaps in batch IDs. Point it at a disposable or non-production warehouse.
