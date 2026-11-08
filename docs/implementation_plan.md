# Implementation plan

Status: waiting for approval. Do not create the pipeline, the warehouse DDL, or the Docker services until the assumptions in `docs/architecture.md` section 12.2 are accepted or corrected.

The design documents are the contract:

| Document | Contents |
| --- | --- |
| `docs/architecture.md` | Requirements, layers, flow, deployment, contradictions, assumptions |
| `docs/data_dictionary.md` | Grain, keys, foreign keys, indexes, column meaning |
| `docs/etl_design.md` | Stages, rules, replay, acceptance |
| `docs/mart_design.md` | Route card, metrics, BI use |
| `docs/ml_design.md` | Split, features, leakage, scoring |

## 1. Decisions to confirm before coding

| ID | Decision in the design | Change if you reject it |
| --- | --- | --- |
| A-WIN | Extract on `GREATEST(created_at, updated_at)::date` | Name another extract column. Mart and ML dates stay as specified |
| A-FX-1 | Exact stored rate; RUB rate 1; round half away from zero to 2 d.p.; RUB `price <= 1000` only | |
| A-FX-2 | Duplicate rates: greatest `id` | |
| A-CAT | Fill NULL categories only; PostgreSQL numeric rounding | |
| A-NULL | NULL measures use the existing reject codes | |
| A-CUR | Unknown currency is quarantined | |
| A-CITY | Region is the text after the first comma | |
| A-BODY-ML | One model row per deal, from `prices.body_type`; mart still explodes link groups | |
| A-MOM | MoM and YoY on `median_trip` | Switch the percentage base to `median_km` without changing the grain |
| A-REL | Freshness is the age of `MAX(price_date)` inside the cell | |
| A-PCT | `percentile_cont` | |
| A-MONTH | One observation per deal, month, and body group | |
| A-SCORE | Future score set excludes auctions and composite routes | |
| A-TYPE | Warehouse measures are `numeric` | |
| A-DB | Databases `deals_bigdata` and `logistics_dwh` on one PostgreSQL 17 instance | |

No other product rules are open. In particular, implementation should not add a same-city reject, should not recalculate `outlier_mark`, and should not read `price_group_stats` as a feature.

## 2. Target tree

```text
docker-compose.yml
Dockerfile
.env.example
README.md
docker/init/01_databases.sh
sql/dwh/001_schemas.sql
sql/dwh/010_raw.sql
sql/dwh/020_dq.sql
sql/dwh/030_staging.sql
sql/dwh/040_core.sql
sql/dwh/050_mart.sql
sql/dwh/060_ml.sql
sql/dwh/070_route_card.sql
etl/__init__.py
etl/cli.py
etl/extract.py
etl/publish.py
etl/logging.py
ml/__init__.py
ml/train_score.py
ml/metrics.py
tests/unit/
tests/sql/
tests/test_leakage.py
tests/test_replay.py
requirements/reference/          unchanged
```

`sql/dwh/070_route_card.sql` is the statement behind stage 8 and stage 9, kept as SQL rather than assembled in Python, so the corridor can be reviewed in one file.

The project README written during implementation must include: how to run `docker compose up`, how to replay a window, the architecture in short, why PostgreSQL and CatBoost were chosen, SQL a reviewer can run against `mart.route_card` and `ml.price_predictions`, the four test metrics from the actual run, and the snapshot limitation that deals with `created_at >= 2026-10-26 02:30:00` were not marked by the nightly outlier job.

## 3. Build sequence

### Phase 0 — Skeleton

- Pin Python 3.12, SQLAlchemy 2.x, psycopg 3, Polars, NumPy, CatBoost, scikit-learn, and Pytest.
- Add `.env.example` with empty passwords and `AS_OF_DATE=2026-10-26`, `RANDOM_SEED=20261026`, `N_PRICES=300000`.
- Compose file: `postgres:17` with a health check, and a one-shot `pipeline` service with `restart: "no"`.
- Init script creates both databases and both roles, then applies the reference `schema.sql` and `synthetic.sql` only to `deals_bigdata`.
- Pipeline image waits for health, applies `sql/dwh`, and exits 0 before any transform exists, as a smoke step that is then extended.

Acceptance: a clean volume comes up; the source contains 300000 `prices` rows; `logistics_dwh` has the six schemas; the pipeline role cannot insert into the source.

### Phase 1 — Raw landing

- Implement stage 0–1 from `docs/etl_design.md`.
- Full mode and window mode.
- Child replace for body types and price dates.
- Land `price_group_stats` and stop there for that table.

Acceptance: replay leaves raw counts and the price sum unchanged; a deal outside the window is untouched; child counts match the source.

### Phase 2 — Quality and conform

- Implement stages 2–7: quarantine codes, FX, category fill, `dim_city`, body bridge, core upsert.
- Fixtures for every acceptance bullet in those stages, using small inserted deals, not the 300000-row snapshot.

Acceptance: the fixture matrix in `docs/etl_design.md` passes, including same-city retention, exact FX date, RUB `price = 1000`, and Kazan dedup.

### Phase 3 — Mart

- Implement `mart.route_card`, the corridor view, month replace, MoM, YoY, and reliability.
- Bind `as_of_date`. Do not call `CURRENT_DATE` in mart SQL.

Acceptance: the edge-case table in `docs/mart_design.md` section 8 is covered by SQL tests. A second rebuild matches sums and labels.

### Phase 4 — Model

- Feature SQL with the allow-list and the three roles.
- CatBoost fit and batch score.
- NULL actuals for `score_future`.
- Metrics row and log line.

Acceptance: the criteria in `docs/ml_design.md` section 7 pass. Leakage test fails the build if a banned name appears.

### Phase 5 — Full rehearsal and README

- `docker compose up` from an empty volume runs init, full ETL, mart, train, and score.
- Record R², MAPE, RMSE, and MAE from that run in the README.
- Document the replay command.

Acceptance: pipeline container exit code 0; prediction table has test rows with errors and future rows with NULL errors; mart has no `price_month` after `2026-10-01` that is produced by a `price_date` after `2026-10-26` (October itself is allowed).

### Phase 6 — Only after the mandatory path is green

These are bonus or optional. They must not change mart numbers or prediction numbers of `catboost_regressor`:

- Ridge or decision-tree baseline under a separate `model_name`.
- Airflow DAG that calls the same CLI.
- Superset dataset on `mart.route_card`.
- ClickHouse copy of `mart.route_card`.
- Optional outlier recomputation, stored beside `outlier_mark`, never as a replacement for it.

## 4. Test plan

| Test | Phase | What it locks |
| --- | --- | --- |
| Category boundaries at 0.5, 2.5, 5, 10, 30 tonnes, including the 0.1 rounding halves | 2 | A-CAT |
| Volume 0, 120, and above 120 | 2 | A-CAT |
| Non-null source category kept when it disagrees with weight | 2 | A-CAT |
| Each quarantine code, and a row with two codes | 2 | Quality rules |
| Same-city positive distance is in core | 2 | Non-rule |
| FX uses `nearest_rate_date` and ignores a different `loading_date` rate | 2 | FR-3 |
| Foreign price 500 is kept; RUB price 1000 is quarantined | 2 | A-FX-1 |
| `dim_city` one row for `gc_id` 1001 and 1003; Kazan region text | 2 | FR-9, A-CITY |
| Body pairs `(56, 61)`, `(56, 15)`, fallback, and NULL | 2–3 | FR-10 |
| Future `price_date` absent from the card; `2026-10-26` present | 3 | FR-11 |
| Outlier in core and absent from the card | 3 | FR-5 |
| MoM NULL when the previous cell is deleted | 3 | Calendar rule |
| Reliability branches HIGH, MEDIUM, LOW_DATA | 3 | A-REL |
| Replay of a fixture window: unchanged counts and sums | 1–4 | NFR-5 |
| Feature allow-list and ban-list | 4 | Leakage |
| Split bounds around `2026-07-26`, `2026-10-05`, `2026-10-26`, `2026-10-27` | 4 | FR-12 |
| Future errors are NULL while source price is not | 4 | FR-13 |
| Sine and cosine wrap at year end and week end | 4 | Calendar encoding |
| SQL under `ml` and `mart` does not depend on `price_group_stats` | 4 | Ban |

Default Pytest excludes the full 300000-row snapshot. The compose rehearsal is that run.

## 5. Implementation risks

| Risk | Mitigation in the plan |
| --- | --- |
| Synthetic init of 300000 deals is the slow part of `docker compose up` | Init runs once per volume. The pipeline does not regenerate it. Document expected wait in the README after the first measured run |
| Float sums make replay tests flicker | Cast to `numeric` at landing (A-TYPE) and compare sums on `price_rub` |
| Train/serve drift on categories or calendars | Both are SQL, covered by unit tests |
| A month rebuilt from only the window's deals deletes everyone else | Stage 8 reads all eligible core rows for the touched months. A test loads two deals into one month, replays a window that contains one of them, and checks the other deal's contribution |
| CatBoost writes files or uses wall-clock early stopping | `allow_writing_files=false`, no eval set, fixed iterations |
| Reviewer looks for demo cargo filters and a weekly index | README states those prototype elements are outside the brief |

## 6. Prioritized checklist

The order is the implementation order after approval. Items above the line are mandatory. Items below the line are optional and come last.

1. Confirm or correct assumptions A-WIN through A-DB.
2. Add the compose file, the Dockerfile, `.env.example`, and the source init of both databases with 300000 deals.
3. Add warehouse DDL for `raw`, `dq`, `staging`, `core`, `mart`, and `ml`, matching `docs/data_dictionary.md`.
4. Land the source window idempotently, including full child replacement and a full load of rates, cities, body types, and `price_group_stats`.
5. Quarantine invalid rows with the specified reason codes, and keep same-city rows when distance is positive.
6. Convert foreign currency on the exact `nearest_rate_date`, and apply the RUB `price <= 1000` rule to the source price.
7. Fill only missing weight and volume categories, with numeric rounding to 0.1 t.
8. Publish deduplicated cities, collapsed body groups, and conformed facts, including future `price_dates`.
9. Build `mart.route_card` and the `market_corridor` view at the monthly grain, with the corridor, evidence, `cv_rate`, calendar MoM and YoY, and reliability.
10. Exclude outliers, composite routes, auctions, non-positive measures, and `price_date > 2026-10-26` from that card.
11. Build the feature matrix from the allow-list, with the fixed split and with `price_group_stats` unread.
12. Train `CatBoostRegressor` with seed `20261026`, report R², MAPE, RMSE, and MAE on test, and write predictions.
13. Store NULL `actual_price_rub`, `abs_error_rub`, and `ape_pct` for `loading_date > 2026-10-26`.
14. Add Pytest coverage for quality rules, body and city edges, mart filters, replay, the split, and the feature ban list.
15. Write the project README with the run steps, the replay command, the snapshot outlier limitation, example SQL, and the measured metrics.
16. Rehearse `docker compose up` on a clean volume and keep the pipeline exit code at 0.
17. Optional: Ridge or decision-tree baseline under a separate model name.
18. Optional: Airflow, Superset, or a ClickHouse copy of the same mart rows.
19. Optional: a side-by-side outlier recomputation that does not replace `outlier_mark`.

## 7. Stop line

Implementation starts after you approve this set or mark the assumptions to change. The next edit should be Phase 0, not a partial mart or a model notebook.
