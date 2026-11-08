# ML evaluation report

Stage 7 trains a CatBoost regressor that predicts the carrier trip price `price_rub` (RUB without VAT). It evaluates the model on a held-out chronological test window, compares it with three baselines, and scores the test deals and the planned future deals into `ml.ml_price_predictions`.

Every number below was measured on the real warehouse:
- 299,797 core deals at as_of 2026-10-26;
- model version `20261026-8f32ec6d6901`;
- model file SHA-256 `e089fe64fb66fddf8588b6dbf32d4997ad079b6ff05eb856a918bf77a68f49a8`.

The R&D note's figures (R² ≈ 0.93, MAPE ≈ 13%) came from another dataset. They are reference points, not targets, and nothing here promises them.

**Environment.** Docker is not installed on this host: there is no `docker` binary and no daemon socket. The workflow therefore ran on the local PostgreSQL 17.11 instance that has served every stage, with Python 3.11, CatBoost 1.2.10 and scikit-learn 1.7.2. The compose `pipeline` service runs the same commands: `ml.model.train`, then `ml.scoring.score`. The container uses Python 3.12 and has not been built on this host.

## Workflow

```bash
python -m etl.migrate          # applies sql/ml/001_price_predictions.sql
python -m ml.model.train       # Stage 6 features -> CatBoost + baselines -> ml/artifacts/<version>/ -> ml.ml_model_registry
python -m ml.scoring.score     # test + future deals -> ml.ml_price_predictions, audit.ml_scoring_batch
psql -f sql/ml/fact_vs_forecast.sql
```

| Step | Measured time |
|---|---|
| Feature build plus training: early-stopping probe, final fit, 3 baselines | 5.0 s |
| First scoring: 17,699 rows inserted | 1.98 s |
| Repeat scoring: 17,699 unchanged | 0.66 s |

## Data and split

| Set | `loading_date` | Rows | Use |
|---|---|---|---|
| Fit | 2026-07-26 .. 2026-09-20 | 18,703 | Early-stopping probe only |
| Validation | 2026-09-21 .. 2026-10-04 | 5,523 | Chooses the tree count. The last 14 days of train, taken chronologically |
| Train | 2026-07-26 .. 2026-10-04 | 24,226 | Final model fit |
| Test | 2026-10-05 .. 2026-10-26 | 12,808 | Metrics and historical predictions |
| Future | 2026-10-27 .. 2027-02-13 | 4,891 | Predictions without actuals |

- Features are the 24 Stage 6 columns, in Stage 6 order. Train and test both use the Stage 6 filters (`is_fact`, not complex, not outlier, `price_rub > 0`, `distance > 0`).
- Rows are in `(loading_date, price_id)` order. The order is never randomized and no fold is random.
- CatBoost runs with `has_time=True`, so its ordered statistics follow that chronological order instead of a random permutation.
- `2026-10-05` is in test only.

**Future population:** `loading_date > 2026-10-26`, not complex, `price_type <> 6` (no auction bids), `distance > 0`.
- Membership doesn't depend on any price or price-derived flag; neither `is_fact` nor `outlier_mark` is used.
- Every planned deal in the snapshot carries a source price, and most are flagged `is_fact = true`. That price is never read: the future query selects only the 14 base feature columns.
- Five future source deals with a non-positive price were quarantined by the ETL, never reached core, and are not scored.

## Model

| Setting | Value |
|---|---|
| Estimator | `catboost.CatBoostRegressor` |
| Loss / eval metric | RMSE / RMSE |
| `random_seed` | 42 |
| `depth`, `learning_rate`, `l2_leaf_reg` | 6, 0.05, 3.0 |
| Iteration cap / early stopping | 3,000 / 200 rounds without improvement on the validation window |
| Best iteration | 262, so the final model has 263 trees fitted on the whole train window |
| Categorical features (native CatBoost) | `origin_city`, `destination_city`, `origin_region`, `destination_region`, `body_group`, `weight_category`, `volume_category` |
| `has_time`, `thread_count`, `verbose` | true, 4, 100 |
| Target | `price_rub` in RUB, not logged. The research note says logging does not help trees |

Early stopping uses validation rows that lie strictly before the test window. The test window never influences training or the tree count.

The probe model on the validation window scored R² 0.9133, MAPE 15.27%, RMSE 56,973.50 and MAE 27,433.43.

### Feature importance

These are CatBoost `PredictionValuesChange` values, in percent:

| Feature | Importance | Feature | Importance |
|---|---|---|---|
| `distance` | 66.79 | `origin_region` | 0.36 |
| `weight_category` | 17.44 | `origin_city` | 0.36 |
| `total_weight` | 7.46 | `destination_region` | 0.25 |
| `body_group` | 6.06 | `destination_latitude` | 0.19 |
| The other 15 features together (calendar, volume, `destination_city`, the remaining coordinates) | 1.10 | `year` | 0.00 |

Distance dominates, as the research note found (39–40% there; 67% here). Weight and body group come next. Geography adds little once distance is known: the synthetic generator prices mostly by distance, tonnage and body type. Calendar features contribute almost nothing, and `year` is constant.

## Test metrics

All models were fitted on the same 24,226 train rows and evaluated on the same 12,808 test rows. MAPE uses rows with an actual above zero; no test row is zero or negative, so all 12,808 count.

| Model | R² | MAPE % | RMSE, RUB | MAE, RUB | Median APE % | Bias, RUB |
|---|---|---|---|---|---|---|
| **CatBoost** | **0.9203** | **15.72** | **54,018.83** | **28,190.88** | **12.04** | +2,512.51 |
| DecisionTree (depth 12, min leaf 20) | 0.9067 | 15.87 | 58,432.65 | 30,287.09 | 12.58 | −3,285.93 |
| Ridge (one-hot + standard scaling) | 0.8405 | 49.43 | 76,408.01 | 48,308.56 | 23.78 | +6,903.23 |
| Median RUB/km per body group × distance | 0.7486 | 41.90 | 95,926.78 | 57,968.45 | 23.37 | +8,870.55 |

Bias is the mean of predicted minus actual.

- CatBoost is best on every metric.
- The tree is close behind: CatBoost cuts RMSE by 7.6% and MAE by 6.9% against it, but MAPE by only 0.15 points.
- Ridge is far worse in relative terms, as the research note predicts. A linear model on raw RUB misprices short trips badly; its MAPE is 49%.

The database recomputes the CatBoost metrics from the stored, rounded rows in `ml.v_prediction_error_summary`: R² 0.920268, MAE 28,190.88, RMSE 54,018.83, MAPE 15.7230%. These equal the in-memory metrics, and an integration test asserts it.

### Error distribution (test)

| APE band | Deals | % of deals | Mean signed % error |
|---|---|---|---|
| 0–5% | 2,777 | 21.68 | −0.03 |
| 5–10% | 2,683 | 20.95 | −0.33 |
| 10–20% | 3,830 | 29.90 | +0.47 |
| 20–30% | 2,066 | 16.13 | +5.28 |
| 30–50% | 1,174 | 9.17 | +23.76 |
| 50%+ | 278 | 2.17 | +77.41 |

- 42.6% of test deals are within 10% of the actual and 72.5% within 20%. The 90th-percentile APE is 31.3%.
- The bias is small in roubles (+1.3% of the mean actual).
- The large percentage errors are mostly over-predictions of unusually cheap deals: the signed error rises with the band. Percentage error is unbounded upward and bounded at −100% downward, which makes MAPE asymmetric.

Calibration is good. The mean prediction per prediction decile is within −1.4% to +2.7% of the mean actual in that decile.

### Error by segment (test)

| Distance | Deals | Mean actual | MAE | MAPE % | Mean signed % |
|---|---|---|---|---|---|
| < 300 km | 437 | 26,691 | 6,464 | 26.46 | +15.57 |
| 300–999 km | 3,658 | 68,819 | 10,238 | 15.49 | +3.80 |
| 1,000–2,999 km | 5,755 | 160,382 | 23,188 | 15.18 | +4.04 |
| 3,000+ km | 2,958 | 443,480 | 63,335 | 15.48 | +5.81 |

| Body group | Deals | MAPE % | Mean signed % |
|---|---|---|---|
| Не указан | 252 | 16.46 | +7.79 |
| Тент | 6,743 | 16.27 | +6.82 |
| Борт | 1,079 | 15.30 | +4.51 |
| Изотерм | 865 | 15.30 | −0.89 |
| Рефрижератор | 2,568 | 15.13 | +2.71 |
| Прочие | 1,301 | 14.53 | +1.68 |

| Loading week | Deals | MAE | MAPE % |
|---|---|---|---|
| 2026-10-05 | 3,154 | 27,514 | 15.81 |
| 2026-10-12 | 3,626 | 28,357 | 15.54 |
| 2026-10-19 | 4,782 | 28,596 | 16.03 |
| 2026-10-26 (one day) | 1,246 | 27,867 | 14.88 |

- Error is flat across the three test weeks, so there is no visible decay inside the horizon.
- Short hauls are the weakest segment: about 26% MAPE and over-prediction.
- One example route: Moscow → Kazan, Тент, had 53 test deals with mean actual 78,083, mean predicted 80,367 and MAPE 12.24%.

### Largest errors

The ten largest absolute errors are mostly deals priced 3–4 times their route's market level. For example, Воронеж → Томск, Прочие, 3,719 km, cost 1,979,857 RUB against a prediction of 425,296. Such deals look like extremes that the source never marked as outliers.

- 54 test deals (0.4%) are off by more than a factor of 2.5 in either direction: 24 under-predicted and 30 over-predicted.
- They carry 25.1% of the squared error.
- Without them, RMSE would be 46,856 and MAPE 14.92%. This is a diagnostic only; the reported metrics include every test deal.

## Batch scoring

Each run does the following:

1. It loads `ml/artifacts/latest.json`, or `--model-version`, and checks that the model file's SHA-256 matches the card and that the file's feature names equal the Stage 6 allow-list in order.
2. It rebuilds the test and future features with the frozen train vocabulary from the card.
3. It predicts in one vectorized CatBoost call per group.
4. Inside one transaction under the core ETL advisory lock, it:
   - COPYs `(price_id, split_role, predicted_price_rub)` into a temporary table;
   - calls `ml.upsert_price_predictions`;
   - deletes predictions of this model version for deals that left the population;
   - writes the statistics to `audit.ml_scoring_batch`.

   A failure rolls everything back and marks the batch `failed`.

`actual_price_rub` is filled in SQL from `core.fact_deal.price_rub` for test rows only, through `CASE WHEN split_role = 'test'`. Python never sends an actual. A check constraint makes a future row with an actual, an error, or a loading date on or before as_of impossible to store.

The upsert key is the primary key `(price_id, model_name, model_version)`. A row is rewritten only when a stored value differs, so a repeat run leaves `scored_at` and `scoring_batch_id` untouched.

| Run | Test | Future | Inserted | Updated | Unchanged | Deleted |
|---|---|---|---|---|---|---|
| First run for the version | 12,808 | 4,891 | 17,699 | 0 | 0 | 0 |
| Any repeat (latest: batch 7) | 12,808 | 4,891 | 0 | 0 | 17,699 | 0 |

Predictions range from 7,048.57 to 1,601,668.99 RUB, and none is zero or negative. Future rows by loading month:

| Month | 2026-10 (27–31) | 2026-11 | 2026-12 | 2027-01 | 2027-02 |
|---|---|---|---|---|---|
| Deals | 188 | 1,365 | 1,392 | 1,378 | 568 |

## Fact versus forecast

`sql/ml/001_price_predictions.sql` creates three views:

| View | Contents |
|---|---|
| `ml.v_fact_vs_forecast` | One row per prediction: route names, body group, distance, actual, predicted, signed and absolute error, signed % error, APE, APE band, record type (historical or future), model version, batch |
| `ml.v_prediction_error_summary` | Per model version and record type: deals, date range, mean actual and predicted, MAE, RMSE, MAPE, R², bias, median and p90 APE, share within 10% and 20% |
| `ml.v_prediction_error_distribution` | Historical deals per APE band, with share and mean signed % error |

`sql/ml/fact_vs_forecast.sql` holds ten queries; the tables above come from them:
- the summary and the error distribution;
- the largest errors;
- error by body group, by distance band and by week;
- calibration deciles;
- future predictions by month and body group;
- Moscow → Kazan historical vs future;
- the scoring batches.

## Artifacts

`ml/artifacts/<model_version>/` (ignored by git; in compose it is the `ml_models` volume) holds:

| File | Contents |
|---|---|
| `model.cbm` | CatBoost model. The random `model_guid` is replaced by the model version and the wall-clock `train_finish_time` is removed, so retraining on the same data gives a byte-identical file |
| `model_card.json` | Model name and version, library versions, seed, as_of, train, validation and test periods, training config, full CatBoost params, best and final iterations, feature list and order, categorical and numeric lists, feature definitions, frozen categorical vocabulary, row counts, data digests, core batch, validation/train/test metrics, baseline metrics, feature importance, model SHA-256 |
| `baselines.pkl` | The fitted Ridge, DecisionTree and rate-per-km baselines |
| `../latest.json` | Pointer to the version the scorer uses by default. Written only after the registry row commits |

The model version is `as_of` plus the first 12 hex digits of a SHA-256 over:
- the training config;
- the feature spec and feature list;
- the split;
- the train X and y digests;
- the CatBoost version.

Same data and config therefore give the same version. Changed data or config gives a new version, which gets its own prediction rows. `ml.ml_model_registry` holds one row per version with periods, seed, features, config, metrics, baseline metrics and the model SHA-256.

## Tests

| Requirement | Tests |
|---|---|
| No leakage columns | `make_pool` rejects any non-allow-list column, including `price_rub`. The model's features exclude every banned column. The future SQL selects no price, `is_fact`, `price_type` or `outlier_mark`. Python stages only `price_id`, `split_role` and the prediction. No `ml` view or function reads `raw`, `price_group_stats` or `price_src` |
| Time boundaries | Validation is 2026-09-21..2026-10-04, and fit ends 2026-09-20. Card and registry periods are checked. Stored test rows fall in 2026-10-05..2026-10-26, future rows after 2026-10-26, and no training-period row is stored. Counts equal independent SQL counts |
| No random shuffle | `has_time=True`. The validation mask rejects unsorted rows. A source scan finds no `train_test_split`, `shuffle`, `sample(`, `KFold` or clock call |
| Metric calculations | Hand-computed values. Agreement with scikit-learn's R², MAE, RMSE and MAPE. Non-positive targets are skipped by MAPE and counted. Bad input raises. Database metrics equal training metrics |
| Future actuals NULL | All 4,891 future rows have NULL actual, absolute error and APE, although every one has a source price in core. A check constraint rejects an update that sets an actual |
| Prediction uniqueness | The primary key is `(price_id, model_name, model_version)`, a duplicate insert fails, and there are no duplicates |
| Repeatable scoring | Second and third runs: 0 inserted, 0 updated, 0 deleted, same content hash, every batch `succeeded`. An injected failure after the upsert rolls back to the previous rows and records `failed` |
| Artifact reproducibility | Retraining gives the same version, the same model SHA-256 and the same metrics, on the real data and on a synthetic set. A tampered model file is rejected |
| Feature ordering | `model.feature_names_` equals the card and the allow-list. Reordered columns are rejected by `predict` and `make_pool`. Load checks file order against the card |

`tests/test_ml_model.py` has 19 tests and needs no database. `tests/test_ml_scoring.py` has 13 tests against the real warehouse, including the brief-named prediction mart view `mart.ml_price_predictions`.

## Limitations

1. **Short horizon and no seasonality.** Train covers 26 July to 4 October 2026. The future rows run from October 2026 to February 2027, months the model has never seen: November–December peak demand and the early-year slowdown from the research note. Trees cannot extrapolate calendar features, so future predictions carry the price level of late summer and early autumn. `year` is constant and has zero importance. Future predictions are planning estimates, not validated forecasts; their error is unknown until those deals happen.
2. **No trend extrapolation.** Test prices sit about 5% above train, and the model's mean bias on test is small (+1.3%). A longer horizon would drift as prices rise, so the model needs regular retraining on a rolling window.
3. **Unmarked extremes in the target.** Train keeps deals up to 5.74 M RUB, and test has deals 3–4 times their route's market level. RMSE is driven by these deals: 0.4% of test rows give 25% of the squared error. No outlier rule beyond the source's `outlier_mark` is applied, because the brief does not define one. A train-only fence would change the target population and must be agreed first.
4. **Short hauls.** Trips under 300 km have about 26% MAPE and are over-predicted by about 16% on average. Fixed costs dominate there, and the synthetic data has few such deals: 437 in test.
5. **Single-point estimate.** No prediction interval is produced. The validation and test errors (median APE 12%, p90 31%) indicate the uncertainty for typical deals.
6. **Synthetic data.** All results come from one synthetic snapshot (seed 0.20261026). Metrics on real market data will differ. The R&D reference values are not guaranteed.
7. **Planned deals rejected by the ETL.** Five future source deals with a non-positive price were quarantined and are not scored. Accepting price-less planned deals would need a separate core rule.
8. **Docker not exercised.** The compose path is configured but was not run on this host.
