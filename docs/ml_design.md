# ML design

The model predicts the carrier trip price in RUB without VAT, `y = price_rub`, for direct factual deals. It also scores planned future deals, whose actuals must stay empty.

This document is the design contract. The measured results are in two reports:

- [feature_engineering_report.md](feature_engineering_report.md): features, distributions, and the leakage audit;
- [ml_evaluation_report.md](ml_evaluation_report.md): metrics, baselines, error analysis, and scoring runs.

The research note's metric levels (R² near 0.93, MAPE near 13% for CatBoost) come from another dataset. They are orientation, not acceptance thresholds, and this pipeline does not promise them.

| Module | Role |
|---|---|
| `ml/features/spec.py` | Feature definitions, allow-list, ban list, fixed split windows, seed |
| `ml/features/extract.py` | Population SQL for train/test and for future deals, over `core.fact_deal` ⋈ `core.dim_city` |
| `ml/features/transform.py` | Row-wise feature values and `FeaturePipeline` (categorical vocabulary fitted on train only) |
| `ml/features/dataset.py` | Split, fit, transform, leakage guard, Parquet + metadata (`python -m ml.features.dataset`) |
| `ml/model/train.py` | CatBoost and baselines, artifacts, registry row (`python -m ml.model.train`) |
| `ml/model/metrics.py`, `ml/model/baselines.py`, `ml/model/artifacts.py` | Metrics, Ridge / DecisionTree / rate-per-km baselines, model card and deterministic model file |
| `ml/scoring/score.py` | Batch inference into `ml.ml_price_predictions` (`python -m ml.scoring.score`) |
| `sql/ml/001_price_predictions.sql` | Registry, scoring audit, predictions table, upsert function, fact-versus-forecast views |
| `sql/ml/002_prediction_mart.sql` | `mart.ml_price_predictions`, the brief-named prediction mart view |

## 1. Population and temporal split

`as_of = 2026-10-26` is the snapshot date. It is a constant in `SplitWindows`, never the run's system date.

| Role | Predicate on core | Rows | Use |
|---|---|---|---|
| train | `is_fact AND NOT complex AND NOT outlier_mark AND price_rub > 0 AND distance_km > 0`, `loading_date ∈ [2026-07-26, 2026-10-05)` | 24,226 | Fit. The last 14 days, `[2026-09-21, 2026-10-05)`, are also the early-stopping validation window |
| test | Same filters, `loading_date ∈ [2026-10-05, 2026-10-27)`, that is 2026-10-05 to 2026-10-26 inclusive | 12,808 | Metrics and historical predictions |
| future | `loading_date > 2026-10-26 AND NOT complex AND price_type <> 6 AND distance_km > 0` | 4,891 | Predictions with NULL actuals |

```mermaid
flowchart LR
  before["before 2026-07-26<br/>mart only"] --> fit["fit<br/>07-26 .. 09-20"] --> val["validation<br/>09-21 .. 10-04"] --> test["test<br/>10-05 .. 10-26"] --> future["future<br/>after 10-26"]
```

Split rules:

- `2026-10-05` belongs to test only. `2026-10-26` is test, not future.
- The split is a filter on `loading_date`. There is no `train_test_split`, no `shuffle`, no `sample`, and no K-fold.
- Rows are ordered by `(loading_date, price_id)`. CatBoost runs with `has_time=True`, so even its internal ordered statistics follow time.
- A test scans `ml/`, `orchestration/tasks.py`, and `orchestration/verify.py` for shuffle and clock calls.

Future membership:

- It does not depend on any price or price-derived flag; neither `is_fact` nor `outlier_mark` is used.
- Every planned deal in the snapshot carries a source price, and the future query does not select it.
- Five planned source deals have a non-positive price. The ETL quarantined them, so they never reached core and are not scored (see Known limitations in the README).

History before 2026-07-26 stays in the mart and is not used for training, as the brief's fixed window requires.

## 2. Features

There are 24 features: 7 categorical and 17 numeric. Every one is a function of its own row plus the frozen train vocabulary. There are no cross-row aggregates.

| Group | Features |
|---|---|
| Geography | `origin_city`, `destination_city`, `origin_region`, `destination_region` (from `core.dim_city.full_name_local`, split at the first comma); `origin_latitude`, `origin_longitude`, `destination_latitude`, `destination_longitude` |
| Load and equipment | `distance`, `total_weight`, `total_volume`, `body_group`, `weight_category`, `volume_category` |
| Calendar, from `loading_date` | `year`, `month`, `day_of_week` (ISO, Monday = 1), `day_of_year`; `month_sin/cos` (T = 12), `weekday_sin/cos` (T = 7), `day_of_year_sin/cos` (T = 365.25) |

- Origin and destination are encoded separately. There is no concatenated route string, because high-cardinality routes are sparse.
- Cities come from the deduplicated `core.dim_city`, which keeps the smallest `cities.id` per `gc_id`.
- `body_group` is the group of `prices.body_type`. A deal with several linked body types is trained once; it is not duplicated per group.
- Categorical vocabularies for city and region are learned from train only:
  - unseen values become `__UNKNOWN__`;
  - nulls become `__MISSING__`.
- Body group and the weight and volume categories use the fixed core domains.
- Numeric values are as stored. No imputer or scaler is used for CatBoost. The Ridge baseline fits its scaler and one-hot encoder on train only.

## 3. Leakage controls

| Banned input | Why | How it is kept out |
|---|---|---|
| `price`, `price_src`, `price_rub` | The target or its unconverted form | Target selected once and written only to `y`. Not in the allow-list |
| `price_with_vat`, customer cost, margin, `carrier_cost` | Accounting identities with the target. Not in the source schema | Banned by name and by fragments `price`, `cost`, `profit`, `vat`, `margin` |
| `price / distance`, `price / total_weight` | Unit prices of the deal being predicted | Never computed. `price_per_km`, `price_km`, and similar names are banned |
| `price_group_stats.*`, `price_group_id` | Nightly aggregate that includes the predicted deal | Not extracted into the warehouse at all. The feature SQL reads only `core` |
| `outlier_mark` | Used to clean the target. Must not be a feature | WHERE clause only |
| `fx_rate`, `currency_code`, `nearest_rate_date` | With the source price they rebuild the target | Not selected |
| Mart medians, MoM, YoY | The mart cell includes the deal's own month | Not read |
| Encoders or statistics fitted on test or future | Temporal leakage | Vocabulary fitted on train. Early stopping uses the last 14 days of train, never test |

Enforcement:

- `assert_no_leakage` runs on every matrix before anything is written. `make_pool` rejects any column outside the allow-list.
- Tests prove that copying the target into 13 banned columns, or scrambling it, does not change the features.
- The scorer stages only `(price_id, split_role, predicted_price_rub)`. `actual_price_rub` is filled in SQL from `core.fact_deal.price_rub`, and only when `split_role = 'test'`.
- A check constraint on `ml.ml_price_predictions` makes a future row with an actual, an error, or a loading date on or before `as_of` impossible to store.

## 4. Training

| Setting | Value |
|---|---|
| Estimator | `catboost.CatBoostRegressor`, native categorical features |
| Loss / eval metric | RMSE / RMSE |
| `random_seed` | 42. `RANDOM_SEED = 20261026` seeds Python and NumPy for the feature stage |
| `depth`, `learning_rate`, `l2_leaf_reg` | 6, 0.05, 3.0 |
| Iterations | Up to 3,000, with early stopping after 200 rounds without improvement on the validation window. The final model is refitted on all of train with `best_iteration + 1` trees (263) |
| `has_time`, `thread_count`, `allow_writing_files` | true, 4, false |
| Target | `price_rub` in RUB, not logged. The research note found that the log does not help trees |

Baselines use the same split, features, and rows:

- `Ridge` with one-hot encoding and standard scaling, fitted on train;
- `DecisionTreeRegressor` with depth 12 and minimum leaf size 20;
- the median RUB/km per body group, multiplied by distance.

Artifacts go to `ml/artifacts/<model_version>/` (the `ml_models` volume in compose):

- `model.cbm`, made deterministic: the random GUID is replaced by the version and the training timestamp is removed;
- `model_card.json`;
- `baselines.pkl`;
- `../latest.json`, the pointer the scorer uses by default. It is written only after the registry row commits, so the scorer can never pick a version that the foreign key on `ml.ml_price_predictions` would reject.

`model_version` is `as_of` plus 12 hex digits of a SHA-256 over the config, the feature spec, the split, the train data digests, and the CatBoost version. Retraining on the same data gives the same version and a byte-identical model file. `ml.ml_model_registry` gets one row per version.

## 5. Metrics

Metrics are computed on test rows only, with `y` the actual `price_rub` and `ŷ` the prediction:

| Metric | Definition |
|---|---|
| R² | `1 − SS_res / SS_tot` around the test mean |
| MAPE, % | mean of `abs(y − ŷ) / y × 100` over rows with `y > 0` (all test rows qualify) |
| RMSE, RUB | `sqrt(mean((y − ŷ)²))` |
| MAE, RUB | `mean(abs(y − ŷ))` |

Metrics are logged and stored in two places:

- `model_card.json` and `ml.ml_model_registry.metrics`;
- `audit.ml_scoring_batch.test_metrics` for each scoring batch.

`ml.v_prediction_error_summary` recomputes them in SQL from the stored rounded predictions. An integration test asserts that both computations agree.

## 6. Batch scoring

1. The scorer loads `latest.json`, or `--model-version`. It verifies the model file's SHA-256 against the card and the feature order against the allow-list.
2. It rebuilds test and future features with the frozen train vocabulary and predicts in one vectorized call per role.
3. In one transaction under the core ETL advisory lock, it:
   - COPYs `(price_id, split_role, predicted)` into a temp table;
   - calls `ml.upsert_price_predictions(model, version, as_of, batch, roles)`;
   - writes `audit.ml_scoring_batch` with counts, test metrics, and a future summary.
4. On failure everything rolls back, and the batch row is marked `failed`.

Upsert behaviour:

- The key is `(price_id, model_name, model_version)`.
- A row is rewritten only when a stored value differs, so a repeat run reports 17,699 unchanged and leaves `scored_at` alone.
- Rows of the scored roles that left the population are deleted. Other roles are untouched, which lets the pipeline score test and future as two tasks.

| Column | Test | Future |
|---|---|---|
| `actual_price_rub` | `core.fact_deal.price_rub` | NULL, whatever the source price says |
| `predicted_price_rub` | Model output, `numeric(18, 2)` | Model output, `numeric(18, 2)` |
| `abs_error_rub`, `ape_pct` | Computed in SQL | NULL |

Train deals are not written to the predictions table. Fitted values on training rows would look like accuracy and are not the fact-versus-forecast comparison the brief asks for.

## 7. Fact versus forecast

| View | Contents |
|---|---|
| `ml.v_fact_vs_forecast` | One row per prediction: route and body names, distance, actual, predicted, signed and absolute error, APE, APE band, `record_type` (`historical` or `future`) |
| `ml.v_prediction_error_summary` | Per model version and record type: R², MAE, RMSE, MAPE, bias, median and p90 APE, share within 10% and 20% |
| `ml.v_prediction_error_distribution` | Historical deals per APE band |
| `mart.ml_price_predictions` | The prediction mart under the brief's name and column list (`price_id`, `loading_date`, `loading_city_gcid`, `unloading_city_gcid`, `body_group`, `actual_price_rub`, `predicted_price_rub`, `abs_error_rub`, `ape_pct`, `model_name`, `scored_at`), plus `model_version` and `split_role` (`sql/ml/002_prediction_mart.sql`) |

`sql/ml/fact_vs_forecast.sql` holds ten example queries over these views. Queries 3 to 9 are restricted to the most recently registered model version.

## 8. Reproducibility

| Source of variation | Control |
|---|---|
| Snapshot | `setseed(0.20261026)` and `n_prices = 300000` inside the unchanged `synthetic.sql` |
| Split | Literal dates in `SplitWindows`. No clock |
| Row order | `ORDER BY loading_date, price_id`. CatBoost `has_time=True` |
| Model | Fixed seed, deterministic early stopping on a fixed window, deterministic model file |
| Packages | `requirements.txt` plus `constraints.txt` with exact versions of every package |

## 9. Out of scope

- Recomputed outlier rules, log-price fences, and prediction intervals.
- Hyperparameter search, target encoding, and rolling market features (lagged corridor values would need a strictly past-only definition).
- Using the mart as a feature.
- Treating the research note's R² ≈ 0.93 or MAPE ≈ 13% as a pass gate.
