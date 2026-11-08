"""The platform's tasks, their dependencies, and the task subset of each execution mode."""

from __future__ import annotations

import contextlib
import io
import logging
import os
import time
import uuid
from dataclasses import dataclass, field
from pathlib import Path

from sqlalchemy import exc as sa_exc
from sqlalchemy import text
from sqlalchemy.engine import Engine

from etl.config import DateWindow, EtlConfig, source_engine, warehouse_engine
from etl.log import log_event
from logistics.settings import SettingsError
from orchestration.dag import Task

logger = logging.getLogger("pipeline")

DEFAULT_REPORT_DIR = Path("artifacts") / "reports"
DEFAULT_FEATURE_DIR = Path("artifacts") / "features"


class DatabaseNotReady(RuntimeError):
    """The database did not become reachable and initialized within the wait timeout."""


class ReadinessError(RuntimeError):
    """A readiness check failed."""


class DataQualityError(RuntimeError):
    """Source-to-core reconciliation found unexplained or duplicated deals."""


class IntegrityCheckError(RuntimeError):
    """A post-run integrity check failed."""


def _env_float(name: str, default: float) -> float:
    value = os.environ.get(name, "").strip()
    if not value:
        return default
    try:
        parsed = float(value)
    except ValueError as exc:
        raise SettingsError(f"{name} must be a number") from exc
    if parsed < 0:
        raise SettingsError(f"{name} must not be negative")
    return parsed


def _env_path(name: str, default: Path) -> Path:
    return Path(os.environ.get(name, "").strip() or default)


@dataclass
class RunContext:
    mode: str
    window: DateWindow
    config: EtlConfig
    artifact_root: Path
    feature_dir: Path
    report_dir: Path
    wait_timeout_s: float
    wait_interval_s: float
    retry_backoff_s: float
    force_mart: bool = False
    run_id: uuid.UUID = field(default_factory=uuid.uuid4)
    results: dict = field(default_factory=dict)
    state: dict = field(default_factory=dict)
    _warehouse: Engine | None = None
    _source: Engine | None = None

    @classmethod
    def from_environment(cls, mode: str, window: DateWindow, config: EtlConfig, force_mart: bool = False):
        from ml.features.spec import DEFAULT_SPLIT
        from ml.model.config import artifact_root

        if config.as_of_date != DEFAULT_SPLIT.as_of:
            raise SettingsError(
                f"AS_OF_DATE {config.as_of_date} differs from the ML split as_of {DEFAULT_SPLIT.as_of}; "
                "the mart and the model would describe different snapshots"
            )
        return cls(
            mode=mode, window=window, config=config, artifact_root=artifact_root(),
            feature_dir=_env_path("ML_FEATURE_DIR", DEFAULT_FEATURE_DIR),
            report_dir=_env_path("PIPELINE_REPORT_DIR", DEFAULT_REPORT_DIR),
            wait_timeout_s=_env_float("PIPELINE_DB_WAIT_TIMEOUT_S", 600.0),
            wait_interval_s=_env_float("PIPELINE_DB_WAIT_INTERVAL_S", 5.0),
            retry_backoff_s=_env_float("PIPELINE_RETRY_BACKOFF_S", 5.0),
            force_mart=force_mart,
        )

    def warehouse(self) -> Engine:
        if self._warehouse is None:
            self._warehouse = warehouse_engine(self.config)
        return self._warehouse

    def source(self) -> Engine:
        if self._source is None:
            self._source = source_engine(self.config)
        return self._source

    def model_version(self) -> str | None:
        """The model trained in this run, else the one latest.json points to."""
        if "model_version" in self.state:
            return self.state["model_version"]
        import json

        pointer = self.artifact_root / "latest.json"
        if pointer.is_file():
            return json.loads(pointer.read_text())["model_version"]
        return None

    def close(self) -> None:
        for engine in (self._warehouse, self._source):
            if engine is not None:
                engine.dispose()
        self._warehouse = self._source = None


def _short(error: BaseException) -> str:
    return " ".join(f"{type(error).__name__}: {getattr(error, 'orig', None) or error}".split())[:300]


def wait_for(ctx: RunContext, target: str, probe) -> dict:
    """Poll probe() until it returns (True, detail). Database errors count as not ready, except bad credentials."""
    started = time.monotonic()
    attempt = 0
    while True:
        attempt += 1
        try:
            ready, detail = probe()
            if ready:
                return {"attempts": attempt, "waited_s": round(time.monotonic() - started, 3), **detail}
            reason = detail["reason"]
        except sa_exc.DBAPIError as error:
            if "password authentication failed" in str(error):
                raise
            reason = _short(error)
        if time.monotonic() - started >= ctx.wait_timeout_s:
            raise DatabaseNotReady(f"{target} not ready after {ctx.wait_timeout_s:g}s ({attempt} attempts): {reason}")
        log_event(logger, "database_waiting", target=target, attempt=attempt, reason=reason,
                  retry_in_s=ctx.wait_interval_s)
        time.sleep(ctx.wait_interval_s)


def source_ready(ctx: RunContext) -> dict:
    """Steps 1-2: the source server answers and the generator has written every requested deal."""
    expected = ctx.config.database.n_prices

    def probe():
        with ctx.source().connect() as conn:
            version = conn.execute(text("SHOW server_version")).scalar_one()
            if conn.execute(text("SELECT to_regclass('public.prices')")).scalar_one() is None:
                return False, {"reason": "public.prices does not exist yet"}
            prices = int(conn.execute(text("SELECT count(*) FROM public.prices")).scalar_one())
        if prices != expected:
            return False, {"reason": f"public.prices has {prices} rows, expected {expected}"}
        return True, {"server_version": version, "source_prices": prices}

    return wait_for(ctx, "source", probe)


def warehouse_ready(ctx: RunContext) -> dict:
    """Step 3: the warehouse bootstrap exists, then every readiness check passes."""
    from logistics.readiness import run_checks

    def probe():
        with ctx.warehouse().connect() as conn:
            if conn.execute(text("SELECT to_regclass('audit.bootstrap')")).scalar_one() is None:
                return False, {"reason": "audit.bootstrap does not exist yet"}
            count = conn.execute(text("SELECT source_price_count FROM audit.bootstrap WHERE id = 1")).scalar_one_or_none()
        if count is None:
            return False, {"reason": "audit.bootstrap has no row yet"}
        return True, {"bootstrap_price_count": int(count)}

    waited = wait_for(ctx, "warehouse", probe)
    with contextlib.redirect_stdout(io.StringIO()) as captured:
        report = run_checks(ctx.config.database)
    lines = captured.getvalue().splitlines()
    failed = [line.removeprefix("[smoke] FAIL ") for line in lines if line.startswith("[smoke] FAIL")]
    if failed:
        raise ReadinessError("; ".join(failed))
    return {**waited, "checks_passed": sum(line.startswith("[smoke] PASS") for line in lines), "ok": report.ok}


def migrate(ctx: RunContext) -> dict:
    from etl.migrate import apply_migrations

    return {"files": apply_migrations()}


def etl(ctx: RunContext) -> dict:
    from etl.pipeline.runner import run_pipeline

    summary = run_pipeline(ctx.window, ctx.config)
    ctx.state["etl_summary"] = summary
    keys = ("batch_id", "mode", "start_date", "end_date", "extracted_count", "accepted_count", "rejected_count",
            "reason_rows", "reason_counts", "timings")
    return {**{key: summary[key] for key in keys}, "fact_deal": summary["load"]["core"]["fact_deal"],
            "quarantine": summary["load"]["quarantine"], "accepted_profile": summary["accepted_profile"]}


def data_quality(ctx: RunContext) -> dict:
    from etl.quality.reconcile import reconcile_window

    report = reconcile_window(ctx.window)
    mismatches = {name: count for name, count in report["mismatches"].items() if count}
    output = {key: report[key] for key in ("window", "source_count", "raw_count", "fact_count", "quarantined_count",
                                          "accepted_explained", "quarantined_explained", "unexplained",
                                          "duplicate_price_ids", "reason_counts")}
    output["mismatches"] = mismatches
    if report["unexplained"] or report["duplicate_price_ids"]:
        raise DataQualityError(f"{report['unexplained']} unexplained deals, {report['duplicate_price_ids']} duplicate "
                               f"price_ids; mismatches {mismatches}; examples {report['mismatch_examples']}")
    return output


def mart(ctx: RunContext) -> dict:
    from etl.mart.refresh import refresh_if_changed, refresh_market_corridor

    refresh = refresh_market_corridor if ctx.force_mart else refresh_if_changed
    return refresh(ctx.warehouse(), ctx.config.as_of_date)


def features(ctx: RunContext) -> dict:
    from ml.features.dataset import build_dataset, write_dataset

    with ctx.warehouse().connect() as conn, conn.begin():
        conn.execute(text("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY"))
        dataset = build_dataset(conn)
    meta = write_dataset(dataset, ctx.feature_dir)
    ctx.state["dataset"] = dataset
    return {"output": str(ctx.feature_dir), "rows": meta["rows"], "features": len(meta["features"]),
            "core_batch_id": meta["population"]["core_batch_id"], "split": meta["split"]}


def train(ctx: RunContext) -> dict:
    from ml.features.dataset import build_dataset
    from ml.model.artifacts import publish_latest, save_artifacts
    from ml.model.train import register_model, train_model

    dataset = ctx.state.get("dataset")
    if dataset is None:
        with ctx.warehouse().connect() as conn, conn.begin():
            conn.execute(text("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY"))
            dataset = build_dataset(conn)
    result = train_model(dataset)
    card = save_artifacts(result, ctx.artifact_root)
    with ctx.warehouse().begin() as conn:
        register_model(conn, card)
    publish_latest(ctx.artifact_root, card)
    ctx.state["model_version"] = card["model_version"]
    return {"model_name": card["model_name"], "model_version": card["model_version"],
            "model_sha256": card["model_sha256"], "best_iteration": card["best_iteration"],
            "final_iterations": card["final_iterations"], "rows": card["rows"],
            "metrics": card["metrics"], "baseline_metrics": card["baseline_metrics"]}


def _score(ctx: RunContext, role: str) -> dict:
    from ml.scoring.score import score

    return score(ctx.warehouse(), ctx.artifact_root, ctx.model_version(), (role,))


def score_test(ctx: RunContext) -> dict:
    return _score(ctx, "test")


def score_future(ctx: RunContext) -> dict:
    return _score(ctx, "future")


def skip_scoring_without_model(ctx: RunContext) -> str | None:
    """A backfill rescores with the existing model; before the first training there is nothing to rescore."""
    if ctx.mode == "backfill" and ctx.model_version() is None:
        return f"no trained model under {ctx.artifact_root}"
    return None


def verify(ctx: RunContext) -> dict:
    from orchestration.verify import run_integrity_checks

    checks = run_integrity_checks(ctx)
    failed = [check for check in checks if not check["ok"]]
    if failed:
        raise IntegrityCheckError("; ".join(f"{check['name']}: {check['detail']}" for check in failed))
    return {"checks": len(checks), "passed": len(checks), "results": checks}


TASKS: tuple[Task, ...] = (
    Task("source_ready", "1-2", "wait for the source database and the generated deals", source_ready),
    Task("warehouse_ready", "3", "wait for the warehouse bootstrap and run readiness checks", warehouse_ready,
         ("source_ready",)),
    Task("migrate", "4", "apply sql/core, sql/mart, sql/ml migrations", migrate, ("warehouse_ready",), retries=2),
    Task("etl", "5", "extract, validate, and load deals into raw/core/audit", etl, ("source_ready", "migrate"),
         retries=2),
    Task("data_quality", "6", "reconcile the source with raw, core, and quarantine", data_quality, ("etl",),
         retries=2),
    Task("mart", "7", "refresh mart.mart_market_corridor when its inputs changed", mart,
         ("migrate", "data_quality"), retries=2),
    Task("features", "8", "build the ML feature dataset", features, ("migrate", "data_quality"), retries=2),
    Task("train", "9", "train and evaluate CatBoost and baselines, save artifacts", train, ("features",),
         retries=1),
    Task("score_test", "10", "score the historical test window", score_test, ("migrate", "data_quality", "train"),
         retries=2, skip_if=skip_scoring_without_model),
    Task("score_future", "11", "score planned future deals", score_future, ("migrate", "data_quality", "train"),
         retries=2, skip_if=skip_scoring_without_model),
    Task("verify", "12", "check keys, foreign keys, populated tables, and future actuals", verify,
         ("migrate", "data_quality", "mart", "train", "score_test", "score_future"), retries=2),
)

MODES: dict[str, tuple[str, ...]] = {
    "full": tuple(task.name for task in TASKS),
    "etl": ("source_ready", "warehouse_ready", "migrate", "etl", "data_quality", "verify"),
    "mart": ("warehouse_ready", "migrate", "mart", "verify"),
    "train": ("warehouse_ready", "migrate", "features", "train", "verify"),
    "score": ("warehouse_ready", "migrate", "score_test", "score_future", "verify"),
    "backfill": ("source_ready", "warehouse_ready", "migrate", "etl", "data_quality", "mart", "score_test",
                 "score_future", "verify"),
}
