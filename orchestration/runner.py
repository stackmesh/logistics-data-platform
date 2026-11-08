"""Plan, execute, report."""

from __future__ import annotations

import logging
import sys
import time

from etl.config import DateWindow, EtlConfig, load_config
from etl.log import configure_logging, log_event
from orchestration.dag import FAILED, NOT_RUN, execute, plan
from orchestration.report import build_report, format_report, persist, utc_now
from orchestration.tasks import MODES, TASKS, RunContext

logger = logging.getLogger("pipeline")


def run(mode: str, window: DateWindow, *, force_mart: bool = False, config: EtlConfig | None = None,
        context: RunContext | None = None) -> tuple[int, dict]:
    configure_logging()
    config = config or load_config()
    ctx = context or RunContext.from_environment(mode, window, config, force_mart)
    ordered = plan(TASKS, MODES[mode])
    started_at = utc_now()
    started = time.perf_counter()
    log_event(logger, "pipeline_started", run_id=str(ctx.run_id), mode=mode, window=window.describe(),
              as_of_date=config.as_of_date, tasks=[task.name for task in ordered])
    try:
        results = execute(ordered, ctx, backoff_s=ctx.retry_backoff_s)
        tasks_s = round(time.perf_counter() - started, 3)
        log_event(logger, "task_started", task="report", step="12", description="build the execution report")
        report = build_report(ctx, results, started_at, utc_now(), tasks_s)
        duration_s = round(time.perf_counter() - started, 3)
        report["run"]["completed_at"] = utc_now()
        report["run"]["duration_s"] = duration_s
        report["durations_s"].update(tasks=tasks_s, report=round(duration_s - tasks_s, 3), total=duration_s)
        persist(ctx, report, results)
    finally:
        ctx.close()
    ok = not any(result.status in (FAILED, NOT_RUN) for result in results.values())
    log_event(logger, "pipeline_completed" if ok else "pipeline_failed", logging.INFO if ok else logging.ERROR,
              run_id=str(ctx.run_id), mode=mode, status=report["run"]["status"], duration_s=duration_s,
              failed_tasks=report["run"]["failed_tasks"], not_run_tasks=report["run"]["not_run_tasks"],
              counts=report.get("counts"), report=report["persisted"])
    print(format_report(report), file=sys.stderr, flush=True)
    return (0 if ok else 1), report
