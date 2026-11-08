"""Tasks with explicit dependencies, a deterministic execution order, and bounded retries."""

from __future__ import annotations

import logging
import time
import traceback
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Callable

import psycopg
from sqlalchemy import exc as sa_exc

from etl.log import log_event

logger = logging.getLogger("pipeline")

SUCCEEDED = "succeeded"
FAILED = "failed"
SKIPPED = "skipped"
NOT_RUN = "not_run"
SATISFIED = frozenset({SUCCEEDED, SKIPPED})
MAX_TRACEBACK_LINES = 12
PERMANENT_MARKERS = ("password authentication failed", "no pg_hba.conf entry", "role \"", "permission denied")


@dataclass(frozen=True)
class Task:
    name: str
    step: str
    description: str
    run: Callable[[Any], dict]
    depends_on: tuple[str, ...] = ()
    retries: int = 0
    skip_if: Callable[[Any], str | None] | None = None


@dataclass
class TaskResult:
    name: str
    step: str
    status: str = NOT_RUN
    attempts: int = 0
    started_at: datetime | None = None
    finished_at: datetime | None = None
    duration_s: float = 0.0
    output: dict = field(default_factory=dict)
    error: dict | None = None
    reason: str | None = None

    def to_dict(self) -> dict:
        return {
            "name": self.name, "step": self.step, "status": self.status, "attempts": self.attempts,
            "started_at": self.started_at, "finished_at": self.finished_at, "duration_s": self.duration_s,
            "reason": self.reason, "error": self.error, "output": self.output,
        }


class PlanError(ValueError):
    """The task graph is invalid: unknown name, duplicate, or cycle."""


def plan(tasks: tuple[Task, ...], selected: tuple[str, ...]) -> list[Task]:
    """Topological order of the selected tasks; ties keep declaration order.

    Dependencies outside the selection are dropped: their output is expected to exist in the warehouse.
    """
    by_name = {}
    for task in tasks:
        if task.name in by_name:
            raise PlanError(f"duplicate task {task.name}")
        by_name[task.name] = task
    for task in tasks:
        unknown = [dep for dep in task.depends_on if dep not in by_name]
        if unknown:
            raise PlanError(f"{task.name} depends on unknown tasks {unknown}")
    missing = [name for name in selected if name not in by_name]
    if missing:
        raise PlanError(f"unknown tasks {missing}")
    chosen = set(selected)
    position = {task.name: index for index, task in enumerate(tasks)}
    pending = {name: {dep for dep in by_name[name].depends_on if dep in chosen} for name in chosen}
    ordered: list[Task] = []
    while pending:
        ready = sorted((name for name, deps in pending.items() if not deps), key=position.__getitem__)
        if not ready:
            raise PlanError(f"dependency cycle among {sorted(pending)}")
        name = ready[0]
        ordered.append(by_name[name])
        del pending[name]
        for deps in pending.values():
            deps.discard(name)
    return ordered


def is_transient(error: BaseException) -> bool:
    """Connection loss, server restart, serialization failure, deadlock: safe to retry an idempotent task.

    Credential and privilege errors are operational errors too, but retrying cannot fix them.
    """
    if any(marker in str(error) for marker in PERMANENT_MARKERS):
        return False
    seen = set()
    stack = [error]
    while stack:
        current = stack.pop()
        if current is None or id(current) in seen:
            continue
        seen.add(id(current))
        if isinstance(current, psycopg.OperationalError):
            return True
        if isinstance(current, sa_exc.OperationalError):
            return True
        if isinstance(current, sa_exc.DBAPIError) and current.connection_invalidated:
            return True
        stack += [getattr(current, "orig", None), current.__cause__, current.__context__]
    return False


def describe_error(error: BaseException) -> dict:
    lines = traceback.format_exception(type(error), error, error.__traceback__)
    tail = "".join(lines).strip().splitlines()[-MAX_TRACEBACK_LINES:]
    return {"type": type(error).__name__, "message": " ".join(str(error).split())[:2000],
            "transient": is_transient(error), "traceback_tail": tail}


def _now() -> datetime:
    return datetime.now(timezone.utc)


def execute(ordered: list[Task], context, *, backoff_s: float = 5.0, sleep: Callable[[float], None] = time.sleep,
            ) -> dict[str, TaskResult]:
    """Run tasks in order. A task runs only when every selected dependency succeeded or was skipped."""
    results: dict[str, TaskResult] = {}
    for task in ordered:
        result = TaskResult(task.name, task.step)
        results[task.name] = context.results[task.name] = result
        blocked = [dep for dep in task.depends_on if dep in results and results[dep].status not in SATISFIED]
        if blocked:
            result.reason = f"upstream task {blocked[0]} {results[blocked[0]].status}"
            log_event(logger, "task_not_run", logging.WARNING, task=task.name, step=task.step, reason=result.reason)
            continue
        reason = task.skip_if(context) if task.skip_if else None
        if reason:
            result.status, result.reason = SKIPPED, reason
            log_event(logger, "task_skipped", task=task.name, step=task.step, reason=reason)
            continue
        result.started_at = _now()
        started = time.perf_counter()
        log_event(logger, "task_started", task=task.name, step=task.step, description=task.description)
        while True:
            result.attempts += 1
            try:
                result.output = task.run(context) or {}
                result.status = SUCCEEDED
                break
            except Exception as error:  # noqa: BLE001 - every failure is recorded and reported
                retry = is_transient(error) and result.attempts <= task.retries
                if not retry:
                    result.status, result.error = FAILED, describe_error(error)
                    break
                delay = backoff_s * 2 ** (result.attempts - 1)
                log_event(logger, "task_retry", logging.WARNING, task=task.name, attempt=result.attempts,
                          delay_s=delay, error=f"{type(error).__name__}: {error}")
                sleep(delay)
        result.finished_at = _now()
        result.duration_s = round(time.perf_counter() - started, 3)
        if result.status == SUCCEEDED:
            log_event(logger, "task_succeeded", task=task.name, step=task.step, attempts=result.attempts,
                      duration_s=result.duration_s, output=result.output)
        else:
            log_event(logger, "task_failed", logging.ERROR, task=task.name, step=task.step, attempts=result.attempts,
                      duration_s=result.duration_s, error=result.error)
    return results
