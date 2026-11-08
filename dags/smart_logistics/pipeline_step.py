"""Run one platform step in the pipeline interpreter and translate the outcome into Airflow semantics.

The pipeline has its own virtualenv (PIPELINE_PYTHON, project code in PIPELINE_HOME): Airflow's dependency
constraints differ from the platform's pinned stack, so no platform module is imported into the Airflow process.
"""

from __future__ import annotations

import json
import logging
import os
import signal
import subprocess
import tempfile
import time
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

logger = logging.getLogger(__name__)

EXIT_OK, EXIT_FAILED, EXIT_USAGE, EXIT_TRANSIENT = 0, 1, 2, 75
DEFAULT_PIPELINE_PYTHON = "/opt/pipeline/venv/bin/python"
DEFAULT_PIPELINE_HOME = "/opt/pipeline/app"
TERMINATE_GRACE_S = 30
MODEL_TASK = "train_or_load_model"
# Not needed by the pipeline; Airflow's own secrets stay out of the child environment.
CHILD_ENV_DROP_PREFIXES = ("AIRFLOW__", "_AIRFLOW")
CHILD_ENV_DROP = ("POSTGRES_PASSWORD", "AIRFLOW_DB_PASSWORD")


class StepFailed(Exception):
    def __init__(self, step: str, exit_code: int, error: dict | None):
        self.step, self.exit_code, self.error = step, exit_code, error or {}
        detail = f"{self.error.get('type')}: {self.error.get('message')}" if self.error else "no error detail"
        super().__init__(f"{step} exited with {exit_code}: {detail}")

    @property
    def transient(self) -> bool:
        return self.exit_code == EXIT_TRANSIENT


def resolve_window(params: dict, interval_start: datetime | None, interval_end: datetime | None) -> tuple[date, date]:
    """Inclusive loading_date window: params start_date/end_date if given, else the run's data interval.

    A daily interval [D 00:00, D+1 00:00) UTC is the window D..D; a zero-length interval is its start day.
    """
    start, end = params.get("start_date"), params.get("end_date")
    if start or end:
        if not (start and end):
            raise ValueError("params start_date and end_date must be given together")
        first, last = date.fromisoformat(str(start)), date.fromisoformat(str(end))
    else:
        if interval_start is None or interval_end is None:
            raise ValueError("the run has no data interval: trigger it with a logical date, or set params "
                             "start_date and end_date")
        first = interval_start.astimezone(timezone.utc).date()
        last = max(first, (interval_end.astimezone(timezone.utc) - timedelta(microseconds=1)).date())
    if first > last:
        raise ValueError(f"start_date {first} is after end_date {last}")
    return first, last


def child_environment(base: dict[str, str] | None = None) -> dict[str, str]:
    env = dict(os.environ if base is None else base)
    for key in list(env):
        if key in CHILD_ENV_DROP or key.startswith(CHILD_ENV_DROP_PREFIXES):
            del env[key]
    return env


def _terminate(process: subprocess.Popen) -> None:
    """Stop the step and everything it started; its open transactions roll back."""
    if process.poll() is not None:
        return
    try:
        os.killpg(process.pid, signal.SIGTERM)
        process.wait(TERMINATE_GRACE_S)
    except subprocess.TimeoutExpired:
        os.killpg(process.pid, signal.SIGKILL)
        process.wait()
    except ProcessLookupError:
        pass


def run_step(step: str, *, run_key: str, window: tuple[date, date], options: tuple[str, ...] = (),
             payload: dict | None = None) -> dict:
    """Run `python -m orchestration.airflow_steps STEP ...`, stream its log, return its result or raise StepFailed."""
    python = os.environ.get("PIPELINE_PYTHON", DEFAULT_PIPELINE_PYTHON)
    home = os.environ.get("PIPELINE_HOME", DEFAULT_PIPELINE_HOME)
    with tempfile.TemporaryDirectory(prefix=f"{step}-") as tmp:
        result_file = Path(tmp) / "result.json"
        command = [python, "-m", "orchestration.airflow_steps", step, "--run-key", run_key,
                   "--start-date", window[0].isoformat(), "--end-date", window[1].isoformat(),
                   "--result-file", str(result_file), *options]
        if payload is not None:
            input_file = Path(tmp) / "input.json"
            input_file.write_text(json.dumps(payload, default=str), encoding="utf-8")
            command += ["--input-file", str(input_file)]
        logger.info("running %s", " ".join(command))
        process = subprocess.Popen(command, cwd=home, env=child_environment(), stdout=subprocess.PIPE,
                                   stderr=subprocess.STDOUT, text=True, bufsize=1, start_new_session=True)
        try:
            for line in process.stdout:
                logger.info("%s", line.rstrip())
            exit_code = process.wait()
        except BaseException:
            # Task timeout or kill: Airflow raises inside this process; the step must not outlive it.
            _terminate(process)
            raise
        outcome = json.loads(result_file.read_text(encoding="utf-8")) if result_file.is_file() else {}
    if exit_code != EXIT_OK:
        raise StepFailed(step, exit_code, outcome.get("error"))
    return outcome["result"]


def _run_key(context) -> str:
    return f"{context['dag'].dag_id}/{context['run_id']}"


def _window(context) -> tuple[date, date]:
    from airflow.sdk.exceptions import AirflowFailException

    try:
        return resolve_window(context["params"], context.get("data_interval_start"), context.get("data_interval_end"))
    except ValueError as error:
        raise AirflowFailException(str(error)) from error


def execute_step(step: str) -> dict:
    """Airflow callable for one pipeline step. Transient failures raise for retry; others fail the task at once."""
    from airflow.sdk import get_current_context
    from airflow.sdk.exceptions import AirflowException, AirflowFailException

    context = get_current_context()
    ti, params = context["ti"], context["params"]
    window = _window(context)
    options: list[str] = []
    if step == "refresh_marts" and params.get("force_mart"):
        options.append("--force-mart")
    if step == "train_or_load_model" and params.get("retrain"):
        options.append("--retrain")
    if step in ("run_batch_inference", "validate_prediction_records"):
        model = ti.xcom_pull(task_ids=MODEL_TASK)
        if not model:
            raise AirflowFailException(f"no model version from {MODEL_TASK}")
        options += ["--model-version", model["output"]["model_version"]]
    started = time.monotonic()
    try:
        output = run_step(step, run_key=_run_key(context), window=window, options=tuple(options))
    except StepFailed as failure:
        ti.xcom_push(key="failure", value={"attempt": ti.try_number, "exit_code": failure.exit_code,
                                           "transient": failure.transient, **failure.error})
        if failure.transient:
            raise AirflowException(str(failure)) from None
        raise AirflowFailException(str(failure)) from None
    return {"step": step, "window": [window[0].isoformat(), window[1].isoformat()], "attempt": ti.try_number,
            "duration_s": round(time.monotonic() - started, 3), "output": output}


def publish_statistics(pipeline_tasks: tuple[str, ...]) -> dict:
    """Record the run in audit.pipeline_run and the report directory, then fail if any pipeline task did not succeed."""
    from airflow.sdk import get_current_context
    from airflow.sdk.exceptions import AirflowFailException

    context = get_current_context()
    ti, dag_run = context["ti"], context["dag_run"]
    states = ti.get_task_states(dag_id=ti.dag_id, task_ids=list(pipeline_tasks), run_ids=[ti.run_id])
    states = states.get(ti.run_id, {})
    items = []
    for task_id in pipeline_tasks:
        returned = ti.xcom_pull(task_ids=task_id) or {}
        failure = ti.xcom_pull(task_ids=task_id, key="failure")
        items.append({"task_id": task_id, "state": states.get(task_id),
                      "attempts": returned.get("attempt") or (failure or {}).get("attempt") or 0,
                      "duration_s": returned.get("duration_s", 0.0), "output": returned.get("output"),
                      "error": failure})
    window = _window(context)
    payload = {
        "tasks": items,
        "started_at": dag_run.start_date.isoformat() if dag_run.start_date else None,
        "airflow": {"dag_id": ti.dag_id, "run_id": ti.run_id, "run_type": str(dag_run.run_type),
                    "logical_date": context.get("logical_date"),
                    "data_interval_start": context.get("data_interval_start"),
                    "data_interval_end": context.get("data_interval_end"), "params": dict(context["params"])},
    }
    try:
        summary = run_step("publish_pipeline_statistics", run_key=_run_key(context), window=window, payload=payload)
    except StepFailed as failure:
        if failure.transient:
            raise
        raise AirflowFailException(str(failure)) from None
    unsuccessful = {item["task_id"]: item["state"] for item in items if item["state"] not in ("success", "skipped")}
    if unsuccessful:
        raise AirflowFailException(f"pipeline run {summary['run_id']} recorded as {summary['status']}; "
                                   f"unsuccessful tasks: {unsuccessful}")
    return summary
