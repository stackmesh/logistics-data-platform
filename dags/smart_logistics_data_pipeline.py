"""smart_logistics_data_pipeline: the Smart Logistics batch pipeline for one data interval.

Every task runs one step of orchestration.airflow_steps in the pipeline's own interpreter; the business logic
stays in the platform's etl, ml, and orchestration packages. See docs/airflow.md.
"""

from __future__ import annotations

import os
from datetime import timedelta

import pendulum
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG, CronDataIntervalTimetable, Param, TriggerRule

from smart_logistics.pipeline_step import execute_step, publish_statistics

DAG_ID = "smart_logistics_data_pipeline"


def _env_int(name: str, default: int) -> int:
    value = os.environ.get(name, "").strip()
    return int(value) if value else default


# task_id -> (execution timeout, retries; None means the DAG default)
STEPS: dict[str, tuple[timedelta, int | None]] = {
    "check_source_connectivity": (timedelta(minutes=2), 5),
    "validate_source_data": (timedelta(minutes=15), None),
    "prepare_warehouse": (timedelta(minutes=15), None),
    "run_etl": (timedelta(minutes=45), None),
    "run_data_quality": (timedelta(minutes=20), None),
    "refresh_marts": (timedelta(minutes=30), None),
    "generate_features": (timedelta(minutes=20), None),
    "train_or_load_model": (timedelta(minutes=60), 1),
    "run_batch_inference": (timedelta(minutes=30), None),
    "validate_prediction_records": (timedelta(minutes=20), None),
}

default_args = {
    "owner": "data-platform",
    "depends_on_past": False,
    "retries": _env_int("PIPELINE_AIRFLOW_RETRIES", 3),
    "retry_delay": timedelta(seconds=_env_int("PIPELINE_AIRFLOW_RETRY_DELAY_S", 60)),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=15),
}

with DAG(
    dag_id=DAG_ID,
    description="Source checks, ETL, data quality, mart, ML features, model, batch inference, run statistics",
    # A run at D+1 00:00 UTC covers [D, D+1): loading_date D. "@daily" would give zero-length intervals.
    schedule=CronDataIntervalTimetable("0 0 * * *", timezone="UTC"),
    start_date=pendulum.datetime(2023, 10, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    dagrun_timeout=timedelta(hours=4),
    default_args=default_args,
    params={
        "start_date": Param(None, type=["null", "string"], format="date",
                            description="First loading_date; overrides the data interval together with end_date"),
        "end_date": Param(None, type=["null", "string"], format="date",
                          description="Last loading_date, inclusive"),
        "force_mart": Param(False, type="boolean", description="Rebuild the mart even if its inputs are unchanged"),
        "retrain": Param(False, type="boolean", description="Train even if this model version is registered"),
    },
    tags=["smart-logistics", "etl", "ml"],
    doc_md=__doc__,
) as dag:
    tasks = {
        task_id: PythonOperator(task_id=task_id, python_callable=execute_step, op_kwargs={"step": task_id},
                                execution_timeout=timeout,
                                **({} if retries is None else {"retries": retries}))
        for task_id, (timeout, retries) in STEPS.items()
    }
    publish = PythonOperator(
        task_id="publish_pipeline_statistics", python_callable=publish_statistics,
        op_kwargs={"pipeline_tasks": tuple(STEPS)}, execution_timeout=timedelta(minutes=10), retries=2,
        trigger_rule=TriggerRule.ALL_DONE,
    )

    (tasks["check_source_connectivity"] >> tasks["validate_source_data"] >> tasks["prepare_warehouse"]
     >> tasks["run_etl"] >> tasks["run_data_quality"] >> [tasks["refresh_marts"], tasks["generate_features"]])
    tasks["generate_features"] >> tasks["train_or_load_model"] >> tasks["run_batch_inference"]
    [tasks["refresh_marts"], tasks["run_batch_inference"]] >> tasks["validate_prediction_records"] >> publish
