-- Runs of the Airflow DAG smart_logistics_data_pipeline are recorded with mode 'airflow'.
-- Repeatable. Replaces the mode check only while it does not allow 'airflow' yet.

BEGIN;

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conrelid = 'audit.pipeline_run'::regclass AND conname = 'pipeline_run_mode_check'
          AND pg_get_constraintdef(oid) LIKE '%airflow%'
    ) THEN
        ALTER TABLE audit.pipeline_run DROP CONSTRAINT IF EXISTS pipeline_run_mode_check;
        ALTER TABLE audit.pipeline_run ADD CONSTRAINT pipeline_run_mode_check
            CHECK (mode IN ('full', 'etl', 'mart', 'train', 'score', 'backfill', 'airflow'));
    END IF;
END
$$;

COMMENT ON TABLE audit.pipeline_run IS
    'One row per scripts.run_pipeline execution or Airflow DAG run (mode airflow, run_id derived from the '
    'Airflow run_id): per-task status, counts, metrics, and logical fingerprints.';

COMMIT;
