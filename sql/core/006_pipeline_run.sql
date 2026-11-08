-- End-to-end pipeline runs written by scripts/run_pipeline.py.
-- Repeatable. One row per run, written when the run finishes.

BEGIN;

CREATE TABLE IF NOT EXISTS audit.pipeline_run (
    run_id          uuid PRIMARY KEY,
    mode            text NOT NULL,
    status          text NOT NULL,
    start_date      date,
    end_date        date,
    as_of_date      date NOT NULL,
    started_at      timestamptz NOT NULL,
    completed_at    timestamptz NOT NULL,
    duration_s      numeric(12, 3) NOT NULL,
    failed_task     text,
    error_message   text,
    tasks           jsonb NOT NULL,
    report          jsonb NOT NULL,
    CONSTRAINT pipeline_run_mode_check
        CHECK (mode IN ('full', 'etl', 'mart', 'train', 'score', 'backfill')),
    CONSTRAINT pipeline_run_status_check CHECK (status IN ('succeeded', 'failed')),
    CONSTRAINT pipeline_run_failure_check CHECK ((status = 'failed') = (failed_task IS NOT NULL)),
    CONSTRAINT pipeline_run_window_check CHECK (
        (start_date IS NULL AND end_date IS NULL)
        OR (start_date IS NOT NULL AND end_date IS NOT NULL AND start_date <= end_date)
    ),
    CONSTRAINT pipeline_run_time_check CHECK (completed_at >= started_at AND duration_s >= 0)
);

CREATE INDEX IF NOT EXISTS pipeline_run_mode_started_idx ON audit.pipeline_run (mode, started_at);

COMMENT ON TABLE audit.pipeline_run IS
    'One row per scripts.run_pipeline execution: per-task status, counts, metrics, and logical fingerprints.';
COMMENT ON COLUMN audit.pipeline_run.report IS
    'The execution report also written to artifacts/reports/. fingerprints compare logical results across runs.';

COMMIT;
