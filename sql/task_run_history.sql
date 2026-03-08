CREATE TABLE IF NOT EXISTS task_run_history (
    id           bigserial PRIMARY KEY,
    task_name    text                     NOT NULL,
    run_started  timestamp with time zone NOT NULL DEFAULT now(),
    run_finished timestamp with time zone,
    status       text                     NOT NULL,
    error_text   text
);

CREATE INDEX IF NOT EXISTS idx_task_run_history_task_time
    ON task_run_history (task_name, run_started DESC);