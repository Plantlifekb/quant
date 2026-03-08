CREATE TABLE IF NOT EXISTS task_metadata (
    task_name TEXT PRIMARY KEY,
    status TEXT NOT NULL DEFAULT 'never_run',
    last_run TIMESTAMP WITH TIME ZONE,
    last_success TIMESTAMP WITH TIME ZONE,
    last_error TEXT
);