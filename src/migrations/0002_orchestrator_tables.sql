CREATE TABLE IF NOT EXISTS orchestrator_cycles (
    cycle_id      TEXT PRIMARY KEY,
    started_at    TIMESTAMP WITH TIME ZONE NOT NULL,
    finished_at   TIMESTAMP WITH TIME ZONE NOT NULL,
    status        TEXT NOT NULL,
    version       JSONB NOT NULL
);

CREATE TABLE IF NOT EXISTS orchestrator_steps (
    cycle_id      TEXT NOT NULL,
    step_name     TEXT NOT NULL,
    started_at    TIMESTAMP WITH TIME ZONE NOT NULL,
    finished_at   TIMESTAMP WITH TIME ZONE NOT NULL,
    status        TEXT NOT NULL,
    issues        JSONB NOT NULL,
    PRIMARY KEY (cycle_id, step_name),
    FOREIGN KEY (cycle_id) REFERENCES orchestrator_cycles (cycle_id)
);