-- bootstrap.sql — canonical quant platform schema

CREATE SCHEMA IF NOT EXISTS public;

CREATE TABLE IF NOT EXISTS tickers (
    ticker TEXT PRIMARY KEY
);

CREATE TABLE IF NOT EXISTS public.prices (
    date      DATE    NOT NULL,
    adj_close NUMERIC,
    close     NUMERIC,
    high      NUMERIC,
    low       NUMERIC,
    open      NUMERIC,
    volume    BIGINT,
    ticker    TEXT,
    PRIMARY KEY (date, ticker)
);

CREATE TABLE IF NOT EXISTS returns (
    ticker TEXT NOT NULL REFERENCES tickers(ticker),
    date   DATE NOT NULL,
    ret    DOUBLE PRECISION NOT NULL,
    PRIMARY KEY (ticker, date)
);

CREATE TABLE IF NOT EXISTS fundamentals (
    ticker     TEXT NOT NULL REFERENCES tickers(ticker),
    as_of_date DATE NOT NULL,
    revenue    DOUBLE PRECISION,
    earnings   DOUBLE PRECISION,
    book_value DOUBLE PRECISION,
    eps        DOUBLE PRECISION,
    pe_ratio   DOUBLE PRECISION,
    pb_ratio   DOUBLE PRECISION,
    PRIMARY KEY (ticker, as_of_date)
);

CREATE TABLE IF NOT EXISTS orchestrator_cycles (
    cycle_id    TEXT PRIMARY KEY,
    started_at  TIMESTAMP WITH TIME ZONE NOT NULL,
    finished_at TIMESTAMP WITH TIME ZONE NOT NULL,
    status      TEXT NOT NULL,
    version     JSONB NOT NULL
);

CREATE TABLE IF NOT EXISTS orchestrator_steps (
    cycle_id    TEXT NOT NULL,
    step_name   TEXT NOT NULL,
    started_at  TIMESTAMP WITH TIME ZONE NOT NULL,
    finished_at TIMESTAMP WITH TIME ZONE NOT NULL,
    status      TEXT NOT NULL,
    issues      JSONB NOT NULL,
    PRIMARY KEY (cycle_id, step_name),
    FOREIGN KEY (cycle_id) REFERENCES orchestrator_cycles (cycle_id)
);

CREATE TABLE IF NOT EXISTS task_metadata (
    task_name    TEXT PRIMARY KEY,
    status       TEXT NOT NULL DEFAULT 'never_run',
    last_run     TIMESTAMP WITH TIME ZONE,
    last_success TIMESTAMP WITH TIME ZONE,
    last_error   TEXT
);

CREATE TABLE IF NOT EXISTS task_run_history (
    id           BIGSERIAL PRIMARY KEY,
    task_name    TEXT                     NOT NULL,
    run_started  TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now(),
    run_finished TIMESTAMP WITH TIME ZONE,
    status       TEXT                     NOT NULL,
    error_text   TEXT
);

CREATE INDEX IF NOT EXISTS idx_task_run_history_task_time
    ON task_run_history (task_name, run_started DESC);
