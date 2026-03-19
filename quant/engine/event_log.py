"""
SQL-backed Event Emitter
------------------------

This module provides a clean, narratable, institutional-grade interface
for emitting operational events from the orchestrator into the
PostgreSQL event_log table.

It replaces the old in-memory dashboard emitter entirely.
"""

import time
import uuid
import os
import psycopg2
from contextlib import contextmanager


DATABASE_URL = os.environ["DATABASE_URL"]


@contextmanager
def get_conn():
    conn = psycopg2.connect(DATABASE_URL)
    try:
        yield conn
    finally:
        conn.close()


def log_event(step, event_type, severity, message=None, cycle_id=None, duration=None):
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO event_log (step, event_type, severity, message, cycle_id, duration)
                VALUES (%s, %s, %s, %s, %s, %s)
                """,
                (step, event_type, severity, message, cycle_id, duration),
            )
            conn.commit()


class StepTimer:
    """
    Context manager that automatically emits:
    - step begin
    - step end
    - duration
    - error events
    """

    def __init__(self, step, cycle_id=None):
        self.step = step
        self.cycle_id = cycle_id

    def __enter__(self):
        self.start = time.perf_counter()
        log_event(
            step=self.step,
            event_type="start",
            severity="info",
            message=f"{self.step} started",
            cycle_id=self.cycle_id,
        )
        return self

    def __exit__(self, exc_type, exc, tb):
        end = time.perf_counter()
        duration = end - self.start

        if exc_type:
            log_event(
                step=self.step,
                event_type="error",
                severity="error",
                message=f"{self.step} failed: {exc}",
                cycle_id=self.cycle_id,
                duration=duration,
            )
        else:
            log_event(
                step=self.step,
                event_type="end",
                severity="info",
                message=f"{self.step} completed",
                cycle_id=self.cycle_id,
                duration=duration,
            )