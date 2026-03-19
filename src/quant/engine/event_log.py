"""
SQL-backed Event Emitter

Provides a safe interface for emitting operational events into the
PostgreSQL event_log table. This module avoids reading configuration
at import time and handles transient DB failures gracefully.
"""

import os
import time
import uuid
import logging
from contextlib import contextmanager

import psycopg2
from psycopg2 import OperationalError, DatabaseError

# Configure module logger
logger = logging.getLogger("quant.event_log")
if not logger.handlers:
    # Basic fallback handler; Docker will capture stdout/stderr
    handler = logging.StreamHandler()
    formatter = logging.Formatter("%(asctime)s %(levelname)s %(name)s: %(message)s")
    handler.setFormatter(formatter)
    logger.addHandler(handler)
logger.setLevel(os.getenv("EVENT_LOG_LEVEL", "INFO"))

# Connection retry configuration (can be tuned via environment)
DEFAULT_RETRIES = int(os.getenv("DB_CONNECT_RETRIES", "5"))
DEFAULT_BACKOFF = float(os.getenv("DB_CONNECT_BACKOFF", "0.5"))  # seconds, exponential

def _get_database_url():
    """Return DATABASE_URL or raise a clear RuntimeError if missing."""
    db_url = os.getenv("DATABASE_URL")
    if not db_url:
        raise RuntimeError(
            "DATABASE_URL environment variable is required. "
            "Set it in docker-compose.yml under the orchestrator service."
        )
    return db_url

@contextmanager
def get_conn(retries: int = DEFAULT_RETRIES, backoff: float = DEFAULT_BACKOFF):
    """
    Context manager that yields a psycopg2 connection.
    Retries on OperationalError with exponential backoff.
    Raises RuntimeError if DATABASE_URL is missing.
    """
    db_url = _get_database_url()
    attempt = 0
    last_exc = None
    while attempt <= retries:
        try:
            # Use a short connect timeout to fail fast on unreachable DB
            conn = psycopg2.connect(dsn=db_url, connect_timeout=5)
            try:
                yield conn
            finally:
                try:
                    conn.close()
                except Exception as e:
                    logger.debug("Error closing DB connection: %s", e)
            return
        except OperationalError as e:
            last_exc = e
            attempt += 1
            if attempt > retries:
                logger.error("Failed to connect to DB after %d attempts: %s", retries, e)
                raise
            sleep_time = backoff * (2 ** (attempt - 1))
            logger.warning(
                "Database connection failed (attempt %d/%d). Retrying in %.2fs. Error: %s",
                attempt,
                retries,
                sleep_time,
                e,
            )
            time.sleep(sleep_time)
    # If we exit loop without returning, raise the last exception
    raise last_exc or RuntimeError("Unknown error connecting to database")

def _safe_execute(conn, query, params):
    """Execute a query and commit, raising DatabaseError on failure."""
    with conn.cursor() as cur:
        cur.execute(query, params)
    conn.commit()

def log_event(step, event_type, severity, message=None, cycle_id=None, duration=None):
    """
    Insert an event into the event_log table.

    This function will not raise on transient DB errors; instead it logs
    the failure to the module logger so the orchestrator can continue.
    """
    try:
        with get_conn() as conn:
            try:
                _safe_execute(
                    conn,
                    """
                    INSERT INTO event_log (step, event_type, severity, message, cycle_id, duration)
                    VALUES (%s, %s, %s, %s, %s, %s)
                    """,
                    (step, event_type, severity, message, cycle_id, duration),
                )
            except DatabaseError as db_err:
                # Log the failure locally; do not raise to avoid crashing orchestrator
                logger.exception(
                    "Failed to write event to event_log table. Event: step=%s event_type=%s severity=%s message=%s cycle_id=%s duration=%s. DB error: %s",
                    step,
                    event_type,
                    severity,
                    message,
                    cycle_id,
                    duration,
                    db_err,
                )
    except RuntimeError as rt_err:
        # Missing DATABASE_URL or other configuration error
        logger.error("Event not recorded because configuration is invalid: %s", rt_err)
    except OperationalError as op_err:
        # Connection attempts exhausted
        logger.error("Event not recorded because DB connection failed: %s", op_err)
    except Exception as e:
        # Catch-all to avoid unexpected exceptions bubbling up
        logger.exception("Unexpected error while logging event: %s", e)

class StepTimer:
    """
    Context manager that emits:
    - step start
    - step end with duration
    - error events on exception
    """

    def __init__(self, step, cycle_id=None):
        self.step = step
        self.cycle_id = cycle_id

    def __enter__(self):
        self.start = time.perf_counter()
        try:
            log_event(
                step=self.step,
                event_type="start",
                severity="info",
                message=f"{self.step} started",
                cycle_id=self.cycle_id,
            )
        except Exception:
            # log_event already handles exceptions; ensure __enter__ never raises
            logger.debug("Suppressed exception from log_event during StepTimer.__enter__")
        return self

    def __exit__(self, exc_type, exc, tb):
        end = time.perf_counter()
        duration = end - self.start

        if exc_type:
            try:
                log_event(
                    step=self.step,
                    event_type="error",
                    severity="error",
                    message=f"{self.step} failed: {exc}",
                    cycle_id=self.cycle_id,
                    duration=duration,
                )
            except Exception:
                logger.debug("Suppressed exception from log_event during StepTimer.__exit__ error path")
        else:
            try:
                log_event(
                    step=self.step,
                    event_type="end",
                    severity="info",
                    message=f"{self.step} completed",
                    cycle_id=self.cycle_id,
                    duration=duration,
                )
            except Exception:
                logger.debug("Suppressed exception from log_event during StepTimer.__exit__ success path")