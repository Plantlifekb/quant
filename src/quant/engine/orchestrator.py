# quant/engine/orchestrator.py

import logging
import sys
import time
from datetime import datetime, timezone
from typing import Callable, List

from quant.engine.db import create_db_engine

# -----------------------------------------------------------------------------
# Logging setup
# -----------------------------------------------------------------------------

LOGGER = logging.getLogger("quant.orchestrator")
LOGGER.setLevel(logging.INFO)

_handler = logging.StreamHandler(sys.stdout)
_handler.setFormatter(
    logging.Formatter(
        fmt="%(asctime)s | %(name)s | %(levelname)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
)
LOGGER.addHandler(_handler)


# -----------------------------------------------------------------------------
# Task definitions
# -----------------------------------------------------------------------------

class Task:
    def __init__(self, name: str, fn: Callable[[], None]):
        self.name = name
        self.fn = fn

    def run(self) -> None:
        LOGGER.info(f"task_start name={self.name}")
        start_ts = datetime.now(timezone.utc)
        try:
            self.fn()
            status = "success"
            error_msg = None
            LOGGER.info(f"task_end name={self.name} status=success")
        except Exception as exc:
            status = "failed"
            error_msg = repr(exc)
            LOGGER.exception(f"task_end name={self.name} status=failed error={exc}")
        end_ts = datetime.now(timezone.utc)
        self._record_run(start_ts, end_ts, status, error_msg)

    def _record_run(
        self,
        start_ts: datetime,
        end_ts: datetime,
        status: str,
        error_msg: str | None,
    ) -> None:
        """
        Minimal audit logging into task_run_history if present.
        Safe no‑op if table does not exist.
        """
        try:
            engine = create_db_engine()
            with engine.begin() as conn:
                conn.exec_driver_sql(
                    """
                    INSERT INTO task_run_history
                        (task_name, run_started, run_finished, status, error_text)
                    VALUES
                        (%(task_name)s, %(started_at)s, %(finished_at)s, %(status)s, %(error_message)s)
                    """,
                    {
                        "task_name": self.name,
                        "started_at": start_ts,
                        "finished_at": end_ts,
                        "status": status,
                        "error_message": error_msg,
                    },
                )
        except Exception as exc:
            # Never let audit logging kill the orchestrator
            LOGGER.warning(f"audit_log_failed task={self.name} error={exc!r}")


# -----------------------------------------------------------------------------
# Concrete task implementations
# -----------------------------------------------------------------------------

def task_ingest_prices() -> None:
    from quant.engine.tasks.prices import run as run_prices
    engine = create_db_engine()
    run_prices(engine)


def task_ingest_fundamentals() -> None:
    from quant.engine.tasks.fundamentals import run as run_fundamentals
    engine = create_db_engine()
    run_fundamentals(engine)


def build_dag() -> List[Task]:
    """
    Encode the real dependency graph here.
    Order in the list = execution order for this simple DAG.
    """
    return [
        Task("ingest_prices", task_ingest_prices),
        Task("ingest_fundamentals", task_ingest_fundamentals),
    ]


# -----------------------------------------------------------------------------
# Orchestrator loop
# -----------------------------------------------------------------------------

def run_dag_once() -> None:
    LOGGER.info("dag_start")
    tasks = build_dag()
    for task in tasks:
        task.run()
    LOGGER.info("dag_end")


def main_loop(
    interval_seconds: int = 60,
) -> None:
    """
    Long‑running orchestrator loop.

    interval_seconds = time between DAG runs.
    For a real daily schedule, you’d replace this with
    market‑aware scheduling (e.g., sleep until next market open).
    """
    LOGGER.info(
        f"orchestrator_loop_start interval_seconds={interval_seconds}"
    )
    while True:
        loop_start = datetime.now(timezone.utc)
        LOGGER.info(f"orchestrator_iteration_start at={loop_start.isoformat()}")

        try:
            run_dag_once()
        except Exception as exc:
            LOGGER.exception(f"orchestrator_iteration_failed error={exc!r}")

        loop_end = datetime.now(timezone.utc)
        LOGGER.info(f"orchestrator_iteration_end at={loop_end.isoformat()}")

        LOGGER.info(f"orchestrator_sleep seconds={interval_seconds}")
        time.sleep(interval_seconds)


# -----------------------------------------------------------------------------
# CLI entrypoint
# -----------------------------------------------------------------------------

def main() -> None:
    """
    Entry point for `python -m quant.engine.orchestrator`.
    """
    # For now, fixed interval. You can later wire this to env vars.
    main_loop(interval_seconds=60)


if __name__ == "__main__":
    main()