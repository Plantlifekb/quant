"""
Institutional‑Grade Orchestrator
--------------------------------

Aligned with the actual repo structure:

- Prices ingestion:      quant.engine.tasks.prices.run(engine)
- Fundamentals ingestion: quant.engine.tasks.fundamentals.run(engine)
- Metrics/returns:        quant.engine.tasks.returns.run(engine)

All steps emit SQL-backed events via event_log.
"""

import time
import uuid
import traceback

from quant.engine.db import create_db_engine
from quant.engine.event_log import log_event, StepTimer

# Correct task imports
from quant.engine.tasks import prices as prices_task
from quant.engine.tasks import fundamentals as fundamentals_task
from quant.engine.tasks import returns as returns_task


def run_cycle():
    """
    Execute one full orchestrator cycle with:
    - cycle_id for correlation
    - SQL‑backed event emission
    - deterministic step ordering
    - duration tracking
    - error capture
    """

    cycle_id = str(uuid.uuid4())
    log_event("cycle", "start", "info", "Cycle started", cycle_id)

    # Create DB engine once per cycle
    engine = create_db_engine()

    try:
        # -------------------------
        # Prices ingestion
        # -------------------------
        with StepTimer("prices_ingestion", cycle_id):
            prices_task.run(engine)

        # -------------------------
        # Fundamentals ingestion
        # -------------------------
        with StepTimer("fundamentals_ingestion", cycle_id):
            fundamentals_task.run(engine)

        # -------------------------
        # Metrics / returns update
        # -------------------------
        with StepTimer("metrics_update", cycle_id):
            returns_task.run(engine)

        # -------------------------
        # Cycle completed
        # -------------------------
        log_event("cycle", "end", "info", "Cycle completed", cycle_id)

    except Exception as exc:
        # Emit a cycle‑level error
        log_event(
            "cycle",
            "error",
            "error",
            f"Cycle failed: {exc}",
            cycle_id,
        )

        # Emit traceback as a separate event for debugging
        tb = traceback.format_exc()
        log_event(
            "cycle",
            "log",
            "error",
            f"Traceback:\n{tb}",
            cycle_id,
        )

        raise


def orchestrator_loop(sleep_seconds: int = 300):
    """Continuous orchestrator loop."""
    while True:
        start = time.perf_counter()

        run_cycle()

        end = time.perf_counter()
        duration = end - start

        log_event(
            "cycle",
            "log",
            "info",
            f"Cycle duration: {duration:.2f}s",
            None,
            duration=duration,
        )

        time.sleep(sleep_seconds)


if __name__ == "__main__":
    orchestrator_loop()