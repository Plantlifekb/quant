"""
Thin launcher wrapper for the quant engine.

Kept import‑safe: no top‑level import of orchestrator.
"""

import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("quant.engine.launcher")


def launch_full() -> None:
    """
    Launch the full pipeline.
    """
    from quant.engine.orchestrator import run_all
    run_all()


def launch_tasks(tasks: list[str]) -> None:
    """
    Launch a subset of tasks.
    """
    from quant.engine.orchestrator import run_pipeline
    run_pipeline(tasks or None)


if __name__ == "__main__":
    launch_full()