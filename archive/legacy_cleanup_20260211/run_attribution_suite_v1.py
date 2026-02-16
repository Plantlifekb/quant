"""
run_attribution_suite_v1.py
Quant v1.0 — Attribution Suite
------------------------------------------------------------

Master orchestrator for the attribution suite.
This is the ONLY script Task Scheduler needs to run daily
for attribution-specific outputs.

Execution order:
    1. factor_returns_alignment_v1
    2. attribution_sector_v1
    3. attribution_regime_summary_v1
    4. attribution_rolling_v1
    5. liquidity_costs_enhanced_v1
    6. attribution_expected_vs_realised_v1
    7. attribution_dashboard_v1
    8. attribution_diagnostics_v1

Each module logs independently using logging_attribution_suite_v1.
This orchestrator logs high-level progress and safe-failure behaviour.
"""

import subprocess
import sys
from pathlib import Path
from datetime import datetime, timezone

from logging_attribution_suite_v1 import get_logger

logger = get_logger("run_attribution_suite_v1")

# ---------------------------------------------------------------------
# Suite directory anchor
# ---------------------------------------------------------------------
SUITE_DIR = Path(__file__).resolve().parent


def iso_now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")


# ---------------------------------------------------------------------
# Helper: run a module safely
# ---------------------------------------------------------------------
def run_module(module_name: str) -> bool:
    """
    Run a module as a subprocess.
    Returns True if successful, False otherwise.
    """
    logger.info("Running module: %s", module_name)

    module_path = SUITE_DIR / module_name

    try:
        result = subprocess.run(
            [sys.executable, str(module_path)],
            capture_output=True,
            text=True,
        )

        if result.returncode != 0:
            logger.error(
                "Module %s failed with return code %s\nSTDERR:\n%s",
                module_name,
                result.returncode,
                result.stderr,
            )
            return False

        logger.info("Module %s completed successfully.", module_name)
        return True

    except Exception as e:
        logger.error("Exception while running %s: %s", module_name, str(e))
        return False


# ---------------------------------------------------------------------
# Main Orchestration
# ---------------------------------------------------------------------
def main():
    logger.info("------------------------------------------------------------")
    logger.info("Starting Attribution Suite v1.0")
    logger.info("Run timestamp: %s", iso_now())
    logger.info("Suite directory: %s", SUITE_DIR)
    logger.info("------------------------------------------------------------")

    modules = [
        "factor_returns_alignment_v1.py",
        "attribution_sector_v1.py",
        "attribution_regime_summary_v1.py",
        "attribution_rolling_v1.py",
        "liquidity_costs_enhanced_v1.py",
        "attribution_expected_vs_realised_v1.py",
        "attribution_dashboard_v1.py",
        "attribution_diagnostics_v1.py",
    ]

    for module in modules:
        ok = run_module(module)
        if not ok:
            logger.error("Stopping suite due to failure in %s", module)
            logger.info("Attribution Suite v1.0 terminated with errors.")
            return

    logger.info("------------------------------------------------------------")
    logger.info("Attribution Suite v1.0 completed successfully.")
    logger.info("------------------------------------------------------------")


if __name__ == "__main__":
    main()