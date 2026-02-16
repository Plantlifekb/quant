"""
Quant v1.1 — regime_suite_runner_quant_v1.py

Purpose:
    Execute the full regime-aware analytics spine in the correct dependency order.

Modules executed:
    1. regime_factor_weighting_quant_v1.py
    2. risk_regime_quant_v1.py
    3. factor_exposure_report_quant_v1.py
    4. turnover_regime_quant_v1.py
    5. attribution_regime_quant_v1.py
    6. optimiser_regime_quant_v1.py

Governance:
    - Deterministic ordering
    - UTC timestamps
    - No schema drift
    - All modules logged via logging_quant_v1
"""

import sys
from pathlib import Path
from datetime import datetime, timezone
import importlib

PROJECT_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(PROJECT_ROOT / "scripts"))

from logging_quant_v1 import get_logger  # type: ignore

logger = get_logger("regime_suite_runner_quant_v1")


def iso_now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")


# ---------------------------------------------------------
# Helper: run a module.main() with logging
# ---------------------------------------------------------

def run_module(module_name: str):
    logger.info(f"Running module: {module_name}")
    try:
        mod = importlib.import_module(f"analytics.{module_name}")
        if hasattr(mod, "main"):
            mod.main()
        else:
            raise RuntimeError(f"{module_name} has no main() function.")
        logger.info(f"Completed module: {module_name}")
    except Exception as e:
        logger.error(f"Error in module {module_name}: {e}", exc_info=True)
        raise


# ---------------------------------------------------------
# Main suite runner
# ---------------------------------------------------------

def main():
    logger.info("Starting regime_suite_runner_quant_v1 (v1.1)")
    logger.info(f"Run timestamp: {iso_now()}")

    # ORDER MATTERS — upstream → downstream dependencies
    pipeline = [
        "regime_factor_weighting_quant_v1",   # produces quant_factor_returns_regime_v1.csv
        "risk_regime_quant_v1",               # consumes factor returns → produces quant_risk_regime_v1.csv
        "factor_exposure_report_quant_v1",    # consumes risk-ensemble → produces exposures
        "turnover_regime_quant_v1",           # consumes weights → produces turnover by regime
        "attribution_regime_quant_v1",        # consumes attribution → produces regime attribution
        "optimiser_regime_quant_v1",          # consumes everything → produces final weights
    ]

    for module_name in pipeline:
        run_module(module_name)

    logger.info("regime_suite_runner_quant_v1 completed successfully.")


if __name__ == "__main__":
    main()