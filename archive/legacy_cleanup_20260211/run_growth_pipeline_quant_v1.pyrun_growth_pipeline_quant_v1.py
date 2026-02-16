# ============================================================
# Module: run_growth_pipeline_quant_v1.py
# Quant Version: v1.0
# Purpose:
#   Orchestrate the full Quant v1.0 weekly growth strategy
#   pipeline:
#       1) enrichment_daily_quant_v1.py
#       2) ranking_weekly_quant_v1.py
#       3) backtest_weekly_quant_v1.py
#       4) dashboard_top10_quant_v1.py
#       5) dashboard_growth_quant_v1.py
#
# Inputs:
#   C:\Quant\data\ingestion\ingestion_5years.csv
#
# Outputs:
#   C:\Quant\data\enriched\enriched_daily.csv
#   C:\Quant\data\ranking\weekly_top10_quant_v1.csv
#   C:\Quant\data\backtest\weekly_backtest_quant_v1.csv
#   C:\Quant\data\dashboard\dashboard_top10_quant_v1.csv
#   C:\Quant\data\dashboard\dashboard_growth_quant_v1.csv
#
# Governance Rules:
#   - No schema drift.
#   - No silent changes.
#   - Deterministic, reproducible behaviour.
#   - No writing outside governed directories.
#
# Logging Rules:
#   - Must integrate with logging_quant_v1.py (future).
#
# Encoding:
#   - All CSV outputs UTF‑8.
#
# Dependencies:
#   - Python 3, subprocess, pathlib
#
# Provenance:
#   - Top‑level orchestrator for Quant v1.0 growth strategy.
#   - Any modification requires version bump.
# ============================================================

import subprocess
from pathlib import Path
import sys

BASE_SCRIPTS = Path(r"C:\Quant\scripts")

STEPS = [
    BASE_SCRIPTS / "enrichment" / "enrichment_daily_quant_v1.py",
    BASE_SCRIPTS / "ranking" / "ranking_weekly_quant_v1.py",
    BASE_SCRIPTS / "backtest" / "backtest_weekly_quant_v1.py",
    BASE_SCRIPTS / "dashboard" / "dashboard_top10_quant_v1.py",
    BASE_SCRIPTS / "dashboard" / "dashboard_growth_quant_v1.py",
]

def run_step(script_path: Path) -> None:
    result = subprocess.run(
        [sys.executable, str(script_path)],
        capture_output=True,
        text=True
    )
    if result.returncode != 0:
        raise RuntimeError(
            f"Quant v1.0 pipeline failure in {script_path.name}:\n"
            f"STDOUT:\n{result.stdout}\n\nSTDERR:\n{result.stderr}"
        )

def main() -> None:
    for script in STEPS:
        run_step(script)

if __name__ == "__main__":
    main()