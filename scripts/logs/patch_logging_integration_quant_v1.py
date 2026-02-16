# ============================================================
# Module: patch_logging_integration_quant_v1.py
# Quant Version: v1.0
# Purpose:
#   Inject governed logging into all Quant v1.0 modules:
#       enrichment_daily_quant_v1.py
#       ranking_weekly_quant_v1.py
#       backtest_weekly_quant_v1.py
#       dashboard_top10_quant_v1.py
#       dashboard_growth_quant_v1.py
#       run_growth_pipeline_quant_v1.py
#
# Behaviour:
#   - Adds start/end log lines.
#   - Adds module‑level logger.
#   - No logic changes.
#   - No schema changes.
#   - No drift.
#
# Governance Rules:
#   - Deterministic.
#   - ASCII‑safe logs only.
#   - No silent changes.
#   - Any modification requires version bump.
#
# Dependencies:
#   - pathlib, fileinput, re
#
# Provenance:
#   - Part of governed Quant v1.0 patch pipeline.
# ============================================================

import re
from pathlib import Path
import fileinput

BASE = Path(r"C:\Quant\scripts")
TARGETS = [
    BASE / "enrichment" / "enrichment_daily_quant_v1.py",
    BASE / "ranking" / "ranking_weekly_quant_v1.py",
    BASE / "backtest" / "backtest_weekly_quant_v1.py",
    BASE / "dashboard" / "dashboard_top10_quant_v1.py",
    BASE / "dashboard" / "dashboard_growth_quant_v1.py",
    BASE / "run_growth_pipeline_quant_v1.py",
]

INSERT_BLOCK = [
    "from logs.logging_quant_v1 import get_logger",
    "logger = get_logger(__name__)",
    "logger.info('start')",
]

END_BLOCK = [
    "logger.info('end')",
]

def patch_file(path: Path):
    lines = path.read_text().splitlines()

    # Insert logging block after header
    for i, line in enumerate(lines):
        if re.match(r"^import ", line):
            insert_at = i + 1
            break
    else:
        return

    new_lines = (
        lines[:insert_at]
        + INSERT_BLOCK
        + lines[insert_at:]
        + END_BLOCK
    )

    path.write_text("\n".join(new_lines))

def main():
    for script in TARGETS:
        patch_file(script)

if __name__ == "__main__":
    main()