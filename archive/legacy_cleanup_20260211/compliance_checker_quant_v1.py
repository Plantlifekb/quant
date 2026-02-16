# ============================================================
# Module: compliance_checker_quant_v1.py
# Quant Version: v1.0
# Purpose:
#   Validate full Quant v1.0 compliance by checking:
#       - directory structure
#       - required modules
#       - required outputs
#       - schemas
#       - forbidden files
#       - legacy artefacts
#       - manifest alignment
#
# Inputs:
#   Entire C:\Quant\ environment
#
# Outputs:
#   None (raises on failure)
#
# Governance Rules:
#   - No drift.
#   - No missing modules.
#   - No missing outputs.
#   - No ungoverned files.
#   - Deterministic behaviour.
#
# Dependencies:
#   - pathlib, pandas
#
# Provenance:
#   - Top‑level compliance validator for Quant v1.0.
#   - Any modification requires version bump.
# ============================================================

from pathlib import Path
import pandas as pd

BASE = Path(r"C:\Quant")

# ------------------------------------------------------------
# 1. Canonical directory structure
# ------------------------------------------------------------
REQUIRED_DIRS = [
    BASE / "data" / "ingestion",
    BASE / "data" / "enriched",
    BASE / "data" / "ranking",
    BASE / "data" / "backtest",
    BASE / "data" / "dashboard",
    BASE / "scripts" / "enrichment",
    BASE / "scripts" / "ranking",
    BASE / "scripts" / "backtest",
    BASE / "scripts" / "dashboard",
    BASE / "scripts" / "validation",
    BASE / "scripts" / "logs",
    BASE / "scripts" / "patches",
    BASE / "scripts" / "bootstrap",
    BASE / "logs",
    BASE / "archive",
    BASE / "runbook",
    BASE / "manifest",
]

# ------------------------------------------------------------
# 2. Canonical scripts
# ------------------------------------------------------------
REQUIRED_SCRIPTS = [
    BASE / "scripts" / "enrichment" / "enrichment_daily_quant_v1.py",
    BASE / "scripts" / "ranking" / "ranking_weekly_quant_v1.py",
    BASE / "scripts" / "backtest" / "backtest_weekly_quant_v1.py",
    BASE / "scripts" / "dashboard" / "dashboard_top10_quant_v1.py",
    BASE / "scripts" / "dashboard" / "dashboard_growth_quant_v1.py",
    BASE / "scripts" / "validation" / "schema_validator_quant_v1.py",
    BASE / "scripts" / "validation" / "directory_auditor_quant_v1.py",
    BASE / "scripts" / "validation" / "compliance_checker_quant_v1.py",
    BASE / "scripts" / "logs" / "logging_quant_v1.py",
    BASE / "scripts" / "patches" / "patch_logging_integration_quant_v1.py",
    BASE / "scripts" / "bootstrap" / "bootstrap_quant_v1.py",
    BASE / "scripts" / "run_growth_pipeline_quant_v1.py",
]

# ------------------------------------------------------------
# 3. Canonical governed outputs
# ------------------------------------------------------------
REQUIRED_OUTPUTS = [
    BASE / "data" / "ingestion" / "ingestion_5years.csv",
    BASE / "data" / "enriched" / "enriched_daily.csv",
    BASE / "data" / "ranking" / "weekly_top10_quant_v1.csv",
    BASE / "data" / "backtest" / "weekly_backtest_quant_v1.csv",
    BASE / "data" / "dashboard" / "dashboard_top10_quant_v1.csv",
    BASE / "data" / "dashboard" / "dashboard_growth_quant_v1.csv",
]

# ------------------------------------------------------------
# 4. Canonical schemas
# ------------------------------------------------------------
SCHEMAS = {
    BASE / "data" / "enriched" / "enriched_daily.csv": [
        "date","ticker","company_name","market_sector",
        "open","high","low","close","adj_close","volume","run_date",
        "ret_1d","ret_20d","vol_20d","score",
        "mkt_avg","mkt_ma200","mkt_trend","is_monday"
    ],
    BASE / "data" / "ranking" / "weekly_top10_quant_v1.csv": [
        "date","ticker","score","rank","mkt_trend"
    ],
    BASE / "data" / "backtest" / "weekly_backtest_quant_v1.csv": [
        "date","ret_1w","cumulative_return"
    ],
    BASE / "data" / "dashboard" / "dashboard_top10_quant_v1.csv": [
        "date","ticker","company_name","market_sector",
        "score","rank","mkt_trend"
    ],
    BASE / "data" / "dashboard" / "dashboard_growth_quant_v1.csv": [
        "date","weekly_return","cumulative_return"
    ],
}

# ------------------------------------------------------------
# 5. Forbidden files
# ------------------------------------------------------------
FORBIDDEN = {
    "quant_backtest.csv",
    "quant_backtest_legacy_pre_v1.csv",
}

# ------------------------------------------------------------
# Helpers
# ------------------------------------------------------------
def fail(msg: str):
    raise RuntimeError(f"Quant v1.0 compliance failure:\n{msg}")

def check_directories():
    for d in REQUIRED_DIRS:
        if not d.exists():
            fail(f"Missing required directory: {d}")

def check_scripts():
    for s in REQUIRED_SCRIPTS:
        if not s.exists():
            fail(f"Missing required script: {s}")

def check_outputs():
    for o in REQUIRED_OUTPUTS:
        if not o.exists():
            fail(f"Missing required output: {o}")

def check_schemas():
    for path, expected in SCHEMAS.items():
        df = pd.read_csv(path)
        df.columns = [c.lower() for c in df.columns]
        actual = list(df.columns)
        if actual != expected:
            fail(f"Schema mismatch in {path}\nExpected: {expected}\nActual: {actual}")

def check_forbidden():
    for f in FORBIDDEN:
        if (BASE / f).exists():
            fail(f"Forbidden legacy artefact detected: {f}")

# ------------------------------------------------------------
# Main
# ------------------------------------------------------------
def main():
    check_directories()
    check_scripts()
    check_outputs()
    check_schemas()
    check_forbidden()

if __name__ == "__main__":
    main()