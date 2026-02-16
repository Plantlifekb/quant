# ============================================================
# Module: dashboard_growth_quant_v1.py
# Quant Version: v1.0
# Purpose:
#   Build governed dashboard output for the 5‑year growth
#   strategy performance using weekly_backtest_quant_v1.csv.
#
# Inputs:
#   C:\Quant\data\backtest\weekly_backtest_quant_v1.csv
#
# Outputs:
#   C:\Quant\data\dashboard\dashboard_growth_quant_v1.csv
#
# Expected Output Schema:
#   date, weekly_return, cumulative_return
#
# Governance Rules:
#   - No schema drift.
#   - No silent changes.
#   - Lowercase column names only.
#   - ISO‑8601 dates only.
#   - Deterministic behaviour.
#
# Logging Rules:
#   - Must integrate with logging_quant_v1.py (future).
#
# Encoding:
#   - UTF‑8 CSV output.
#
# Dependencies:
#   - pandas, pathlib
#
# Provenance:
#   - Part of governed Quant v1.0 dashboard pipeline.
#   - Any modification requires version bump.
# ============================================================

import pandas as pd
from pathlib import Path

BASE = Path(r"C:\Quant\data")
BACKTEST = BASE / "backtest" / "weekly_backtest_quant_v1.csv"
OUT = BASE / "dashboard" / "dashboard_growth_quant_v1.csv"

df = pd.read_csv(BACKTEST)
df.columns = [c.lower() for c in df.columns]
df["date"] = pd.to_datetime(df["date"])

# Output directory
OUT.parent.mkdir(parents=True, exist_ok=True)
df.to_csv(OUT, index=False)