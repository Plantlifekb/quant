# ============================================================
# Module: backtest_weekly_quant_v1.py
# Quant Version: v1.0
# Purpose:
#   Compute governed weekly PnL for the Quant v1.0
#   top‑10 growth strategy using:
#       enriched_daily.csv
#       weekly_top10_quant_v1.csv
#
# Inputs:
#   C:\Quant\data\enriched\enriched_daily.csv
#   C:\Quant\data\ranking\weekly_top10_quant_v1.csv
#
# Outputs:
#   C:\Quant\data\backtest\weekly_backtest_quant_v1.csv
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
#   - pandas, numpy, pathlib
#
# Provenance:
#   - Part of governed Quant v1.0 pipeline.
#   - Any modification requires version bump.
# ============================================================

import pandas as pd
from pathlib import Path

BASE = Path(r"C:\Quant\data")
ENRICH = BASE / "enriched" / "enriched_daily.csv"
RANK = BASE / "ranking" / "weekly_top10_quant_v1.csv"
OUT = BASE / "backtest" / "weekly_backtest_quant_v1.csv"

df = pd.read_csv(ENRICH)
df.columns = [c.lower() for c in df.columns]
df["date"] = pd.to_datetime(df["date"])

top10 = pd.read_csv(RANK)
top10.columns = [c.lower() for c in top10.columns]
top10["date"] = pd.to_datetime(top10["date"])

# Compute next‑week returns for each ticker
df["ret_1w"] = df.groupby("ticker")["adj_close"].pct_change(5)

# Merge weekly returns into ranking file
merged = top10.merge(
    df[["date", "ticker", "ret_1w"]],
    on=["date", "ticker"],
    how="left"
)

# Equal‑weight top 10
weekly = merged.groupby("date")["ret_1w"].mean().reset_index()
weekly = weekly.sort_values("date")

# Cumulative return
weekly["cumulative_return"] = (1 + weekly["ret_1w"]).cumprod() - 1

# Output directory
OUT.parent.mkdir(parents=True, exist_ok=True)
weekly.to_csv(OUT, index=False)