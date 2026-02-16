# ============================================================
# Module: ranking_weekly_quant_v1.py
# Quant Version: v1.0
# Purpose:
#   Generate weekly top‑10 rankings for the Quant v1.0
#   growth strategy using enriched_daily.csv.
#
# Inputs:
#   C:\Quant\data\enriched\enriched_daily.csv
#
# Outputs:
#   C:\Quant\data\ranking\weekly_top10_quant_v1.csv
#
# Expected Output Schema:
#   date, ticker, score, rank, mkt_trend
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
OUT = BASE / "ranking" / "weekly_top10_quant_v1.csv"

df = pd.read_csv(ENRICH)
df.columns = [c.lower() for c in df.columns]

df["date"] = pd.to_datetime(df["date"])
df = df.sort_values(["date", "ticker"])

# Mondays only
mondays = df[df["is_monday"]]

# Only when market trend is positive
mondays = mondays[mondays["mkt_trend"] == True]

# Rank by score (descending)
mondays["rank"] = mondays.groupby("date")["score"].rank(method="first", ascending=False)

# Top 10 only
top10 = mondays[mondays["rank"] <= 10]

# Output directory
OUT.parent.mkdir(parents=True, exist_ok=True)

top10.to_csv(OUT, index=False)