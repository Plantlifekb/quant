# ============================================================
# Module: enrichment_daily_quant_v1.py
# Quant Version: v1.0
# Purpose:
#   Build the governed enrichment layer for Quant v1.0.
#   Computes deterministic, schema‑consistent daily features:
#       ret_1d, ret_20d, vol_20d, score,
#       mkt_avg, mkt_ma200, mkt_trend, is_monday
#
# Inputs:
#   C:\Quant\data\ingestion\ingestion_5years.csv
#
# Outputs:
#   C:\Quant\data\enriched\enriched_daily.csv
#
# Expected Output Schema (lowercase, ISO‑8601 dates):
#   date, ticker, company_name, market_sector,
#   open, high, low, close, adj_close, volume, run_date,
#   ret_1d, ret_20d, vol_20d, score,
#   mkt_avg, mkt_ma200, mkt_trend, is_monday
#
# Governance Rules:
#   - No schema drift.
#   - No silent changes.
#   - Lowercase column names only.
#   - ISO‑8601 date formats only.
#   - Deterministic, reproducible behaviour.
#   - No writing outside governed directories.
#
# Logging Rules:
#   - Must use logging_quant_v1.py (future integration).
#   - Log start, end, and key events.
#
# Encoding:
#   - All CSV outputs UTF‑8.
#
# Dependencies:
#   - pandas, numpy, pathlib
#
# Provenance:
#   - Part of governed Quant v1.0 pipeline.
#   - Any modification requires version bump.
# ============================================================

import pandas as pd
import numpy as np
from pathlib import Path

BASE = Path(r"C:\Quant\data")
INGEST = BASE / "ingestion" / "ingestion_5years.csv"
OUT = BASE / "enriched" / "enriched_daily.csv"

df = pd.read_csv(INGEST)
df.columns = [c.lower() for c in df.columns]

df["date"] = pd.to_datetime(df["date"])
df = df.sort_values(["ticker", "date"])

# Daily returns
df["ret_1d"] = df.groupby("ticker")["adj_close"].pct_change()

# 20‑day return
df["ret_20d"] = df.groupby("ticker")["adj_close"].pct_change(20)

# 20‑day volatility
df["vol_20d"] = (
    df.groupby("ticker")["ret_1d"]
    .rolling(20)
    .std()
    .reset_index(0, drop=True)
)

# Growth score
df["score"] = df["ret_20d"] / df["vol_20d"]

# Market average
mkt = df.groupby("date")["adj_close"].mean().rename("mkt_avg")
df = df.merge(mkt, on="date", how="left")

# 200‑day MA of market average
mkt_ma200 = mkt.rolling(200).mean().rename("mkt_ma200")
df = df.merge(mkt_ma200, on="date", how="left")

# Trend filter
df["mkt_trend"] = df["mkt_avg"] > df["mkt_ma200"]

# Monday marker
df["is_monday"] = df["date"].dt.weekday == 0

OUT.parent.mkdir(parents=True, exist_ok=True)
df.to_csv(OUT, index=False)