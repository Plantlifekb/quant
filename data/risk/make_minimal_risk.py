# make_minimal_risk.py
from pathlib import Path
import pandas as pd
import numpy as np

BASE = Path(r"C:\Quant\data")
AN = BASE / "analytics"
RISK_DIR = BASE / "risk"
RISK_DIR.mkdir(parents=True, exist_ok=True)

PERF = AN / "performance_quant_v2.parquet"
OPT = AN / "portfolio_quant_v2.parquet"
OUT = RISK_DIR / "quant_risk_daily_v1.csv"

# Load performance
if not PERF.exists():
    raise SystemExit("performance file missing: " + str(PERF))
perf = pd.read_parquet(PERF)
perf["date"] = pd.to_datetime(perf["date"])
perf = perf.sort_values("date").reset_index(drop=True)

# Ensure port_ret exists
if "port_ret" not in perf.columns:
    raise SystemExit("performance missing port_ret column")

# Rolling vol and VaR (21-day window)
window = 21
perf["vol_21"] = perf["port_ret"].rolling(window).std() * (252**0.5)
perf["var_5pct_21"] = perf["port_ret"].rolling(window).apply(lambda x: np.percentile(x, 5) if len(x) >= window else np.nan)

# Turnover from optimizer if available
turnover = None
if OPT.exists():
    opt = pd.read_parquet(OPT)
    opt["date"] = pd.to_datetime(opt["date"])
    # compute daily turnover as sum(abs(w_t - w_{t-1})) aggregated by date
    if "weight" in opt.columns and "asset" in opt.columns:
        opt = opt.sort_values(["asset", "date"])
        pivot = opt.pivot_table(index="date", columns="asset", values="weight", aggfunc="first").fillna(0)
        turnover_series = pivot.diff().abs().sum(axis=1)
        turnover = turnover_series.rename("turnover").reset_index()
        turnover["date"] = pd.to_datetime(turnover["date"])
    else:
        turnover = None

# Build risk df
risk_df = perf[["date"]].drop_duplicates().sort_values("date").reset_index(drop=True)
risk_df = risk_df.merge(perf[["date", "vol_21", "var_5pct_21"]].drop_duplicates(subset=["date"]), on="date", how="left")
if turnover is not None:
    risk_df = risk_df.merge(turnover, on="date", how="left")
else:
    risk_df["turnover"] = pd.NA

# Save CSV
risk_df.to_csv(OUT, index=False)
print("Wrote risk file:", OUT)