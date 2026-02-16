r"""
Quant v1.0 â€” optimiser_quant_v1.py
Version: v1.0

1. Module name
- optimiser_quant_v1

2. Quant version
- Quant v1.0

3. Purpose
- Apply a governed, volatility-targeting overlay to risk-neutral ensemble portfolios:
  - longshort
  - longonly
- For each portfolio_type:
  - Use rolling realised volatility to compute a daily leverage scalar
  - Target a fixed annualised volatility
  - Enforce a leverage cap
  - Produce volatility-targeted returns and cumulative returns

4. Inputs
- C:\Quant\data\analytics\quant_risk_timeseries_ensemble_risk.csv

  Required columns:
    - date
    - portfolio_type   ("longshort" / "longonly")
    - daily_return
    - rolling_vol_63

5. Outputs
- C:\Quant\data\analytics\quant_optimised_performance_ensemble_risk.csv

  Columns:
    - date
    - portfolio_type
    - daily_return
    - rolling_vol_63
    - target_ann_vol
    - leverage_cap
    - applied_leverage
    - optimised_daily_return
    - optimised_cumulative_return

6. Governance rules
- No schema drift.
- No silent changes.
- All output columns lowercase.
- ISO-8601 dates only.
- Deterministic, reproducible behaviour.
- No writing outside governed directories.

7. Logging rules
- Uses logging_quant_v1.py
- Logs start, end, and key events.
- Logs errors narratably.

8. Encoding rules
- All CSV outputs UTF-8.

9. Dependencies
- pandas
- numpy
- logging_quant_v1

10. Provenance
- This module is a governed component of Quant v1.0.
- Any modification requires version bump and architecture update.
"""

import sys
from pathlib import Path
import numpy as np
import pandas as pd

# Project root
PROJECT_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(PROJECT_ROOT / "scripts"))

from logging_quant_v1 import get_logger

logger = get_logger("optimiser_quant_v1")

# Files
RISK_TS_FILE = PROJECT_ROOT / "data" / "analytics" / "quant_risk_timeseries_ensemble_risk.csv"
OPT_OUTPUT_FILE = PROJECT_ROOT / "data" / "analytics" / "quant_optimised_performance_ensemble_risk.csv"

# Assumptions
TRADING_DAYS_PER_YEAR = 252

# Volatility targets (annualised) and leverage caps per portfolio_type
TARGETS = {
    "longonly": {
        "target_ann_vol": 0.15,   # 15% annual vol target
        "leverage_cap": 2.0,      # max 2x
    },
    "longshort": {
        "target_ann_vol": 0.10,   # 10% annual vol target
        "leverage_cap": 3.0,      # max 3x
    },
}


def load_risk_timeseries() -> pd.DataFrame:
    logger.info(f"Loading risk time series from {RISK_TS_FILE}")
    df = pd.read_csv(RISK_TS_FILE)

    required = {"date", "portfolio_type", "daily_return", "rolling_vol_63"}
    missing = required - set(df.columns)
    if missing:
        msg = f"Missing required columns in risk time series file: {missing}"
        logger.error(msg)
        raise ValueError(msg)

    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df = df.dropna(subset=["date", "portfolio_type", "daily_return", "rolling_vol_63"])

    df["daily_return"] = pd.to_numeric(df["daily_return"], errors="coerce")
    df["rolling_vol_63"] = pd.to_numeric(df["rolling_vol_63"], errors="coerce")
    df = df.dropna(subset=["daily_return", "rolling_vol_63"])

    df = df.sort_values(["portfolio_type", "date"]).reset_index(drop=True)

    logger.info(f"Loaded {len(df)} risk time series rows after cleaning.")
    return df[["date", "portfolio_type", "daily_return", "rolling_vol_63"]]


def apply_vol_targeting(df: pd.DataFrame) -> pd.DataFrame:
    logger.info("Applying volatility targeting optimiser to risk-neutral portfolios.")

    records = []

    for ptype, group in df.groupby("portfolio_type"):
        if ptype not in TARGETS:
            logger.warning(f"Unknown portfolio_type '{ptype}' in TARGETS; skipping.")
            continue

        params = TARGETS[ptype]
        target_ann_vol = params["target_ann_vol"]
        leverage_cap = params["leverage_cap"]

        g = group.sort_values("date").reset_index(drop=True).copy()

        # Convert rolling daily vol to annualised
        rolling_daily_vol = g["rolling_vol_63"].values
        rolling_ann_vol = rolling_daily_vol * np.sqrt(TRADING_DAYS_PER_YEAR)

        # Leverage = target_vol / realised_vol, capped
        with np.errstate(divide="ignore", invalid="ignore"):
            raw_leverage = target_ann_vol / rolling_ann_vol
        raw_leverage = np.where(np.isfinite(raw_leverage), raw_leverage, 0.0)
        applied_leverage = np.clip(raw_leverage, 0.0, leverage_cap)

        optimised_daily_return = applied_leverage * g["daily_return"].values
        optimised_cum = np.cumsum(optimised_daily_return)

        for i in range(len(g)):
            records.append(
                {
                    "date": g.loc[i, "date"],
                    "portfolio_type": ptype,
                    "daily_return": float(g.loc[i, "daily_return"]),
                    "rolling_vol_63": float(g.loc[i, "rolling_vol_63"]),
                    "target_ann_vol": float(target_ann_vol),
                    "leverage_cap": float(leverage_cap),
                    "applied_leverage": float(applied_leverage[i]),
                    "optimised_daily_return": float(optimised_daily_return[i]),
                    "optimised_cumulative_return": float(optimised_cum[i]),
                }
            )

    out = pd.DataFrame.from_records(records)
    if out.empty:
        logger.warning("No optimised records generated; check input data and TARGETS.")
    else:
        out = out.sort_values(["portfolio_type", "date"]).reset_index(drop=True)

    logger.info("Volatility targeting optimiser applied.")
    return out


def save_outputs(opt: pd.DataFrame) -> None:
    logger.info(f"Saving optimised performance to {OPT_OUTPUT_FILE}")
    opt.to_csv(OPT_OUTPUT_FILE, index=False, encoding="utf-8")


def main():
    logger.info("Starting optimiser_quant_v1 run (v1.0).")

    ts = load_risk_timeseries()
    opt = apply_vol_targeting(ts)
    save_outputs(opt)

    logger.info("optimiser_quant_v1 run completed successfully.")


if __name__ == "__main__":
    main()
