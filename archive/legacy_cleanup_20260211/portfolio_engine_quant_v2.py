r"""
Quant v1.0 — portfolio_engine_quant_v2.py
Version: v2.0

1. Module name
- portfolio_engine_quant_v2

2. Quant version
- Quant v1.0

3. Purpose
- Take signal-driven v2 long-short weights and transform them into
  trade-ready portfolio weights with:
  - position limits
  - minimum tradable weight
  - turnover / Δweight filter
  - volatility targeting to a higher target vol

4. Inputs
- C:\Quant\data\analytics\quant_portfolio_weights_ensemble_risk_longshort_v2.csv
- C:\Quant\data\analytics\quant_returns_panel.csv

5. Outputs
- C:\Quant\data\analytics\quant_portfolio_weights_ensemble_risk_longshort_v2_trading.csv

  Columns:
    - date
    - ticker
    - weight_trading_v2

- C:\Quant\data\portfolio\quant_portfolio_raw_targets_v1.csv

  Columns:
    - date
    - ticker
    - raw_weight
    - raw_turnover

6. Governance rules
- No schema drift.
- All output columns lowercase.
- ISO-8601 dates only.
- Deterministic behaviour.

7. Dependencies
- pandas
- numpy
- logging_quant_v1

8. Key parameters
- TARGET_VOL = 0.12 (12% annualised)
- LOOKBACK_DAYS = 60 (for realised vol)
- MAX_ABS_WEIGHT = 0.03 (3% per name cap)
- MIN_ABS_WEIGHT = 0.0005 (5 bps minimum tradable size)
- TURNOVER_THRESHOLD = 0.002 (20 bps Δweight filter)
- MAX_GROSS_LEVERAGE = 3.0

9. Provenance
- Governed component of Quant v1.0.
"""

import sys
from pathlib import Path
import numpy as np
import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(PROJECT_ROOT / "scripts"))

from logging_quant_v1 import get_logger

logger = get_logger("portfolio_engine_quant_v2")

W_LS_V2_FILE = PROJECT_ROOT / "data" / "analytics" / "quant_portfolio_weights_ensemble_risk_longshort_v2.csv"
RET_PANEL_FILE = PROJECT_ROOT / "data" / "analytics" / "quant_returns_panel.csv"
OUT_TRADING_FILE = PROJECT_ROOT / "data" / "analytics" / "quant_portfolio_weights_ensemble_risk_longshort_v2_trading.csv"

# New: raw targets for regime overlay
RAW_TARGETS_FILE = PROJECT_ROOT / "data" / "portfolio" / "quant_portfolio_raw_targets_v1.csv"

TRADING_DAYS = 252
TARGET_VOL = 0.12
LOOKBACK_DAYS = 60

MAX_ABS_WEIGHT = 0.03
MIN_ABS_WEIGHT = 0.0005
TURNOVER_THRESHOLD = 0.002
MAX_GROSS_LEVERAGE = 3.0


def load_inputs():
    logger.info("Loading v2 long-short weights and returns panel.")

    w = pd.read_csv(W_LS_V2_FILE)
    r = pd.read_csv(RET_PANEL_FILE)

    w["date"] = pd.to_datetime(w["date"], errors="coerce")
    r["date"] = pd.to_datetime(r["date"], errors="coerce")

    w = w.dropna(subset=["date", "ticker", "weight_longshort_v2"])
    r = r.dropna(subset=["date", "ticker", "daily_return"])

    w["weight_longshort_v2"] = pd.to_numeric(w["weight_longshort_v2"], errors="coerce")
    r["daily_return"] = pd.to_numeric(r["daily_return"], errors="coerce")

    w = w.dropna(subset=["weight_longshort_v2"])
    r = r.dropna(subset=["daily_return"])

    w = w.sort_values(["date", "ticker"]).reset_index(drop=True)
    r = r.sort_values(["date", "ticker"]).reset_index(drop=True)

    logger.info("Loaded %d weight rows and %d return rows.", len(w), len(r))
    return w, r


def compute_base_pnl_by_date(weights: pd.DataFrame, returns: pd.DataFrame) -> pd.DataFrame:
    logger.info("Computing base portfolio PnL for realised vol estimation.")

    df = weights.merge(returns, on=["date", "ticker"], how="left")
    df = df.dropna(subset=["daily_return"])

    df["pnl"] = df["weight_longshort_v2"] * df["daily_return"]

    daily = (
        df.groupby("date")
        .agg({"pnl": "sum"})
        .reset_index()
        .sort_values("date")
        .reset_index(drop=True)
    )

    return daily


def compute_vol_target_scalers(daily_pnl: pd.DataFrame) -> pd.DataFrame:
    logger.info("Computing volatility targeting scalers.")

    daily_pnl = daily_pnl.sort_values("date").reset_index(drop=True)
    daily_pnl["realised_vol"] = (
        daily_pnl["pnl"]
        .rolling(LOOKBACK_DAYS)
        .std(ddof=1)
        * np.sqrt(TRADING_DAYS)
    )

    def _scale(row):
        vol = row["realised_vol"]
        if pd.isna(vol) or vol <= 0:
            return 1.0
        scale = TARGET_VOL / vol
        return float(scale)

    daily_pnl["vol_scale"] = daily_pnl.apply(_scale, axis=1)
    return daily_pnl[["date", "vol_scale"]]


def apply_position_limits(weights: pd.Series) -> pd.Series:
    w = weights.copy()
    w = w.clip(lower=-MAX_ABS_WEIGHT, upper=MAX_ABS_WEIGHT)
    return w


def apply_min_weight_filter(weights: pd.Series) -> pd.Series:
    w = weights.copy()
    mask_small = w.abs() < MIN_ABS_WEIGHT
    w[mask_small] = 0.0
    return w


def apply_turnover_filter(
    prev_weights: pd.Series | None, new_weights: pd.Series
) -> pd.Series:
    if prev_weights is None:
        return new_weights

    all_tickers = sorted(set(prev_weights.index) | set(new_weights.index))
    prev_vec = prev_weights.reindex(all_tickers).fillna(0.0)
    new_vec = new_weights.reindex(all_tickers).fillna(0.0)

    delta = new_vec - prev_vec
    mask_small = delta.abs() < TURNOVER_THRESHOLD

    adjusted = prev_vec.copy()
    adjusted[~mask_small] = new_vec[~mask_small]

    return adjusted


def enforce_gross_leverage(weights: pd.Series) -> pd.Series:
    gross = weights.abs().sum()
    if gross <= MAX_GROSS_LEVERAGE or gross <= 0:
        return weights
    scale = MAX_GROSS_LEVERAGE / gross
    return weights * scale


def build_trading_weights(
    w: pd.DataFrame,
    returns: pd.DataFrame,
) -> pd.DataFrame:
    logger.info("Building trade-ready v2 long-short weights.")

    base_pnl = compute_base_pnl_by_date(w, returns)
    scalers = compute_vol_target_scalers(base_pnl)

    w = w.merge(scalers, on="date", how="left")
    w["vol_scale"] = w["vol_scale"].fillna(1.0)

    records = []
    prev_weights: pd.Series | None = None

    for d, g in w.groupby("date"):
        base = g.set_index("ticker")["weight_longshort_v2"]

        # 1) Vol targeting
        scale = g["vol_scale"].iloc[0]
        w_scaled = base * scale

        # 2) Position limits
        w_limited = apply_position_limits(w_scaled)

        # 3) Minimum tradable weight
        w_min = apply_min_weight_filter(w_limited)

        # 4) Turnover / Δweight filter
        w_turn = apply_turnover_filter(prev_weights, w_min)

        # 5) Enforce gross leverage cap
        w_final = enforce_gross_leverage(w_turn)

        for t, wt in w_final.items():
            records.append(
                {
                    "date": d,
                    "ticker": t,
                    "weight_trading_v2": float(wt),
                }
            )

        prev_weights = w_final

    df_out = pd.DataFrame.from_records(records)
    df_out = df_out.sort_values(["date", "ticker"]).reset_index(drop=True)
    logger.info("Built %d trade-ready weight rows.", len(df_out))
    return df_out


def save_trading_weights(df: pd.DataFrame) -> None:
    logger.info("Saving trade-ready weights to %s", OUT_TRADING_FILE)
    df.to_csv(OUT_TRADING_FILE, index=False, encoding="utf-8")


def save_raw_targets_for_regime_overlay(df: pd.DataFrame) -> None:
    """
    Export raw portfolio targets for the regime overlay.

    Uses:
    - raw_weight = weight_trading_v2
    - raw_turnover = 0.0 (placeholder until explicit turnover is wired in)
    """
    logger.info("Exporting raw portfolio targets for regime overlay to %s", RAW_TARGETS_FILE)

    raw = df.copy()
    raw = raw.rename(columns={"weight_trading_v2": "raw_weight"})
    raw["raw_turnover"] = 0.0

    raw = raw[["date", "ticker", "raw_weight", "raw_turnover"]]

    RAW_TARGETS_FILE.parent.mkdir(parents=True, exist_ok=True)
    raw.to_csv(RAW_TARGETS_FILE, index=False, encoding="utf-8")


def main():
    logger.info("Starting portfolio_engine_quant_v2 run (v2.0).")

    w, r = load_inputs()
    trading_w = build_trading_weights(w, r)
    save_trading_weights(trading_w)
    save_raw_targets_for_regime_overlay(trading_w)

    logger.info("portfolio_engine_quant_v2 run completed successfully.")


if __name__ == "__main__":
    main()