r"""
Quant v1.0 — portfolio_performance_v2.py
Version: v2.0

1. Module name
- portfolio_performance_v2

2. Quant version
- Quant v1.0

3. Purpose
- Compute daily and cumulative portfolio performance using v2 weights.
- Compute turnover and cost-adjusted returns.
- Produce summary metrics for longshort and longonly:
  - annualised vol
  - annualised Sharpe (rf = 0)
  - max drawdown
  - Calmar
  - turnover
  - cost-adjusted Sharpe

4. Inputs
- C:\Quant\data\analytics\quant_portfolio_weights_ensemble_risk_longshort_v2.csv
- C:\Quant\data\analytics\quant_portfolio_weights_ensemble_risk_longonly_v2.csv
- C:\Quant\data\analytics\quant_returns_panel.csv

  Required columns:
    returns panel:
      - date
      - ticker
      - daily_return

5. Outputs
- C:\Quant\data\analytics\quant_portfolio_performance_longshort_v2.csv
- C:\Quant\data\analytics\quant_portfolio_performance_longonly_v2.csv
- C:\Quant\data\analytics\quant_portfolio_performance_summary_v2.csv

6. Governance rules
- No schema drift.
- All output columns lowercase.
- ISO-8601 dates only.
- Deterministic behaviour.

7. Dependencies
- pandas
- numpy
- logging_quant_v1

8. Provenance
- Governed component of Quant v1.0.
"""

import sys
from pathlib import Path
import numpy as np
import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(PROJECT_ROOT / "scripts"))

from logging_quant_v1 import get_logger

logger = get_logger("portfolio_performance_v2")

# Files
W_LS_V2_FILE = PROJECT_ROOT / "data" / "analytics" / "quant_portfolio_weights_ensemble_risk_longshort_v2.csv"
W_LO_V2_FILE = PROJECT_ROOT / "data" / "analytics" / "quant_portfolio_weights_ensemble_risk_longonly_v2.csv"
RET_FILE = PROJECT_ROOT / "data" / "analytics" / "quant_returns_panel.csv"

OUT_LS = PROJECT_ROOT / "data" / "analytics" / "quant_portfolio_performance_longshort_v2.csv"
OUT_LO = PROJECT_ROOT / "data" / "analytics" / "quant_portfolio_performance_longonly_v2.csv"
OUT_SUMMARY = PROJECT_ROOT / "data" / "analytics" / "quant_portfolio_performance_summary_v2.csv"

TRADING_DAYS = 252
COST_PER_TURNOVER = 0.0005  # 5 bps per 1.0 turnover


def load_inputs():
    logger.info("Loading v2 weights and returns panel.")

    w_ls = pd.read_csv(W_LS_V2_FILE)
    w_lo = pd.read_csv(W_LO_V2_FILE)
    ret = pd.read_csv(RET_FILE)

    for df in [w_ls, w_lo, ret]:
        df["date"] = pd.to_datetime(df["date"], errors="coerce")

    w_ls = w_ls.dropna(subset=["date", "ticker", "weight_longshort_v2"])
    w_lo = w_lo.dropna(subset=["date", "ticker", "weight_longonly_v2"])
    ret = ret.dropna(subset=["date", "ticker", "daily_return"])

    w_ls["weight_longshort_v2"] = pd.to_numeric(w_ls["weight_longshort_v2"], errors="coerce")
    w_lo["weight_longonly_v2"] = pd.to_numeric(w_lo["weight_longonly_v2"], errors="coerce")
    ret["daily_return"] = pd.to_numeric(ret["daily_return"], errors="coerce")

    logger.info(
        f"Loaded {len(w_ls)} LS weights, {len(w_lo)} LO weights, {len(ret)} return rows."
    )

    return w_ls, w_lo, ret


def compute_portfolio_pnl(weights: pd.DataFrame, ret: pd.DataFrame, weight_col: str):
    logger.info(f"Computing portfolio PnL for {weight_col}.")

    df = weights.merge(ret, on=["date", "ticker"], how="left")
    df = df.dropna(subset=["daily_return"])

    df["pnl"] = df[weight_col] * df["daily_return"]

    # Aggregate by date
    daily = (
        df.groupby("date")
        .agg(
            {
                "pnl": "sum",
            }
        )
        .reset_index()
    )

    daily = daily.sort_values("date").reset_index(drop=True)
    daily["cumulative_return"] = daily["pnl"].cumsum()

    return daily


def compute_turnover(weights: pd.DataFrame, weight_col: str):
    logger.info(f"Computing turnover for {weight_col}.")

    weights = weights.sort_values(["date", "ticker"]).reset_index(drop=True)

    turnover_records = []
    prev = None

    for d, g in weights.groupby("date"):
        w = g.set_index("ticker")[weight_col]

        if prev is None:
            turnover = 0.0
        else:
            all_tickers = sorted(set(prev.index) | set(w.index))
            prev_vec = prev.reindex(all_tickers).fillna(0.0).values
            w_vec = w.reindex(all_tickers).fillna(0.0).values
            turnover = np.sum(np.abs(w_vec - prev_vec))

        turnover_records.append({"date": d, "turnover": float(turnover)})
        prev = w

    return pd.DataFrame.from_records(turnover_records).sort_values("date")


def compute_summary(daily: pd.DataFrame, turnover: pd.DataFrame):
    logger.info("Computing summary metrics.")

    pnl = daily["pnl"].values
    vol = np.std(pnl, ddof=1) * np.sqrt(TRADING_DAYS)
    mean = np.mean(pnl) * TRADING_DAYS
    sharpe = mean / vol if vol > 0 else np.nan

    equity = 1 + daily["cumulative_return"]
    dd = (equity - np.maximum.accumulate(equity)) / np.maximum.accumulate(equity)
    max_dd = float(dd.min())
    calmar = mean / abs(max_dd) if max_dd < 0 else np.nan

    avg_turnover = turnover["turnover"].mean()
    cost = avg_turnover * COST_PER_TURNOVER
    cost_adj_mean = mean - cost * TRADING_DAYS
    cost_adj_sharpe = cost_adj_mean / vol if vol > 0 else np.nan

    return {
        "ann_vol": float(vol),
        "ann_sharpe": float(sharpe),
        "max_drawdown": float(max_dd),
        "calmar": float(calmar),
        "avg_turnover": float(avg_turnover),
        "cost_adj_ann_sharpe": float(cost_adj_sharpe),
    }


def main():
    logger.info("Starting portfolio_performance_v2 run (v2.0).")

    w_ls, w_lo, ret = load_inputs()

    # Long-short
    ls_daily = compute_portfolio_pnl(w_ls, ret, "weight_longshort_v2")
    ls_turn = compute_turnover(w_ls, "weight_longshort_v2")
    ls_summary = compute_summary(ls_daily, ls_turn)
    ls_daily.to_csv(OUT_LS, index=False, encoding="utf-8")

    # Long-only
    lo_daily = compute_portfolio_pnl(w_lo, ret, "weight_longonly_v2")
    lo_turn = compute_turnover(w_lo, "weight_longonly_v2")
    lo_summary = compute_summary(lo_daily, lo_turn)
    lo_daily.to_csv(OUT_LO, index=False, encoding="utf-8")

    # Summary table
    summary = pd.DataFrame.from_records(
        [
            {"portfolio_type": "longshort", **ls_summary},
            {"portfolio_type": "longonly", **lo_summary},
        ]
    )
    summary.to_csv(OUT_SUMMARY, index=False, encoding="utf-8")

    logger.info("portfolio_performance_v2 run completed successfully.")


if __name__ == "__main__":
    main()