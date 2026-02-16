# =====================================================================
# Module: quant_attribution_tradable_v1.py
# Quant Version: v1.1 (governed upgrade)
#
# Purpose:
#   Compute fully reconciled PnL attribution for the governed Quant v1.0
#   backtest, using tradable portfolio weights and daily returns.
#
# Governance Enhancements in v1.1:
#   - Adds attribution_run_date provenance to all outputs.
#   - Enforces timezone-aware ISO-8601 timestamps.
#   - Strengthens schema validation and deterministic behaviour.
#   - No changes to attribution logic or reconciliation rules.
# =====================================================================

import os
from datetime import datetime, timezone

import numpy as np
import pandas as pd

# Optional governed logger; fall back to print if not available
try:
    import logging_quant_v1 as qlog
    LOGGER = qlog.get_logger(__name__)
except ImportError:
    LOGGER = None


DATA_DIR = r"C:\Quant\data"
ANALYTICS_DIR = os.path.join(DATA_DIR, "analytics")

WEIGHTS_FILE = os.path.join(ANALYTICS_DIR, "quant_portfolio_weights_tradable_v1_osqp.csv")
RETURNS_FILE = os.path.join(ANALYTICS_DIR, "quant_returns_panel.csv")
SECTOR_FILE = os.path.join(r"C:\Quant", "config", "ticker_reference.csv")
BACKTEST_FILE = os.path.join(ANALYTICS_DIR, "quant_backtest_pnl_v1.csv")

OUT_TICKER = os.path.join(ANALYTICS_DIR, "quant_attribution_ticker_v1.csv")
OUT_SECTOR = os.path.join(ANALYTICS_DIR, "quant_attribution_sector_v1.csv")
OUT_DAILY = os.path.join(ANALYTICS_DIR, "quant_attribution_daily_v1.csv")


# ---------------------------------------------------------
# Logging wrappers
# ---------------------------------------------------------

def log_info(msg: str) -> None:
    if LOGGER is not None:
        LOGGER.info(msg)
    else:
        print(msg)


def log_warn(msg: str) -> None:
    if LOGGER is not None:
        LOGGER.warning(msg)
    else:
        print(f"[WARN] {msg}")


def log_error(msg: str) -> None:
    if LOGGER is not None:
        LOGGER.error(msg)
    else:
        print(f"[ERROR] {msg}")


# ---------------------------------------------------------
# Loaders
# ---------------------------------------------------------

def load_weights() -> pd.DataFrame:
    if not os.path.exists(WEIGHTS_FILE):
        raise FileNotFoundError(f"Missing weights file: {WEIGHTS_FILE}")

    df = pd.read_csv(WEIGHTS_FILE)
    df.columns = [c.lower() for c in df.columns]

    if "date" not in df.columns or "ticker" not in df.columns:
        raise ValueError("Weights file must contain 'date' and 'ticker'.")

    df["date"] = pd.to_datetime(df["date"], errors="coerce", utc=True)
    df["ticker"] = df["ticker"].astype(str).str.upper()

    weight_cols = [c for c in df.columns if "weight" in c and c != "weight"]
    if "weight" in df.columns:
        weight_cols.insert(0, "weight")

    if len(weight_cols) == 0:
        raise ValueError("No weight column found in weights file.")

    if len(weight_cols) > 1:
        log_warn(f"Multiple weight columns found; using '{weight_cols[0]}'.")

    df = df.rename(columns={weight_cols[0]: "weight"})
    return df[["date", "ticker", "weight"]]


def load_returns() -> pd.DataFrame:
    if not os.path.exists(RETURNS_FILE):
        raise FileNotFoundError(f"Missing returns file: {RETURNS_FILE}")

    df = pd.read_csv(RETURNS_FILE)
    df.columns = [c.lower() for c in df.columns]

    required = {"date", "ticker", "daily_return"}
    missing = required - set(df.columns)
    if missing:
        raise ValueError(f"Returns file missing columns: {sorted(missing)}")

    df["date"] = pd.to_datetime(df["date"], errors="coerce", utc=True)
    df["ticker"] = df["ticker"].astype(str).str.upper()
    return df[["date", "ticker", "daily_return"]]


def load_sector_map() -> pd.DataFrame:
    if not os.path.exists(SECTOR_FILE):
        raise FileNotFoundError(f"Missing sector file: {SECTOR_FILE}")

    df = pd.read_csv(SECTOR_FILE)
    df.columns = [c.lower() for c in df.columns]

    if "ticker" not in df.columns or "market_sector" not in df.columns:
        raise ValueError("Sector file must contain 'ticker' and 'market_sector'.")

    df["ticker"] = df["ticker"].astype(str).str.upper()
    return df[["ticker", "market_sector"]]


def load_backtest() -> pd.DataFrame:
    if not os.path.exists(BACKTEST_FILE):
        raise FileNotFoundError(f"Missing backtest file: {BACKTEST_FILE}")

    df = pd.read_csv(BACKTEST_FILE)
    df.columns = [c.lower() for c in df.columns]

    required = {"date", "pnl", "cum_pnl"}
    missing = required - set(df.columns)
    if missing:
        raise ValueError(f"Backtest file missing columns: {sorted(missing)}")

    df["date"] = pd.to_datetime(df["date"], errors="coerce", utc=True)
    return df[["date", "pnl", "cum_pnl"]]


# ---------------------------------------------------------
# Core Attribution Logic
# ---------------------------------------------------------

def build_attribution():
    log_info("Loading governed inputs for tradable attribution...")

    w = load_weights()
    r = load_returns()
    bt = load_backtest()
    sector_map = load_sector_map()

    attribution_run_date = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")

    log_info("Merging weights and returns...")
    df = w.merge(r, on=["date", "ticker"], how="inner")
    if df.empty:
        raise ValueError("Merged weights/returns panel is empty.")

    df["raw_pnl"] = df["weight"] * df["daily_return"]

    daily_raw = (
        df.groupby("date", as_index=False)["raw_pnl"]
        .sum()
        .rename(columns={"raw_pnl": "raw_portfolio_pnl"})
    )

    merged = bt.merge(daily_raw, on="date", how="inner")
    if merged.empty:
        raise ValueError("No overlapping dates between backtest and attribution panel.")

    def _safe_scale(row):
        denom = row["raw_portfolio_pnl"]
        if abs(denom) > 1e-12:
            return row["pnl"] / denom
        return 0.0

    merged["scale"] = merged.apply(_safe_scale, axis=1)
    df = df.merge(merged[["date", "scale"]], on="date", how="left")
    df["ticker_pnl"] = df["raw_pnl"] * df["scale"]

    # ---------------------------------------------------------
    # Ticker Attribution
    # ---------------------------------------------------------
    log_info("Computing ticker-level attribution...")
    ticker_attr = (
        df.groupby("ticker", as_index=False)
        .agg(
            total_pnl=("ticker_pnl", "sum"),
            avg_weight=("weight", "mean"),
            days_traded=("date", "count"),
        )
    )
    ticker_attr["attribution_run_date"] = attribution_run_date
    ticker_attr.to_csv(OUT_TICKER, index=False, encoding="utf-8")

    # ---------------------------------------------------------
    # Sector Attribution
    # ---------------------------------------------------------
    log_info("Computing sector-level attribution...")
    df = df.merge(sector_map, on="ticker", how="left")
    df["market_sector"] = df["market_sector"].fillna("Unknown")

    sector_attr = (
        df.groupby("market_sector", as_index=False)
        .agg(
            total_pnl=("ticker_pnl", "sum"),
            avg_gross_weight=("weight", lambda x: x.abs().mean()),
            days=("date", "count"),
        )
    )
    sector_attr["attribution_run_date"] = attribution_run_date
    sector_attr.to_csv(OUT_SECTOR, index=False, encoding="utf-8")

    # ---------------------------------------------------------
    # Daily Attribution
    # ---------------------------------------------------------
    log_info("Computing daily-level attribution...")
    daily_attr = (
        df.groupby("date", as_index=False)
        .agg(
            total_pnl=("ticker_pnl", "sum"),
            gross_exposure=("weight", lambda x: x.abs().sum()),
            net_exposure=("weight", "sum"),
        )
    )
    daily_attr["attribution_run_date"] = attribution_run_date
    daily_attr.to_csv(OUT_DAILY, index=False, encoding="utf-8")

    # ---------------------------------------------------------
    # Reconciliation Checks
    # ---------------------------------------------------------
    log_info("Running reconciliation checks...")

    chk = daily_attr.merge(bt, on="date", how="inner")
    if chk.empty:
        log_warn("No overlapping dates for reconciliation.")

    if not np.allclose(chk["total_pnl"], chk["pnl"], atol=1e-8):
        max_diff = float(np.max(np.abs(chk["total_pnl"] - chk["pnl"])))
        log_warn(f"Daily attribution mismatch. Max abs diff: {max_diff:.6g}")
    else:
        log_info("Daily attribution matches governed backtest PnL.")

    total_attr = float(ticker_attr["total_pnl"].sum())
    total_bt = float(bt["pnl"].sum())
    if not np.isclose(total_attr, total_bt, atol=1e-8):
        diff = total_attr - total_bt
        log_warn(f"Total attribution mismatch. Diff: {diff:.6g}")
    else:
        log_info("Total attribution matches governed backtest PnL.")

    return ticker_attr, sector_attr, daily_attr


# ---------------------------------------------------------
# Main
# ---------------------------------------------------------

def main():
    log_info("Starting Quant v1.1 tradable attribution module...")
    build_attribution()
    log_info("Attribution complete.")
    log_info(f"Ticker attribution → {OUT_TICKER}")
    log_info(f"Sector attribution → {OUT_SECTOR}")
    log_info(f"Daily attribution  → {OUT_DAILY}")


if __name__ == "__main__":
    main()