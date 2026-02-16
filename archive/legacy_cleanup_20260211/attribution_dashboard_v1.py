r"""
attribution_dashboard_v1.py
Quant v1.0 — Attribution Dashboard

Builds a simple, multi-tab attribution dashboard from the governed
attribution outputs in C:\Quant\data\analytics.
"""

import pandas as pd
from pathlib import Path

from logging_attribution_suite_v1 import get_logger

logger = get_logger("attribution_dashboard_v1")

# ---------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------

PROJECT_ROOT = Path(__file__).resolve().parents[3]
DATA_DIR = PROJECT_ROOT / "data" / "analytics"

DAILY_ATTR_FILE = DATA_DIR / "quant_attribution_daily_v1.csv"
SECTOR_ATTR_FILE = DATA_DIR / "quant_attribution_sector_v1.csv"
REGIME_ATTR_FILE = DATA_DIR / "quant_attribution_regime_v1.csv"
TICKER_ATTR_FILE = DATA_DIR / "quant_attribution_ticker_v1.csv"

ROLLING_ATTR_FILE = DATA_DIR / "quant_attribution_rolling_v1.csv"
LIQ_COSTS_FILE = DATA_DIR / "quant_liquidity_costs_timeseries.csv"
TURNOVER_FILE = DATA_DIR / "quant_turnover_timeseries_ensemble_risk.csv"

DASHBOARD_OUT_FILE = DATA_DIR / "quant_attribution_dashboard_v1.xlsx"


# ---------------------------------------------------------------------
# Loaders
# ---------------------------------------------------------------------

def _safe_read_csv(path: Path, label: str) -> pd.DataFrame:
    if not path.exists():
        logger.warning("%s not found at %s — returning empty DataFrame.", label, path)
        return pd.DataFrame()
    logger.info("Loading %s from %s", label, path)
    df = pd.read_csv(path)
    df.columns = [c.lower() for c in df.columns]
    return df


def load_daily_attribution() -> pd.DataFrame:
    return _safe_read_csv(DAILY_ATTR_FILE, "daily attribution")


def load_sector_attribution() -> pd.DataFrame:
    return _safe_read_csv(SECTOR_ATTR_FILE, "sector attribution")


def load_regime_attribution() -> pd.DataFrame:
    return _safe_read_csv(REGIME_ATTR_FILE, "regime attribution")


def load_ticker_attribution() -> pd.DataFrame:
    return _safe_read_csv(TICKER_ATTR_FILE, "ticker attribution")


def load_rolling_attribution() -> pd.DataFrame:
    return _safe_read_csv(ROLLING_ATTR_FILE, "rolling attribution")


def load_liquidity_costs() -> pd.DataFrame:
    return _safe_read_csv(LIQ_COSTS_FILE, "liquidity costs timeseries")


def load_turnover() -> pd.DataFrame:
    return _safe_read_csv(TURNOVER_FILE, "turnover timeseries")


# ---------------------------------------------------------------------
# Dashboard builder
# ---------------------------------------------------------------------

def build_dashboard():
    logger.info("Building attribution dashboard from governed analytics outputs.")

    daily = load_daily_attribution()
    sector = load_sector_attribution()
    regime = load_regime_attribution()
    ticker = load_ticker_attribution()
    rolling = load_rolling_attribution()
    liq = load_liquidity_costs()
    to = load_turnover()

    # Basic sanity: ensure date columns are datetime where present
    for df, name in [
        (daily, "daily"),
        (sector, "sector"),
        (regime, "regime"),
        (ticker, "ticker"),
        (rolling, "rolling"),
        (liq, "liquidity"),
        (to, "turnover"),
    ]:
        if not df.empty and "date" in df.columns:
            logger.info("Parsing date column for %s attribution frame.", name)
            df["date"] = pd.to_datetime(df["date"], errors="coerce").dt.tz_localize(None)

    return {
        "daily": daily,
        "sector": sector,
        "regime": regime,
        "ticker": ticker,
        "rolling": rolling,
        "liquidity_costs": liq,
        "turnover": to,
    }


# ---------------------------------------------------------------------
# Save dashboard
# ---------------------------------------------------------------------

def save_dashboard(frames: dict):
    logger.info("Saving attribution dashboard to %s", DASHBOARD_OUT_FILE)

    with pd.ExcelWriter(DASHBOARD_OUT_FILE, engine="xlsxwriter") as writer:
        for sheet_name, df in frames.items():
            safe_name = sheet_name[:31]  # Excel sheet name limit
            if df.empty:
                logger.warning("Sheet '%s' is empty — writing header-only sheet.", safe_name)
                empty_df = pd.DataFrame({"info": [f"No data available for {sheet_name}."]})
                empty_df.to_excel(writer, sheet_name=safe_name, index=False)
            else:
                df.to_excel(writer, sheet_name=safe_name, index=False)

    logger.info("Attribution dashboard written successfully.")


# ---------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------

def main():
    logger.info("Starting attribution_dashboard_v1 run.")
    frames = build_dashboard()
    save_dashboard(frames)
    logger.info("attribution_dashboard_v1 completed successfully.")


if __name__ == "__main__":
    main()