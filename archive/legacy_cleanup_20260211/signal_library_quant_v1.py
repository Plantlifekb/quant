#!/usr/bin/env python3
# ==================================================================================================
# Quant v1.0 — Signal Library (Extended Multi-Factor)
# ==================================================================================================
# INPUT COLUMNS (from quant_master.csv):
#   date, ticker, market_sector, open, high, low, close, adj_close,
#   volume, ret, log_ret, vol_5, vol_20, sma_5, sma_20,
#   ema_12, ema_26, high_low_spread, volume_zscore, gap,
#   overnight_ret, sector_rel_ret, outlier_flag, missing_flag
#
# All signals are:
#   • cross-sectionally aligned to df.index
#   • deterministic
#   • no dropping / reindexing
# ==================================================================================================

import numpy as np
import pandas as pd

# --------------------------------------------------------------------------------------------------
# CORE MOMENTUM SIGNALS (ALREADY IN USE)
# --------------------------------------------------------------------------------------------------

def signal_mom_short(df: pd.DataFrame) -> pd.Series:
    """5-day price momentum."""
    return df["close"].groupby(df["ticker"]).pct_change(5).fillna(0.0)


def signal_mom_medium(df: pd.DataFrame) -> pd.Series:
    """20-day price momentum."""
    return df["close"].groupby(df["ticker"]).pct_change(20).fillna(0.0)


def signal_mom_long(df: pd.DataFrame) -> pd.Series:
    """120-day price momentum."""
    return df["close"].groupby(df["ticker"]).pct_change(120).fillna(0.0)


def signal_cross_sectional(df: pd.DataFrame) -> pd.Series:
    """Sector-relative return (already provided)."""
    return df["sector_rel_ret"].fillna(0.0)


def signal_mean_reversion(df: pd.DataFrame) -> pd.Series:
    """2-day mean reversion on returns."""
    ret = df["ret"].fillna(0.0)
    mr = -ret.groupby(df["ticker"]).rolling(2).mean().reset_index(level=0, drop=True)
    return mr.fillna(0.0)

# --------------------------------------------------------------------------------------------------
# NEW TECHNICAL / MICROSTRUCTURE SIGNALS
# --------------------------------------------------------------------------------------------------

def signal_low_vol_252(df: pd.DataFrame) -> pd.Series:
    """
    Low-risk factor: inverse of long-term volatility proxy.
    Approximated using vol_20 smoothed over 20 days.
    """
    vol = df["vol_20"].astype(float).abs()
    vol_smoothed = vol.groupby(df["ticker"]).rolling(20).mean().reset_index(level=0, drop=True)
    vol_smoothed = vol_smoothed.replace(0, np.nan)
    inv_vol = 1.0 / vol_smoothed
    inv_vol = (inv_vol - inv_vol.mean()) / (inv_vol.std(ddof=0) + 1e-8)
    return inv_vol.fillna(0.0)


def signal_volume_persistence(df: pd.DataFrame) -> pd.Series:
    """
    Liquidity / attention proxy:
    rolling mean of volume_zscore (persistent high attention).
    """
    vz = df["volume_zscore"].fillna(0.0)
    rp = vz.groupby(df["ticker"]).rolling(10).mean().reset_index(level=0, drop=True)
    return rp.fillna(0.0)


def signal_seasonality_12m(df: pd.DataFrame) -> pd.Series:
    """
    Simple seasonality: average same-month return over history per ticker.
    """
    d = pd.to_datetime(df["date"])
    month = d.dt.month
    tmp = df.copy()
    tmp["month"] = month
    tmp["ret_clean"] = df["ret"].fillna(0.0)

    avg_by_ticker_month = (
        tmp.groupby(["ticker", "month"])["ret_clean"]
        .transform("mean")
    )
    return avg_by_ticker_month.fillna(0.0)


def signal_trend_vol_combo(df: pd.DataFrame) -> pd.Series:
    """
    Trend × volatility compression:
    - strong medium-term trend
    - low vol_20
    """
    mom = signal_mom_medium(df)
    vol = df["vol_20"].astype(float).abs()
    vol_norm = (vol - vol.mean()) / (vol.std(ddof=0) + 1e-8)
    vol_comp = -vol_norm  # higher when vol is low
    combo = mom * vol_comp
    combo = (combo - combo.mean()) / (combo.std(ddof=0) + 1e-8)
    return combo.fillna(0.0)


def signal_gap_reversal(df: pd.DataFrame) -> pd.Series:
    """
    Gap mean reversion: large overnight gaps tend to partially revert.
    """
    gap = df["gap"].fillna(0.0)
    return -gap

# --------------------------------------------------------------------------------------------------
# HYBRID MODEL SIGNALS
# --------------------------------------------------------------------------------------------------

def _zscore(series: pd.Series) -> pd.Series:
    m = series.mean()
    s = series.std(ddof=0)
    if s == 0 or np.isnan(s):
        return pd.Series(0.0, index=series.index)
    return (series - m) / (s + 1e-8)


def signal_model_composite(df: pd.DataFrame) -> pd.Series:
    """
    Existing hybrid model (v2): momentum + mean reversion + cross-sectional.
    Kept for continuity.
    """
    m_short = _zscore(signal_mom_short(df))
    m_med = _zscore(signal_mom_medium(df))
    m_long = _zscore(signal_mom_long(df))
    cs = _zscore(signal_cross_sectional(df))
    mr = _zscore(signal_mean_reversion(df))

    composite = (
        0.25 * m_short +
        0.25 * m_med +
        0.25 * m_long +
        0.15 * cs +
        0.10 * mr
    )

    comp_df = composite.to_frame("comp")
    comp_df["ticker"] = df["ticker"].values

    smoothed = (
        comp_df.groupby("ticker")["comp"]
        .apply(lambda x: x.ewm(span=10, adjust=False).mean())
    )

    # Drop groupby index level to keep flat index
    smoothed.index = smoothed.index.droplevel(0)

    return smoothed.reindex(df.index).fillna(0.0)


def signal_model_composite_v3(df: pd.DataFrame) -> pd.Series:
    """
    New extended hybrid model (v3):
    - Momentum (short/medium/long)
    - Cross-sectional momentum
    - Mean reversion
    - Low-risk (inverse vol)
    - Volume persistence (liquidity/attention)
    - Seasonality
    - Trend × volatility compression
    - Gap reversal

    All components z-scored cross-sectionally, then combined and smoothed.
    """
    m_short = _zscore(signal_mom_short(df))
    m_med = _zscore(signal_mom_medium(df))
    m_long = _zscore(signal_mom_long(df))
    cs = _zscore(signal_cross_sectional(df))
    mr = _zscore(signal_mean_reversion(df))
    low_risk = _zscore(signal_low_vol_252(df))
    vol_persist = _zscore(signal_volume_persistence(df))
    season = _zscore(signal_seasonality_12m(df))
    trend_vol = _zscore(signal_trend_vol_combo(df))
    gap_rev = _zscore(signal_gap_reversal(df))

    composite = (
        0.15 * m_short +
        0.20 * m_med +
        0.15 * m_long +
        0.10 * cs +
        0.05 * mr +
        0.10 * low_risk +
        0.05 * vol_persist +
        0.05 * season +
        0.10 * trend_vol +
        0.05 * gap_rev
    )

    comp_df = composite.to_frame("comp")
    comp_df["ticker"] = df["ticker"].values

    smoothed = (
        comp_df.groupby("ticker")["comp"]
        .apply(lambda x: x.ewm(span=10, adjust=False).mean())
    )

    # Drop groupby index level to keep flat index
    smoothed.index = smoothed.index.droplevel(0)

    return smoothed.reindex(df.index).fillna(0.0)

# --------------------------------------------------------------------------------------------------
# SIGNAL REGISTRY
# --------------------------------------------------------------------------------------------------

SIGNAL_REGISTRY = {
    "mom_short": {
        "func": signal_mom_short,
        "description": "5-day short-term momentum"
    },
    "mom_medium": {
        "func": signal_mom_medium,
        "description": "20-day medium-term momentum"
    },
    "mom_long": {
        "func": signal_mom_long,
        "description": "120-day long-term momentum"
    },
    "cross_sectional_mom": {
        "func": signal_cross_sectional,
        "description": "Sector-relative cross-sectional momentum"
    },
    "mean_reversion": {
        "func": signal_mean_reversion,
        "description": "2-day mean reversion overlay"
    },
    "low_vol_252": {
        "func": signal_low_vol_252,
        "description": "Inverse long-term volatility (low-risk factor)"
    },
    "volume_persistence": {
        "func": signal_volume_persistence,
        "description": "Rolling persistence of volume z-score (attention/liquidity)"
    },
    "seasonality_12m": {
        "func": signal_seasonality_12m,
        "description": "Same-month 12m seasonality based on historical returns"
    },
    "trend_vol_combo": {
        "func": signal_trend_vol_combo,
        "description": "Trend × volatility compression interaction"
    },
    "gap_reversal": {
        "func": signal_gap_reversal,
        "description": "Overnight gap mean reversion"
    },
    "model_signal_v2": {
        "func": signal_model_composite,
        "description": "Original hybrid multi-factor composite model"
    },
    "model_signal_v3": {
        "func": signal_model_composite_v3,
        "description": "Extended hybrid multi-factor composite model (v3)"
    }
}