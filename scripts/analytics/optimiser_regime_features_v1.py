"""
optimiser_regime_quant_v1
Builds state (trend) and regime (volatility) features for optimiser inputs.

Inputs:
    C:\Quant\data\canonical\prices.parquet
    C:\Quant\data\analytics\price_returns.parquet

Output:
    C:\Quant\data\analytics\optimiser_regime_quant_v1.parquet
"""

from __future__ import annotations
import os
import pandas as pd
import numpy as np


CANONICAL_PRICES = r"C:\Quant\data\canonical\prices.parquet"
ANALYTICS_RETURNS = r"C:\Quant\data\analytics\price_returns.parquet"
ANALYTICS_OPT_REGIME = r"C:\Quant\data\analytics\optimiser_regime_quant_v1.parquet"


def _load_logging_module():
    """Reuse the canonical logging module pattern."""
    import importlib.util

    candidates = [
        r"C:\Quant\scripts\logging_quant_v1.py",
        r"C:\Quant\scripts\canonical\logging_quant_v1.py",
        r"C:\Quant\scripts\logs\logging_quant_v1.py",
    ]
    for p in candidates:
        try:
            if os.path.exists(p):
                spec = importlib.util.spec_from_file_location("logging_quant_v1", p)
                if spec and spec.loader:
                    mod = importlib.util.module_from_spec(spec)
                    spec.loader.exec_module(mod)
                    return mod.get_logger, mod.log
        except Exception:
            continue

    # Fallback logger
    import logging

    def get_logger(name="optimiser_regime_quant_v1"):
        logger = logging.getLogger(name)
        if not logger.handlers:
            h = logging.StreamHandler()
            f = logging.Formatter("%(asctime)s %(levelname)s %(name)s: %(message)s")
            h.setFormatter(f)
            logger.addHandler(h)
            logger.setLevel(logging.INFO)
        return logger

    class _Proxy:
        def info(self, *a, **k): get_logger().info(*a, **k)
        def warning(self, *a, **k): get_logger().warning(*a, **k)
        def error(self, *a, **k): get_logger().error(*a, **k)
        def debug(self, *a, **k): get_logger().debug(*a, **k)

    return get_logger, _Proxy()


get_logger, log = _load_logging_module()


def _compute_state_and_regime(prices: pd.DataFrame, rets: pd.DataFrame) -> pd.DataFrame:
    """
    prices: columns [date, ticker, close]
    rets:   columns [date, ticker, ret_close, ret_adj_close, log_ret_close, log_ret_adj_close]
    """
    # Merge on date/ticker to have everything in one frame
    df = pd.merge(
        prices[["date", "ticker", "close"]],
        rets[["date", "ticker", "ret_close", "ret_adj_close", "log_ret_close", "log_ret_adj_close"]],
        on=["date", "ticker"],
        how="inner",
    )

    df = df.sort_values(["ticker", "date"]).reset_index(drop=True)

    def _per_ticker(group: pd.DataFrame) -> pd.DataFrame:
        g = group.copy()

        # --- Trend / State (Bull / Bear / Sideways) ---
        g["sma50"] = g["close"].rolling(window=50, min_periods=1).mean()
        g["sma200"] = g["close"].rolling(window=200, min_periods=1).mean()

        # State: Bull / Bear / Sideways (within ±2% of SMA200)
        # Use last available SMA200; early rows will be noisy but consistent.
        close = g["close"]
        sma200 = g["sma200"]
        rel = (close - sma200) / sma200.replace(0, np.nan)

        state = np.where(
            sma200 <= 0,
            "Unknown",
            np.where(
                rel > 0.02,
                "Bull",
                np.where(rel < -0.02, "Bear", "Sideways"),
            ),
        )
        g["state"] = state

        # --- Volatility / Regime ---
        # Simple returns volatility
        g["vol20_simple"] = g["ret_close"].rolling(window=20, min_periods=5).std()
        g["vol60_simple"] = g["ret_close"].rolling(window=60, min_periods=10).std()

        # Log returns volatility
        g["vol20_log"] = g["log_ret_close"].rolling(window=20, min_periods=5).std()
        g["vol60_log"] = g["log_ret_close"].rolling(window=60, min_periods=10).std()

        # Drawdown based on close
        rolling_max = g["close"].cummax()
        g["rolling_max"] = rolling_max
        g["drawdown"] = (g["close"] / rolling_max) - 1.0

        # Volatility z-score (use vol20_log as primary risk proxy)
        vol = g["vol20_log"]
        mean_vol = vol.expanding(min_periods=20).mean()
        std_vol = vol.expanding(min_periods=20).std()

        g["vol_zscore"] = (vol - mean_vol) / std_vol.replace(0, np.nan)

        # Regime from z-score:
        # Calm: z < -1
        # Normal: -1 <= z <= 1
        # Volatile: 1 < z <= 2
        # Stressed: z > 2
        z = g["vol_zscore"]
        regime = np.where(
            z.isna(),
            "Unknown",
            np.where(
                z < -1.0,
                "Calm",
                np.where(
                    z <= 1.0,
                    "Normal",
                    np.where(z <= 2.0, "Volatile", "Stressed"),
                ),
            ),
        )
        g["regime"] = regime

        return g

    df = df.groupby("ticker", group_keys=False).apply(_per_ticker)

    # Final column selection
    out = df[[
        "date",
        "ticker",
        "close",
        "sma50",
        "sma200",
        "state",
        "ret_close",
        "ret_adj_close",
        "log_ret_close",
        "log_ret_adj_close",
        "vol20_simple",
        "vol60_simple",
        "vol20_log",
        "vol60_log",
        "rolling_max",
        "drawdown",
        "vol_zscore",
        "regime",
    ]].copy()

    out = out.sort_values(["ticker", "date"]).reset_index(drop=True)
    return out


def build_optimiser_regime():
    """
    Build optimiser regime analytics from canonical prices and price returns.

    Output schema (per row):
        date
        ticker
        close
        sma50
        sma200
        state              # Bull / Bear / Sideways / Unknown
        ret_close
        ret_adj_close
        log_ret_close
        log_ret_adj_close
        vol20_simple
        vol60_simple
        vol20_log
        vol60_log
        rolling_max
        drawdown
        vol_zscore
        regime            # Calm / Normal / Volatile / Stressed / Unknown
    """
    logger = get_logger("optimiser_regime")

    logger.info("Starting optimiser_regime build")
    logger.info(f"Reading canonical prices: {CANONICAL_PRICES}")
    logger.info(f"Reading price returns: {ANALYTICS_RETURNS}")

    if not os.path.exists(CANONICAL_PRICES):
        logger.error(f"Canonical prices missing: {CANONICAL_PRICES}")
        raise FileNotFoundError(CANONICAL_PRICES)

    if not os.path.exists(ANALYTICS_RETURNS):
        logger.error(f"Analytics returns missing: {ANALYTICS_RETURNS}")
        raise FileNotFoundError(ANALYTICS_RETURNS)

    prices = pd.read_parquet(CANONICAL_PRICES)
    rets = pd.read_parquet(ANALYTICS_RETURNS)

    required_prices = {"date", "ticker", "close"}
    required_rets = {"date", "ticker", "ret_close", "ret_adj_close", "log_ret_close", "log_ret_adj_close"}

    missing_prices = required_prices.difference(prices.columns)
    missing_rets = required_rets.difference(rets.columns)

    if missing_prices:
        msg = f"Canonical prices missing required columns: {sorted(missing_prices)}"
        logger.error(msg)
        raise ValueError(msg)

    if missing_rets:
        msg = f"Price returns missing required columns: {sorted(missing_rets)}"
        logger.error(msg)
        raise ValueError(msg)

    prices = prices.sort_values(["ticker", "date"]).reset_index(drop=True)
    rets = rets.sort_values(["ticker", "date"]).reset_index(drop=True)

    logger.info("Computing state and regime features by ticker")
    out = _compute_state_and_regime(prices, rets)

    analytics_dir = os.path.dirname(ANALYTICS_OPT_REGIME)
    if analytics_dir and not os.path.exists(analytics_dir):
        os.makedirs(analytics_dir, exist_ok=True)

    logger.info(f"Writing optimiser regime analytics to: {ANALYTICS_OPT_REGIME}")
    out.to_parquet(ANALYTICS_OPT_REGIME, index=False)

    logger.info("Optimiser regime build complete")
    print("OPTIMISER_REGIME_OK")


def main():
    logger = get_logger("optimiser_regime_pipeline")
    logger.info("Optimiser regime pipeline starting")
    build_optimiser_regime()
    logger.info("Optimiser regime pipeline completed successfully")


if __name__ == "__main__":
    main()