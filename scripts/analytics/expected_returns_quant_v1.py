"""
expected_returns_quant_v1
Builds expected returns from realised returns and regime features.

Inputs:
    C:\Quant\data\analytics\price_returns.parquet
    C:\Quant\data\analytics\optimiser_regime_quant_v1.parquet

Output:
    C:\Quant\data\analytics\expected_returns_quant_v1.parquet
"""

from __future__ import annotations
import os
import pandas as pd
import numpy as np


PRICE_RETURNS_FILE = r"C:\Quant\data\analytics\price_returns.parquet"
REGIME_FEATURES_FILE = r"C:\Quant\data\analytics\optimiser_regime_quant_v1.parquet"
EXPECTED_RETURNS_FILE = r"C:\Quant\data\analytics\expected_returns_quant_v1.parquet"


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

    def get_logger(name="expected_returns_quant_v1"):
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


def _compute_expected_returns(
    rets: pd.DataFrame,
    regime: pd.DataFrame,
) -> pd.DataFrame:
    """
    rets:   price_returns.parquet
            [date, ticker, ret_close, ret_adj_close, log_ret_close, log_ret_adj_close]
    regime: optimiser_regime_quant_v1.parquet
            [date, ticker, state, vol20_log, vol_zscore, regime, ...]
    """
    df = pd.merge(
        rets[
            [
                "date",
                "ticker",
                "ret_close",
                "ret_adj_close",
                "log_ret_close",
                "log_ret_adj_close",
            ]
        ],
        regime[
            [
                "date",
                "ticker",
                "state",
                "vol20_log",
                "vol60_log",
                "vol_zscore",
                "regime",
            ]
        ],
        on=["date", "ticker"],
        how="inner",
    )

    df = df.sort_values(["ticker", "date"]).reset_index(drop=True)

    def _per_ticker(group: pd.DataFrame) -> pd.DataFrame:
        g = group.copy()

        # --- Base expected return from trailing log returns ---
        # Use a 60-day rolling mean of log returns as a simple, stable proxy.
        g["er_base"] = g["log_ret_close"].rolling(window=60, min_periods=20).mean()

        # --- Regime adjustment ---
        # Penalise volatile/stressed regimes, slightly boost calm.
        # This is deliberately simple and transparent; can be replaced by a richer model later.
        regime = g["regime"].fillna("Unknown")

        adj = np.where(
            regime == "Calm",
            1.10,  # +10% boost
            np.where(
                regime == "Normal",
                1.00,  # no change
                np.where(
                    regime == "Volatile",
                    0.75,  # 25% haircut
                    np.where(
                        regime == "Stressed",
                        0.50,  # 50% haircut
                        0.90,  # Unknown / other: mild haircut
                    ),
                ),
            ),
        )

        g["regime_adjustment"] = adj

        # Final expected return
        g["expected_return"] = g["er_base"] * g["regime_adjustment"]

        return g

    df = df.groupby("ticker", group_keys=False).apply(_per_ticker)

    out = df[
        [
            "date",
            "ticker",
            "expected_return",
            "er_base",
            "regime_adjustment",
            "regime",
            "state",
            "vol20_log",
            "vol60_log",
            "vol_zscore",
        ]
    ].copy()

    out = out.sort_values(["ticker", "date"]).reset_index(drop=True)
    return out


def build_expected_returns():
    """
    Build expected returns from realised returns and regime features.

    Output schema:
        date
        ticker
        expected_return       # regime-adjusted expected return
        er_base               # base expected return (rolling mean of log returns)
        regime_adjustment     # multiplicative adjustment factor
        regime                # Calm / Normal / Volatile / Stressed / Unknown
        state                 # Bull / Bear / Sideways / Unknown
        vol20_log
        vol60_log
        vol_zscore
    """
    logger = get_logger("expected_returns")

    logger.info("Starting expected_returns build")
    logger.info(f"Reading price returns: {PRICE_RETURNS_FILE}")
    logger.info(f"Reading regime features: {REGIME_FEATURES_FILE}")

    if not os.path.exists(PRICE_RETURNS_FILE):
        logger.error(f"Price returns missing: {PRICE_RETURNS_FILE}")
        raise FileNotFoundError(PRICE_RETURNS_FILE)

    if not os.path.exists(REGIME_FEATURES_FILE):
        logger.error(f"Regime features missing: {REGIME_FEATURES_FILE}")
        raise FileNotFoundError(REGIME_FEATURES_FILE)

    rets = pd.read_parquet(PRICE_RETURNS_FILE)
    regime = pd.read_parquet(REGIME_FEATURES_FILE)

    required_rets = {
        "date",
        "ticker",
        "ret_close",
        "ret_adj_close",
        "log_ret_close",
        "log_ret_adj_close",
    }
    required_regime = {
        "date",
        "ticker",
        "state",
        "vol20_log",
        "vol60_log",
        "vol_zscore",
        "regime",
    }

    missing_rets = required_rets.difference(rets.columns)
    missing_regime = required_regime.difference(regime.columns)

    if missing_rets:
        msg = f"Price returns missing required columns: {sorted(missing_rets)}"
        logger.error(msg)
        raise ValueError(msg)

    if missing_regime:
        msg = f"Regime features missing required columns: {sorted(missing_regime)}"
        logger.error(msg)
        raise ValueError(msg)

    rets = rets.sort_values(["ticker", "date"]).reset_index(drop=True)
    regime = regime.sort_values(["ticker", "date"]).reset_index(drop=True)

    logger.info("Computing expected returns by ticker")
    out = _compute_expected_returns(rets, regime)

    analytics_dir = os.path.dirname(EXPECTED_RETURNS_FILE)
    if analytics_dir and not os.path.exists(analytics_dir):
        os.makedirs(analytics_dir, exist_ok=True)

    logger.info(f"Writing expected returns to: {EXPECTED_RETURNS_FILE}")
    out.to_parquet(EXPECTED_RETURNS_FILE, index=False)

    logger.info("Expected returns build complete")
    print("EXPECTED_RETURNS_OK")


def main():
    logger = get_logger("expected_returns_pipeline")
    logger.info("Expected returns pipeline starting")
    build_expected_returns()
    logger.info("Expected returns pipeline completed successfully")


if __name__ == "__main__":
    main()