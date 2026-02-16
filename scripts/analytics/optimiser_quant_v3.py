"""
optimiser_quant_v3
Parquet-driven optimiser using expected returns and regime features.

Inputs:
    C:\Quant\data\analytics\expected_returns_quant_v1.parquet
    C:\Quant\data\analytics\optimiser_regime_quant_v1.parquet

Output:
    C:\Quant\data\analytics\portfolio_optimised_quant_v3.parquet
"""

from __future__ import annotations
import os
import pandas as pd
import numpy as np


EXPECTED_RETURNS_FILE = r"C:\Quant\data\analytics\expected_returns_quant_v1.parquet"
REGIME_FEATURES_FILE = r"C:\Quant\data\analytics\optimiser_regime_quant_v1.parquet"
PORTFOLIO_OUTPUT_FILE = r"C:\Quant\data\analytics\portfolio_optimised_quant_v3.parquet"


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

    def get_logger(name="optimiser_quant_v3"):
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


def _build_portfolio_for_date(df: pd.DataFrame) -> pd.DataFrame:
    """
    df: rows for a single date, columns include:
        ticker, expected_return, vol20_log, regime, state
    Returns a DataFrame with columns:
        date, ticker, weight, expected_return, vol20_log, regime, state
    """
    g = df.copy()

    # Drop rows with missing expected_return
    g = g.replace([np.inf, -np.inf], np.nan)
    g = g.dropna(subset=["expected_return"])

    if g.empty:
        return pd.DataFrame(columns=[
            "date", "ticker", "weight",
            "expected_return", "vol20_log", "regime", "state"
        ])

    # Basic risk proxy: use vol20_log; if missing, fallback to cross-sectional median
    vol = g["vol20_log"].copy()
    if vol.isna().all():
        vol = pd.Series(1.0, index=g.index)
    else:
        median_vol = vol.median()
        vol = vol.fillna(median_vol)
        vol = vol.replace(0.0, median_vol if median_vol > 0 else 1.0)

    # Regime-aware scaling: downweight in Volatile/Stressed regimes
    regime = g["regime"].fillna("Unknown")
    regime_scale = np.where(
        regime == "Calm",
        1.0,
        np.where(
            regime == "Normal",
            1.0,
            np.where(
                regime == "Volatile",
                0.7,
                np.where(
                    regime == "Stressed",
                    0.4,
                    0.8,  # Unknown / other
                ),
            ),
        ),
    )

    # Simple long-only, risk-aware score: ER / vol * regime_scale
    score = g["expected_return"] / vol
    score = score * regime_scale

    # Remove negative or zero scores for long-only portfolio
    score = score.where(score > 0.0, 0.0)

    if (score > 0).sum() == 0:
        # No positive opportunities: return zero weights
        g["weight"] = 0.0
    else:
        # Normalise to sum to 1
        total = score.sum()
        weights = score / total
        g["weight"] = weights

    out = g[[
        "date",
        "ticker",
        "weight",
        "expected_return",
        "vol20_log",
        "regime",
        "state",
    ]].copy()

    return out


def build_optimised_portfolio():
    """
    Build a parquet-driven optimised portfolio using expected returns and regime features.

    Output schema:
        date
        ticker
        weight
        expected_return
        vol20_log
        regime
        state
    """
    logger = get_logger("optimiser_quant_v3")

    logger.info("Starting optimiser_quant_v3 build")
    logger.info(f"Reading expected returns: {EXPECTED_RETURNS_FILE}")
    logger.info(f"Reading regime features: {REGIME_FEATURES_FILE}")

    if not os.path.exists(EXPECTED_RETURNS_FILE):
        logger.error(f"Expected returns missing: {EXPECTED_RETURNS_FILE}")
        raise FileNotFoundError(EXPECTED_RETURNS_FILE)

    if not os.path.exists(REGIME_FEATURES_FILE):
        logger.error(f"Regime features missing: {REGIME_FEATURES_FILE}")
        raise FileNotFoundError(REGIME_FEATURES_FILE)

    er = pd.read_parquet(EXPECTED_RETURNS_FILE)
    regime = pd.read_parquet(REGIME_FEATURES_FILE)

    required_er = {"date", "ticker", "expected_return"}
    required_regime = {"date", "ticker", "vol20_log", "regime", "state"}

    missing_er = required_er.difference(er.columns)
    missing_regime = required_regime.difference(regime.columns)

    if missing_er:
        msg = f"Expected returns missing required columns: {sorted(missing_er)}"
        logger.error(msg)
        raise ValueError(msg)

    if missing_regime:
        msg = f"Regime features missing required columns: {sorted(missing_regime)}"
        logger.error(msg)
        raise ValueError(msg)

    er = er.sort_values(["ticker", "date"]).reset_index(drop=True)
    regime = regime.sort_values(["ticker", "date"]).reset_index(drop=True)

    logger.info("Merging expected returns and regime features")
    df = pd.merge(
        er[["date", "ticker", "expected_return"]],
        regime[["date", "ticker", "vol20_log", "regime", "state"]],
        on=["date", "ticker"],
        how="inner",
    )

    df = df.sort_values(["date", "ticker"]).reset_index(drop=True)

    logger.info("Building optimised portfolio by date")
    portfolios = []
    for date, group in df.groupby("date", sort=True):
        logger.info(f"Optimising portfolio for {date}")
        p = _build_portfolio_for_date(group)
        portfolios.append(p)

    if portfolios:
        out = pd.concat(portfolios, axis=0, ignore_index=True)
    else:
        out = pd.DataFrame(columns=[
            "date",
            "ticker",
            "weight",
            "expected_return",
            "vol20_log",
            "regime",
            "state",
        ])

    out = out.sort_values(["date", "ticker"]).reset_index(drop=True)

    analytics_dir = os.path.dirname(PORTFOLIO_OUTPUT_FILE)
    if analytics_dir and not os.path.exists(analytics_dir):
        os.makedirs(analytics_dir, exist_ok=True)

    logger.info(f"Writing optimised portfolio to: {PORTFOLIO_OUTPUT_FILE}")
    out.to_parquet(PORTFOLIO_OUTPUT_FILE, index=False)

    logger.info("Optimiser_quant_v3 build complete")
    print("OPTIMISER_QUANT_V3_OK")


def main():
    logger = get_logger("optimiser_quant_v3_pipeline")
    logger.info("Optimiser_quant_v3 pipeline starting")
    build_optimised_portfolio()
    logger.info("Optimiser_quant_v3 pipeline completed successfully")


if __name__ == "__main__":
    main()