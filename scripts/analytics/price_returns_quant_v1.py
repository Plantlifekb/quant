"""
price_returns_quant_v1
Builds daily return series from canonical prices.

Input:
    C:\Quant\data\canonical\prices.parquet

Output:
    C:\Quant\data\analytics\price_returns.parquet
"""

from __future__ import annotations
import os
import pandas as pd
import numpy as np


CANONICAL_PRICES = r"C:\Quant\data\canonical\prices.parquet"
ANALYTICS_RETURNS = r"C:\Quant\data\analytics\price_returns.parquet"


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

    def get_logger(name="price_returns_quant_v1"):
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


def build_price_returns():
    """
    Build daily returns from canonical prices.

    Output schema:
        date
        ticker
        ret_close
        ret_adj_close
        log_ret_close
        log_ret_adj_close
    """
    logger = get_logger("price_returns")

    logger.info("Starting price returns build")
    logger.info(f"Reading canonical prices: {CANONICAL_PRICES}")

    if not os.path.exists(CANONICAL_PRICES):
        logger.error(f"Canonical prices missing: {CANONICAL_PRICES}")
        raise FileNotFoundError(CANONICAL_PRICES)

    df = pd.read_parquet(CANONICAL_PRICES)

    required_cols = {"date", "ticker", "close", "adj_close"}
    missing = required_cols.difference(df.columns)
    if missing:
        msg = f"Canonical prices missing required columns: {sorted(missing)}"
        logger.error(msg)
        raise ValueError(msg)

    # Ensure correct ordering
    df = df.sort_values(["ticker", "date"]).reset_index(drop=True)

    # Group by ticker to compute returns
    def _returns(group: pd.DataFrame) -> pd.DataFrame:
        g = group.copy()

        # Shifted prices
        close_prev = g["close"].shift(1)
        adj_close_prev = g["adj_close"].shift(1)

        # Simple returns
        g["ret_close"] = (g["close"] / close_prev) - 1.0
        g["ret_adj_close"] = (g["adj_close"] / adj_close_prev) - 1.0

        # Log returns (safe: only where previous price > 0)
        g["log_ret_close"] = np.where(
            close_prev > 0,
            np.log(g["close"] / close_prev),
            np.nan
        )

        g["log_ret_adj_close"] = np.where(
            adj_close_prev > 0,
            np.log(g["adj_close"] / adj_close_prev),
            np.nan
        )

        return g

    logger.info("Computing returns by ticker")
    df = df.groupby("ticker", group_keys=False).apply(_returns)

    # Keep only the analytics primitives we care about
    out = df[[
        "date",
        "ticker",
        "ret_close",
        "ret_adj_close",
        "log_ret_close",
        "log_ret_adj_close",
    ]].copy()

    out = out.sort_values(["ticker", "date"]).reset_index(drop=True)

    # Ensure analytics directory exists
    analytics_dir = os.path.dirname(ANALYTICS_RETURNS)
    if analytics_dir and not os.path.exists(analytics_dir):
        os.makedirs(analytics_dir, exist_ok=True)

    logger.info(f"Writing price returns to: {ANALYTICS_RETURNS}")
    out.to_parquet(ANALYTICS_RETURNS, index=False)

    logger.info("Price returns build complete")
    print("PRICE_RETURNS_OK")


def main():
    logger = get_logger("price_returns_pipeline")
    logger.info("Price returns pipeline starting")
    build_price_returns()
    logger.info("Price returns pipeline completed successfully")


if __name__ == "__main__":
    main()