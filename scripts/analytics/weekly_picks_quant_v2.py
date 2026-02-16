"""
weekly_picks_quant_v2
Canonical weekly picks engine.

Inputs:
    portfolio_optimised_quant_v3.parquet
    realized_returns.parquet

Output:
    weekly_picks_quant_v2.parquet
"""

from __future__ import annotations
import os
import numpy as np
import pandas as pd


def _load_logging_module():
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

    import logging

    def get_logger(name="weekly_picks_quant_v2"):
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

PORTFOLIO_FILE = r"C:\Quant\data\analytics\portfolio_optimised_quant_v3.parquet"
REALIZED_RETURNS_PARQUET = r"C:\Quant\data\analytics\realized_returns.parquet"
OUTPUT_FILE = r"C:\Quant\data\analytics\weekly_picks_quant_v2.parquet"


def build_weekly_picks():
    logger = get_logger("weekly_picks_quant_v2")
    logger.info("Starting weekly_picks_quant_v2 build")

    # ---------------------------------------------------------
    # Load portfolio (contains expected_return, regime, state)
    # ---------------------------------------------------------
    logger.info(f"Reading portfolio: {PORTFOLIO_FILE}")
    portfolio = pd.read_parquet(PORTFOLIO_FILE)
    portfolio["date"] = pd.to_datetime(portfolio["date"], errors="raise")

    portfolio["week_start"] = portfolio["date"] - pd.to_timedelta(
        portfolio["date"].dt.weekday, unit="D"
    )

    # ---------------------------------------------------------
    # Load realized returns
    # ---------------------------------------------------------
    logger.info(f"Reading realised returns parquet: {REALIZED_RETURNS_PARQUET}")
    realized = pd.read_parquet(REALIZED_RETURNS_PARQUET)
    realized["date"] = pd.to_datetime(realized["date"], errors="raise")

    logger.info("Computing weekly realised returns")
    realized = realized.sort_values(["ticker", "date"])
    realized["weekly_realized_return"] = (
        realized.groupby("ticker")["realized_return"]
        .transform(lambda x: x.rolling(5, min_periods=1).sum())
    )
    realized["week_start"] = realized["date"] - pd.to_timedelta(
        realized["date"].dt.weekday, unit="D"
    )
    weekly_realized = realized[
        ["ticker", "week_start", "weekly_realized_return"]
    ].drop_duplicates()

    # ---------------------------------------------------------
    # FULL‑UNIVERSE WEEKLY RANKING (THE FIX)
    # ---------------------------------------------------------
    logger.info("Selecting weekly picks (FULL UNIVERSE)")

    picks = []
    for week, group in portfolio.groupby("week_start"):
        g = group.copy()

        # Rank entire universe by expected_return (model score)
        g = g.sort_values("expected_return", ascending=False).copy()
        g["pick_rank"] = np.arange(1, len(g) + 1)

        picks.append(g)

    picks = pd.concat(picks, ignore_index=True)

    # ---------------------------------------------------------
    # Merge realized returns
    # ---------------------------------------------------------
    logger.info("Merging weekly realised returns")
    picks = picks.merge(
        weekly_realized,
        on=["ticker", "week_start"],
        how="left",
    )

    required_cols = [
        "week_start",
        "date",
        "ticker",
        "pick_rank",
        "weight",
        "expected_return",
        "weekly_realized_return",
        "regime",
        "state",
    ]
    missing = [c for c in required_cols if c not in picks.columns]
    if missing:
        raise RuntimeError(f"Weekly picks missing expected columns: {missing}")

    out = picks[required_cols].copy()

    analytics_dir = os.path.dirname(OUTPUT_FILE)
    if analytics_dir and not os.path.exists(analytics_dir):
        os.makedirs(analytics_dir, exist_ok=True)

    logger.info(f"Writing weekly picks to: {OUTPUT_FILE}")
    out.to_parquet(OUTPUT_FILE, index=False)

    logger.info("weekly_picks_quant_v2 build complete")
    print("WEEKLY_PICKS_QUANT_V2_OK")


def main():
    logger = get_logger("weekly_picks_quant_v2_pipeline")
    logger.info("weekly_picks_quant_v2 pipeline starting")
    build_weekly_picks()
    logger.info("weekly_picks_quant_v2 pipeline completed successfully")


if __name__ == "__main__":
    main()