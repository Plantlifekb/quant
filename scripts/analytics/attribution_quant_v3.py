"""
attribution_quant_v3
Parquet-driven attribution using optimised portfolio weights and realised returns.

Inputs:
    C:\Quant\data\analytics\portfolio_optimised_quant_v3.parquet
    C:\Quant\data\analytics\quant_realized_returns_v1.csv

Output:
    C:\Quant\data\analytics\attribution_quant_v3.parquet
"""

from __future__ import annotations
import os
import pandas as pd
import numpy as np


PORTFOLIO_FILE = r"C:\Quant\data\analytics\portfolio_optimised_quant_v3.parquet"
REALIZED_RETURNS_FILE = r"C:\Quant\data\analytics\quant_realized_returns_v1.csv"
ATTRIBUTION_OUTPUT_FILE = r"C:\Quant\data\analytics\attribution_quant_v3.parquet"


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

    def get_logger(name="attribution_quant_v3"):
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


def _compute_attribution(
    portfolio: pd.DataFrame,
    realized: pd.DataFrame,
) -> pd.DataFrame:
    """
    portfolio: portfolio_optimised_quant_v3.parquet
        [date, ticker, weight, expected_return, vol20_log, regime, state]

    realized: quant_realized_returns_v1.csv
        [date, ticker, realized_return]  (column name inferred below)

    Returns per-ticker attribution with portfolio-level return.
    """
    # Normalise column names for realised returns
    realized = realized.copy()
    realized.columns = [c.lower() for c in realized.columns]

    # Try to find the realised return column
    candidate_cols = [
        "realized_return",
        "realised_return",
        "ret",
        "return",
        "return_close_to_close",   # <-- your actual column
    ]
    rr_col = None
    for c in candidate_cols:
        if c in realized.columns:
            rr_col = c
            break

    if rr_col is None:
        raise ValueError(
            f"Could not find realised return column in {REALIZED_RETURNS_FILE}; "
            f"looked for {candidate_cols}"
        )

    # Standardise schema
    realized = realized.rename(columns={rr_col: "realized_return"})
    if "ticker" not in realized.columns or "date" not in realized.columns:
        raise ValueError(
            f"Realised returns file must contain 'date' and 'ticker' columns; "
            f"found: {sorted(realized.columns)}"
        )

    # Merge portfolio weights with realised returns
    df = pd.merge(
        portfolio[
            [
                "date",
                "ticker",
                "weight",
                "expected_return",
                "vol20_log",
                "regime",
                "state",
            ]
        ],
        realized[["date", "ticker", "realized_return"]],
        on=["date", "ticker"],
        how="inner",
    )

    df = df.sort_values(["date", "ticker"]).reset_index(drop=True)

    # Clean infinities / NaNs
    df = df.replace([np.inf, -np.inf], np.nan)

    # Contribution = weight * realised return
    df["contribution"] = df["weight"] * df["realized_return"]

    # Portfolio-level return per date
    port = (
        df.groupby("date", as_index=False)["contribution"]
        .sum()
        .rename(columns={"contribution": "portfolio_return"})
    )

    df = pd.merge(df, port, on="date", how="left")

    # Final schema
    out = df[
        [
            "date",
            "ticker",
            "weight",
            "realized_return",
            "contribution",
            "portfolio_return",
            "expected_return",
            "vol20_log",
            "regime",
            "state",
        ]
    ].copy()

    out = out.sort_values(["date", "ticker"]).reset_index(drop=True)
    return out


def build_attribution():
    """
    Build parquet-driven attribution from optimised portfolio and realised returns.

    Output schema:
        date
        ticker
        weight
        realized_return
        contribution        # weight * realized_return
        portfolio_return    # sum of contributions per date
        expected_return
        vol20_log
        regime
        state
    """
    logger = get_logger("attribution_quant_v3")

    logger.info("Starting attribution_quant_v3 build")
    logger.info(f"Reading optimised portfolio: {PORTFOLIO_FILE}")
    logger.info(f"Reading realised returns: {REALIZED_RETURNS_FILE}")

    if not os.path.exists(PORTFOLIO_FILE):
        logger.error(f"Optimised portfolio missing: {PORTFOLIO_FILE}")
        raise FileNotFoundError(PORTFOLIO_FILE)

    if not os.path.exists(REALIZED_RETURNS_FILE):
        logger.error(f"Realised returns missing: {REALIZED_RETURNS_FILE}")
        raise FileNotFoundError(REALIZED_RETURNS_FILE)

    portfolio = pd.read_parquet(PORTFOLIO_FILE)
    realized = pd.read_csv(REALIZED_RETURNS_FILE)
    # Force date to datetime for clean merge
    realized["date"] = pd.to_datetime(realized["date"], errors="raise")

    required_portfolio = {"date", "ticker", "weight"}
    missing_portfolio = required_portfolio.difference(portfolio.columns)
    if missing_portfolio:
        msg = f"Optimised portfolio missing required columns: {sorted(missing_portfolio)}"
        logger.error(msg)
        raise ValueError(msg)

    portfolio = portfolio.sort_values(["date", "ticker"]).reset_index(drop=True)
    realized = realized.sort_values(["date", "ticker"]).reset_index(drop=True)

    logger.info("Computing attribution by date and ticker")
    out = _compute_attribution(portfolio, realized)

    analytics_dir = os.path.dirname(ATTRIBUTION_OUTPUT_FILE)
    if analytics_dir and not os.path.exists(analytics_dir):
        os.makedirs(analytics_dir, exist_ok=True)

    logger.info(f"Writing attribution to: {ATTRIBUTION_OUTPUT_FILE}")
    out.to_parquet(ATTRIBUTION_OUTPUT_FILE, index=False)

    logger.info("Attribution_quant_v3 build complete")
    print("ATTRIBUTION_QUANT_V3_OK")


def main():
    logger = get_logger("attribution_quant_v3_pipeline")
    logger.info("Attribution_quant_v3 pipeline starting")
    build_attribution()
    logger.info("Attribution_quant_v3 pipeline completed successfully")


if __name__ == "__main__":
    main()