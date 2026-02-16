"""
optimiser_regime
Canonical regime and volatility feature engine for optimiser and expected returns.

Input:
    C:\Quant\data\ingestion\ingestion_5years.csv

Output:
    C:\Quant\data\analytics\optimiser_regime_quant_v1.parquet

Schema (per date, ticker):
    date (datetime64[ns])
    ticker (string)
    close (float)
    sma50 (float)
    sma200 (float)
    state (string)
    ret_close (float)
    ret_adj_close (float)
    log_ret_close (float)
    log_ret_adj_close (float)
    vol20_simple (float)
    vol60_simple (float)
    vol20_log (float)
    vol60_log (float)
    rolling_max (float)
    drawdown (float)
    vol_zscore (float)
    regime (string)
"""

from __future__ import annotations
import os
import numpy as np
import pandas as pd


INGESTION_FILE = r"C:\Quant\data\ingestion\ingestion_5years.csv"
OUTPUT_FILE = r"C:\Quant\data\analytics\optimiser_regime_quant_v1.parquet"


def _load_logging_module():
    """Reuse canonical logging module pattern."""
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

    def get_logger(name="optimiser_regime"):
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


def _classify_state(drawdown: float, vol_z: float) -> str:
    """Simple, transparent state machine."""
    if np.isnan(drawdown) or np.isnan(vol_z):
        return "Unknown"

    if drawdown < -0.15 and vol_z > 1.0:
        return "Stress"
    if drawdown < -0.10 and vol_z > 0.5:
        return "Correction"
    if drawdown > -0.05 and vol_z < -0.5:
        return "Calm"
    return "Sideways"


def _classify_regime(vol_z: float) -> str:
    """Simple volatility‑based regime classifier."""
    if np.isnan(vol_z):
        return "Unknown"
    if vol_z > 1.0:
        return "HighVol"
    if vol_z < -0.5:
        return "LowVol"
    return "Normal"


def build_regime():
    logger = get_logger("optimiser_regime")

    logger.info("Starting optimiser_regime build")
    logger.info(f"Reading ingestion file: {INGESTION_FILE}")

    if not os.path.exists(INGESTION_FILE):
        logger.error(f"Ingestion file missing: {INGESTION_FILE}")
        raise FileNotFoundError(INGESTION_FILE)

    df = pd.read_csv(INGESTION_FILE)

    # Normalise columns
    df.columns = [c.lower() for c in df.columns]

    required = {"date", "ticker", "close", "adj_close"}
    missing = required.difference(df.columns)
    if missing:
        msg = f"Ingestion file missing required columns: {sorted(missing)}"
        logger.error(msg)
        raise ValueError(msg)

    logger.info("Parsing dates and sorting by ticker/date")
    df["date"] = pd.to_datetime(df["date"], errors="raise")
    df = df.sort_values(["ticker", "date"]).reset_index(drop=True)

    # Core price features
    logger.info("Computing returns and moving averages")
    df["ret_close"] = df.groupby("ticker")["close"].pct_change()
    df["ret_adj_close"] = df.groupby("ticker")["adj_close"].pct_change()

    df["log_ret_close"] = np.log1p(df["ret_close"])
    df["log_ret_adj_close"] = np.log1p(df["ret_adj_close"])

    df["sma50"] = df.groupby("ticker")["close"].transform(lambda x: x.rolling(50, min_periods=10).mean())
    df["sma200"] = df.groupby("ticker")["close"].transform(lambda x: x.rolling(200, min_periods=20).mean())

    # Volatility features
    logger.info("Computing volatility features")
    df["vol20_simple"] = (
        df.groupby("ticker")["ret_close"]
        .transform(lambda x: x.rolling(20, min_periods=10).std())
    )
    df["vol60_simple"] = (
        df.groupby("ticker")["ret_close"]
        .transform(lambda x: x.rolling(60, min_periods=20).std())
    )

    df["vol20_log"] = (
        df.groupby("ticker")["log_ret_close"]
        .transform(lambda x: x.rolling(20, min_periods=10).std())
    )
    df["vol60_log"] = (
        df.groupby("ticker")["log_ret_close"]
        .transform(lambda x: x.rolling(60, min_periods=20).std())
    )

    # Drawdown features
    logger.info("Computing rolling max and drawdown")
    df["rolling_max"] = df.groupby("ticker")["close"].transform(lambda x: x.cummax())
    df["drawdown"] = df["close"] / df["rolling_max"] - 1.0

    # Volatility z‑score (cross‑sectional per date)
    logger.info("Computing volatility z‑scores")
    def _zscore(group: pd.Series) -> pd.Series:
        m = group.mean()
        s = group.std(ddof=0)
        if s == 0 or np.isnan(s):
            return pd.Series(np.nan, index=group.index)
        return (group - m) / s

    df["vol_zscore"] = df.groupby("date")["vol20_log"].transform(_zscore)

    # Regime and state classification
    logger.info("Classifying regime and state")
    df["regime"] = df["vol_zscore"].apply(_classify_regime)
    df["state"] = [
        _classify_state(dd, vz)
        for dd, vz in zip(df["drawdown"].to_numpy(), df["vol_zscore"].to_numpy())
    ]

    # Final schema
    out_cols = [
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
    ]
    out = df[out_cols].copy()
    out = out.sort_values(["ticker", "date"]).reset_index(drop=True)

    analytics_dir = os.path.dirname(OUTPUT_FILE)
    if analytics_dir and not os.path.exists(analytics_dir):
        os.makedirs(analytics_dir, exist_ok=True)

    logger.info(f"Writing regime features parquet: {OUTPUT_FILE}")
    out.to_parquet(OUTPUT_FILE, index=False)

    logger.info("optimiser_regime build complete")
    print("OPTIMISER_REGIME_OK")


def main():
    logger = get_logger("optimiser_regime_pipeline")
    logger.info("optimiser_regime pipeline starting")
    build_regime()
    logger.info("optimiser_regime pipeline completed successfully")


if __name__ == "__main__":
    main()