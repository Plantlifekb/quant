"""
Canonical pipeline (real implementation — prices + fundamentals, overwrite mode).
"""

from __future__ import annotations
import os
import sys
import pandas as pd

# Absolute paths only
INGESTION_FILE = r"C:\Quant\data\ingestion\ingestion_5years.csv"
CANONICAL_PRICES = r"C:\Quant\data\canonical\prices.parquet"
CANONICAL_FUNDAMENTALS = r"C:\Quant\data\canonical\fundamentals.parquet"


# ----------------------------------------------------------------------
# Logging loader (unchanged pattern)
# ----------------------------------------------------------------------
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

    # Fallback logger
    import logging
    def get_logger(name="canonical_v1"):
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


# ----------------------------------------------------------------------
# Canonical Prices
# ----------------------------------------------------------------------
def canonicalise_prices():
    logger = get_logger("canonical_prices")

    logger.info("Starting canonical prices build")
    logger.info(f"Reading ingestion file: {INGESTION_FILE}")

    if not os.path.exists(INGESTION_FILE):
        logger.error(f"Ingestion file missing: {INGESTION_FILE}")
        raise FileNotFoundError(INGESTION_FILE)

    # Load ingestion CSV with explicit schema (fast, deterministic)
    df = pd.read_csv(
        INGESTION_FILE,
        dtype={
            "ticker": "string",
            "company_name": "string",
            "market_sector": "string",
            "open": "float64",
            "high": "float64",
            "low": "float64",
            "close": "float64",
            "adj_close": "float64",
            "volume": "float64",
            "run_date": "string",
        },
        parse_dates=["date"],
        usecols=[
            "date",
            "ticker",
            "company_name",
            "market_sector",
            "open",
            "high",
            "low",
            "close",
            "adj_close",
            "volume",
            "run_date",
        ],
    )

    # Normalise schema
    df["ticker"] = df["ticker"].astype(str).str.upper()

    # Drop ingestion metadata
    if "run_date" in df.columns:
        df = df.drop(columns=["run_date"])

    # Sort deterministically
    df = df.sort_values(["ticker", "date"]).reset_index(drop=True)

    # Write canonical prices
    logger.info(f"Writing canonical prices to: {CANONICAL_PRICES}")
    df.to_parquet(CANONICAL_PRICES, index=False)

    logger.info("Canonical prices build complete")
    print("CANONICAL_PRICES_OK")


# ----------------------------------------------------------------------
# Canonical Fundamentals (raw, un-enriched)
# ----------------------------------------------------------------------
def canonicalise_fundamentals():
    """
    Canonical fundamentals = raw ingestion fields only:
    date, ticker, company_name, market_sector, open, high, low, close, adj_close, volume
    """
    logger = get_logger("canonical_fundamentals")

    logger.info("Starting canonical fundamentals build")
    logger.info(f"Reading ingestion file: {INGESTION_FILE}")

    if not os.path.exists(INGESTION_FILE):
        logger.error(f"Ingestion file missing: {INGESTION_FILE}")
        raise FileNotFoundError(INGESTION_FILE)

    # Load only the raw fields we want in canonical fundamentals
    df = pd.read_csv(
        INGESTION_FILE,
        dtype={
            "ticker": "string",
            "company_name": "string",
            "market_sector": "string",
            "open": "float64",
            "high": "float64",
            "low": "float64",
            "close": "float64",
            "adj_close": "float64",
            "volume": "float64",
            "run_date": "string",
        },
        parse_dates=["date"],
        usecols=[
            "date",
            "ticker",
            "company_name",
            "market_sector",
            "open",
            "high",
            "low",
            "close",
            "adj_close",
            "volume",
            "run_date",
        ],
    )

    # Normalise schema
    df["ticker"] = df["ticker"].astype(str).str.upper()

    # Drop ingestion metadata
    if "run_date" in df.columns:
        df = df.drop(columns=["run_date"])

    # Sort deterministically
    df = df.sort_values(["ticker", "date"]).reset_index(drop=True)

    # Write canonical fundamentals
    logger.info(f"Writing canonical fundamentals to: {CANONICAL_FUNDAMENTALS}")
    df.to_parquet(CANONICAL_FUNDAMENTALS, index=False)

    logger.info("Canonical fundamentals build complete")
    print("CANONICAL_FUNDAMENTALS_OK")


# ----------------------------------------------------------------------
# Entry point
# ----------------------------------------------------------------------
def main():
    logger = get_logger("canonical_pipeline_quant_v1")
    logger.info("Canonical pipeline starting (prices + fundamentals)")

    canonicalise_prices()
    canonicalise_fundamentals()

    logger.info("Canonical pipeline completed successfully")


if __name__ == "__main__":
    main()