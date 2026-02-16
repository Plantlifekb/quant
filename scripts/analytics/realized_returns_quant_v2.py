"""
realized_returns_quant_v2
Modernises realised returns ingestion into a clean, parquet-driven analytics module.

Input:
    C:\Quant\data\analytics\quant_realized_returns_v1.csv
        Expected columns (case-insensitive):
            date
            ticker
            return_close_to_close   (legacy name)

Output:
    C:\Quant\data\analytics\realized_returns_quant_v2.parquet

Schema:
    date (datetime64[ns])
    ticker (string)
    realized_return (float)
"""

from __future__ import annotations
import os
import pandas as pd
import numpy as np


INPUT_FILE = r"C:\Quant\data\analytics\quant_realized_returns_v1.csv"
OUTPUT_FILE = r"C:\Quant\data\analytics\realized_returns_quant_v2.parquet"


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

    def get_logger(name="realized_returns_quant_v2"):
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


def build_realized_returns():
    logger = get_logger("realized_returns_quant_v2")

    logger.info("Starting realized_returns_quant_v2 build")
    logger.info(f"Reading legacy realised returns CSV: {INPUT_FILE}")

    if not os.path.exists(INPUT_FILE):
        logger.error(f"Realised returns CSV missing: {INPUT_FILE}")
        raise FileNotFoundError(INPUT_FILE)

    df = pd.read_csv(INPUT_FILE)
    df.columns = [c.lower() for c in df.columns]

    # Validate required columns
    required = {"date", "ticker"}
    missing = required.difference(df.columns)
    if missing:
        msg = f"Realised returns CSV missing required columns: {sorted(missing)}"
        logger.error(msg)
        raise ValueError(msg)

    # Identify realised return column
    rr_candidates = [
        "realized_return",
        "realised_return",
        "return",
        "ret",
        "return_close_to_close",  # legacy name
    ]
    rr_col = next((c for c in rr_candidates if c in df.columns), None)

    if rr_col is None:
        msg = f"Could not find realised return column; looked for: {rr_candidates}"
        logger.error(msg)
        raise ValueError(msg)

    logger.info(f"Using realised return column: {rr_col}")

    # Standardise schema
    df = df.rename(columns={rr_col: "realized_return"})

    # Enforce datetime
    logger.info("Converting date column to datetime")
    df["date"] = pd.to_datetime(df["date"], errors="raise")

    # Enforce numeric return
    logger.info("Converting realized_return to numeric")
    df["realized_return"] = pd.to_numeric(df["realized_return"], errors="coerce")

    # Drop rows with missing returns
    before = len(df)
    df = df.dropna(subset=["realized_return"])
    after = len(df)
    logger.info(f"Dropped {before - after} rows with invalid realised returns")

    # Sort for consistency
    df = df.sort_values(["ticker", "date"]).reset_index(drop=True)

    # Write parquet
    analytics_dir = os.path.dirname(OUTPUT_FILE)
    if analytics_dir and not os.path.exists(analytics_dir):
        os.makedirs(analytics_dir, exist_ok=True)

    logger.info(f"Writing clean realised returns parquet: {OUTPUT_FILE}")
    df.to_parquet(OUTPUT_FILE, index=False)

    logger.info("realized_returns_quant_v2 build complete")
    print("REALIZED_RETURNS_QUANT_V2_OK")


def main():
    logger = get_logger("realized_returns_quant_v2_pipeline")
    logger.info("realized_returns_quant_v2 pipeline starting")
    build_realized_returns()
    logger.info("realized_returns_quant_v2 pipeline completed successfully")


if __name__ == "__main__":
    main()