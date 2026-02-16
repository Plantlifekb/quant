import sys
import os
import numpy as np
import pandas as pd


PARQUET_PATH = r"C:\Quant\data\analytics\optimiser_regime_quant_v1.parquet"


def fail(msg: str) -> None:
    print(f"[FAIL] {msg}")
    sys.exit(1)


def ok(msg: str) -> None:
    print(f"[OK] {msg}")


def main() -> None:
    # ---------- 1. Load ----------
    if not os.path.exists(PARQUET_PATH):
        fail(f"File not found: {PARQUET_PATH}")

    df = pd.read_parquet(PARQUET_PATH)
    rows, cols = df.shape
    print(f"[INFO] Loaded {rows} rows, {cols} columns")

    # ---------- 2. Shape & panel checks ----------
    expected_cols = [
        "date", "ticker", "close",
        "sma50", "sma200",
        "state",
        "ret_close", "ret_adj_close",
        "log_ret_close", "log_ret_adj_close",
        "vol20_simple", "vol60_simple",
        "vol20_log", "vol60_log",
        "rolling_max", "drawdown",
        "vol_zscore", "regime",
    ]

    if list(df.columns) != expected_cols:
        fail(f"Unexpected columns.\nExpected: {expected_cols}\nGot: {list(df.columns)}")
    ok("Column set matches expected schema")

    n_tickers = df["ticker"].nunique()
    n_dates = df["date"].nunique()

    if n_tickers != 515:
        fail(f"Expected 515 tickers, found {n_tickers}")
    if n_dates != 1320:
        fail(f"Expected 1320 dates, found {n_dates}")
    ok(f"Panel dimensions OK: {n_tickers} tickers × {n_dates} dates")

    per_ticker_counts = df.groupby("ticker").size()
    if not (per_ticker_counts == 1320).all():
        fail("Not all tickers have 1320 rows")
    ok("Each ticker has exactly 1320 rows (fully rectangular panel)")

    # ---------- 3. Missingness checks ----------
    na_ratio = df.isna().mean()

    zero_na_cols = [
        "date", "ticker", "close",
        "rolling_max", "drawdown",
        "state", "regime",
    ]
    for c in zero_na_cols:
        if na_ratio[c] != 0.0:
            fail(f"Column {c} has unexpected missing values: {na_ratio[c]:.6f}")
    ok("Core columns have 0% missingness")

    expected_max_na = {
        "sma200": 0.02,
        "vol60_simple": 0.02,
        "vol60_log": 0.02,
        "sma50": 0.02,
        "vol20_simple": 0.02,
        "vol20_log": 0.02,
        "vol_zscore": 0.02,
        "ret_close": 0.01,
        "ret_adj_close": 0.01,
        "log_ret_close": 0.01,
        "log_ret_adj_close": 0.01,
    }

    for c, max_ratio in expected_max_na.items():
        if na_ratio[c] > max_ratio:
            fail(f"Column {c} has too many NaNs: {na_ratio[c]:.6f} > {max_ratio:.6f}")
    ok("Missingness in rolling/return fields is within expected bounds")

    # ---------- 4. Mathematical consistency ----------
    computed_ret = df["close"] / df.groupby("ticker")["close"].shift(1) - 1
    max_diff_ret = (computed_ret - df["ret_close"]).abs().max()
    if max_diff_ret > 1e-12:
        fail(f"ret_close mismatch: max abs diff = {max_diff_ret}")
    ok("ret_close matches recomputed returns (within numerical precision)")

    log_close = np.log(df["close"])
    log_close_shift = df.groupby("ticker")["close"].shift(1).pipe(np.log)
    computed_log_ret = log_close - log_close_shift
    max_diff_log_ret = (computed_log_ret - df["log_ret_close"]).abs().max()
    if max_diff_log_ret > 1e-12:
        fail(f"log_ret_close mismatch: max abs diff = {max_diff_log_ret}")
    ok("log_ret_close matches recomputed log returns (within numerical precision)")

    if (df["rolling_max"] < df["close"]).any():
        fail("Found rows where rolling_max < close")
    ok("rolling_max is always >= close")

    computed_dd = df["close"] / df["rolling_max"] - 1.0
    max_diff_dd = (computed_dd - df["drawdown"]).abs().max()
    if max_diff_dd > 1e-12:
        fail(f"drawdown mismatch: max abs diff = {max_diff_dd}")
    ok("drawdown matches close / rolling_max - 1")

    print("[SUCCESS] optimiser_regime_quant_v1.parquet passed all validation checks")


if __name__ == "__main__":
    main()