r"""
Quant v1.0 — factor_neutrality_engine_quant_v1.py
Version: v1.1

Purpose:
- Apply factor neutrality to trading weights using a ridge-regularised
  projection to avoid SVD failures and ensure numerical stability.

Key upgrade:
- Replace (B'B)^(-1) with (B'B + lambda I)^(-1), lambda = 1e-6
- Drop zero-variance and all-zero factor columns automatically
"""

import sys
from pathlib import Path
import numpy as np
import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(PROJECT_ROOT / "scripts"))

from logging_quant_v1 import get_logger

logger = get_logger("factor_neutrality_engine_quant_v1")

W_TRADING_FILE = PROJECT_ROOT / "data" / "analytics" / "quant_portfolio_weights_ensemble_risk_longshort_v2_trading.csv"
FACTOR_EXPOSURES_FILE = PROJECT_ROOT / "data" / "analytics" / "quant_factor_exposures_timeseries.csv"
OUT_NEUTRAL_FILE = PROJECT_ROOT / "data" / "analytics" / "quant_portfolio_weights_ensemble_risk_longshort_v2_trading_neutral.csv"

RIDGE_LAMBDA = 1e-6


def load_trading_weights() -> pd.DataFrame:
    logger.info("Loading trading weights from %s", W_TRADING_FILE)
    df = pd.read_csv(W_TRADING_FILE)

    if "date" not in df.columns or "ticker" not in df.columns or "weight_trading_v2" not in df.columns:
        msg = "Trading weights file must contain 'date', 'ticker', 'weight_trading_v2'."
        logger.error(msg)
        raise ValueError(msg)

    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df = df.dropna(subset=["date", "ticker", "weight_trading_v2"])
    df["weight_trading_v2"] = pd.to_numeric(df["weight_trading_v2"], errors="coerce")
    df = df.dropna(subset=["weight_trading_v2"])

    df = df.sort_values(["date", "ticker"]).reset_index(drop=True)
    logger.info("Loaded %d trading weight rows.", len(df))
    return df


def load_factor_exposures() -> tuple[pd.DataFrame, list[str]]:
    logger.info("Loading factor exposures from %s", FACTOR_EXPOSURES_FILE)
    df = pd.read_csv(FACTOR_EXPOSURES_FILE)

    if "date" not in df.columns or "ticker" not in df.columns:
        msg = "Factor exposures file must contain 'date' and 'ticker'."
        logger.error(msg)
        raise ValueError(msg)

    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df = df.dropna(subset=["date", "ticker"])
    df = df.sort_values(["date", "ticker"]).reset_index(drop=True)

    factor_cols = [c for c in df.columns if c not in ["date", "ticker"]]
    if not factor_cols:
        msg = "Factor exposures file must contain at least one factor column."
        logger.error(msg)
        raise ValueError(msg)

    logger.info("Loaded %d factor exposure rows with %d factors.", len(df), len(factor_cols))
    return df, factor_cols


def ridge_neutralise(w: np.ndarray, B: np.ndarray) -> np.ndarray:
    """
    w: vector of weights (n x 1)
    B: factor exposure matrix (n x k)
    """

    # Drop zero-variance columns
    col_std = B.std(axis=0)
    keep = col_std > 0
    B = B[:, keep]

    if B.shape[1] == 0:
        return w

    # Compute ridge-stabilised projection: w_neutral = w - B (B'B + lambda I)^(-1) B' w
    BtB = B.T @ B
    ridge = RIDGE_LAMBDA * np.eye(BtB.shape[0])

    try:
        inv_term = np.linalg.inv(BtB + ridge)
    except np.linalg.LinAlgError:
        # If inversion still fails, fall back to original weights
        return w

    projection = B @ (inv_term @ (B.T @ w))
    w_neutral = w - projection

    # Preserve original gross exposure
    gross_orig = np.sum(np.abs(w))
    gross_new = np.sum(np.abs(w_neutral))
    if gross_new > 0 and gross_orig > 0:
        w_neutral = w_neutral * (gross_orig / gross_new)

    return w_neutral


def build_factor_neutral_weights(
    w: pd.DataFrame,
    f: pd.DataFrame,
    factor_cols: list[str],
) -> pd.DataFrame:
    logger.info("Building factor-neutral trading weights (ridge-regularised).")

    df = w.merge(f, on=["date", "ticker"], how="left")

    # Drop rows without any factor data
    df = df.dropna(subset=factor_cols, how="all")
    df = df.sort_values(["date", "ticker"]).reset_index(drop=True)

    records = []

    for d, g in df.groupby("date"):
        g_local = g.set_index("ticker")

        w_vec = g_local["weight_trading_v2"].values.astype(float)
        B = g_local[factor_cols].values.astype(float)

        w_neutral = ridge_neutralise(w_vec, B)

        for t, wt in zip(g_local.index, w_neutral):
            records.append(
                {
                    "date": d,
                    "ticker": t,
                    "weight_trading_v2_neutral": float(wt),
                }
            )

    out = pd.DataFrame.from_records(records)
    out = out.sort_values(["date", "ticker"]).reset_index(drop=True)
    logger.info("Built %d factor-neutral weight rows.", len(out))
    return out


def save_neutral_weights(df: pd.DataFrame) -> None:
    logger.info("Saving factor-neutral trading weights to %s", OUT_NEUTRAL_FILE)
    df.to_csv(OUT_NEUTRAL_FILE, index=False, encoding="utf-8")


def main():
    logger.info("Starting ridge-stabilised factor neutrality engine.")

    w = load_trading_weights()
    f, factor_cols = load_factor_exposures()
    w_neutral = build_factor_neutral_weights(w, f, factor_cols)
    save_neutral_weights(w_neutral)

    logger.info("Completed successfully with ridge regularisation.")


if __name__ == "__main__":
    main()