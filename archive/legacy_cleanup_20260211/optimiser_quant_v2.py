r"""
Quant v1.0 — optimiser_quant_v2.py
Version: v2.0 (signal-driven, turnover-aware)

1. Module name
- optimiser_quant_v2

2. Quant version
- Quant v1.0

3. Purpose
- Build turnover-aware, signal-driven cross-sectional weights from the
  risk-neutral ensemble signal, without inheriting legacy equal-weight
  scaffolding.
- For each date:
  - longshort:
    - weights proportional to sign(signal) * |signal|
    - normalised so sum |w| = 1
  - longonly:
    - use only positive signals
    - weights proportional to signal
    - normalised so sum w = 1
  - apply exponential smoothing over time to control turnover.

4. Inputs
- C:\Quant\data\analytics\quant_factors_ensemble_risk_v1.csv

  Required columns:
    - date
    - ticker
    - ensemble_signal_v1_resid

5. Outputs
- C:\Quant\data\analytics\quant_portfolio_weights_ensemble_risk_longshort_v2.csv
- C:\Quant\data\analytics\quant_portfolio_weights_ensemble_risk_longonly_v2.csv

  Columns:
    - date
    - ticker
    - weight_longshort_v2 / weight_longonly_v2

6. Optimiser logic (per date)
- Build raw weights from signal:
  - longshort:
    w_raw_i ∝ sign(signal_i) * |signal_i|
    normalise: sum_i |w_raw_i| = 1
  - longonly:
    use only signal_i > 0
    w_raw_i ∝ signal_i
    normalise: sum_i w_raw_i = 1
- Turnover-aware smoothing:
  - w_t = (1 - kappa) * w_{t-1} + kappa * w_raw_t
  - kappa in (0, 1); here kappa = 0.3
- First date:
  - w_0 = w_raw_0 (no legacy equal-weight seeding)

7. Governance rules
- No schema drift.
- All output columns lowercase.
- ISO-8601 dates only.
- Deterministic behaviour.

8. Dependencies
- pandas
- numpy
- logging_quant_v1

9. Provenance
- Governed component of Quant v1.0.
"""

import sys
from pathlib import Path
import numpy as np
import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(PROJECT_ROOT / "scripts"))

from logging_quant_v1 import get_logger

logger = get_logger("optimiser_quant_v2")

FACTORS_FILE = PROJECT_ROOT / "data" / "analytics" / "quant_factors_ensemble_risk_v1.csv"

W_LS_V2_FILE = PROJECT_ROOT / "data" / "analytics" / "quant_portfolio_weights_ensemble_risk_longshort_v2.csv"
W_LO_V2_FILE = PROJECT_ROOT / "data" / "analytics" / "quant_portfolio_weights_ensemble_risk_longonly_v2.csv"

# Turnover / smoothing parameter
KAPPA = 0.3  # 0 = no change vs previous day, 1 = full move to raw signal


def load_factors() -> pd.DataFrame:
    logger.info("Loading factors for optimiser v2.0 from %s", FACTORS_FILE)

    f = pd.read_csv(FACTORS_FILE)

    required = {"date", "ticker", "ensemble_signal_v1_resid"}
    missing = required - set(f.columns)
    if missing:
        msg = f"Missing required columns in factors file: {missing}"
        logger.error(msg)
        raise ValueError(msg)

    f["date"] = pd.to_datetime(f["date"], errors="coerce")
    f = f.dropna(subset=["date", "ticker", "ensemble_signal_v1_resid"])

    f["ensemble_signal_v1_resid"] = pd.to_numeric(
        f["ensemble_signal_v1_resid"], errors="coerce"
    )
    f = f.dropna(subset=["ensemble_signal_v1_resid"])

    f = f.sort_values(["date", "ticker"]).reset_index(drop=True)

    logger.info("Loaded %d factor rows after cleaning.", len(f))
    return f[["date", "ticker", "ensemble_signal_v1_resid"]]


def _build_raw_weights_longshort(df: pd.DataFrame) -> pd.Series:
    """
    Long-short raw weights:
    w_i ∝ sign(signal_i) * |signal_i|
    normalised so sum |w_i| = 1.
    """
    sig = df["ensemble_signal_v1_resid"].values
    tickers = df["ticker"].values

    if len(sig) == 0:
        return pd.Series(dtype=float)

    w_raw = np.sign(sig) * np.abs(sig)
    abs_sum = np.sum(np.abs(w_raw))
    if abs_sum <= 0:
        return pd.Series(0.0, index=tickers)

    w_raw = w_raw / abs_sum
    return pd.Series(w_raw, index=tickers)


def _build_raw_weights_longonly(df: pd.DataFrame) -> pd.Series:
    """
    Long-only raw weights:
    use only positive signals, w_i ∝ signal_i,
    normalised so sum w_i = 1.
    """
    sig = df["ensemble_signal_v1_resid"].values
    tickers = df["ticker"].values

    if len(sig) == 0:
        return pd.Series(dtype=float)

    mask = sig > 0
    if not mask.any():
        # No positive signals: return zero weights
        return pd.Series(0.0, index=tickers)

    sig_pos = sig.copy()
    sig_pos[~mask] = 0.0
    denom = np.sum(sig_pos)
    if denom <= 0:
        return pd.Series(0.0, index=tickers)

    w_raw = sig_pos / denom
    return pd.Series(w_raw, index=tickers)


def _smooth_weights(
    dates: list[pd.Timestamp],
    raw_by_date: dict[pd.Timestamp, pd.Series],
    kappa: float,
) -> list[dict]:
    """
    Exponential smoothing over time:
    w_t = (1 - kappa) * w_{t-1} + kappa * w_raw_t
    First date: w_0 = w_raw_0.
    """
    records: list[dict] = []
    w_prev: pd.Series | None = None

    for d in dates:
        w_raw = raw_by_date.get(d)
        if w_raw is None or w_raw.empty:
            # No signal for this date; carry forward previous weights if any
            if w_prev is None:
                continue
            w_new = w_prev.copy()
        else:
            if w_prev is None:
                # First date: pure signal
                w_new = w_raw.copy()
            else:
                all_tickers = sorted(set(w_prev.index) | set(w_raw.index))
                prev_vec = w_prev.reindex(all_tickers).fillna(0.0).values
                raw_vec = w_raw.reindex(all_tickers).fillna(0.0).values
                w_vec = (1.0 - kappa) * prev_vec + kappa * raw_vec
                w_new = pd.Series(w_vec, index=all_tickers)

        # Normalise
        abs_sum = np.sum(np.abs(w_new.values))
        if abs_sum > 0:
            w_new = w_new / abs_sum

        for t, w in w_new.items():
            records.append(
                {
                    "date": d,
                    "ticker": t,
                    "weight": float(w),
                }
            )

        w_prev = w_new

    return records


def build_v2_weights(f: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    logger.info("Building v2 turnover-aware, signal-driven weights.")

    dates = sorted(f["date"].unique())

    # Build raw weights per date for longshort and longonly
    raw_ls_by_date: dict[pd.Timestamp, pd.Series] = {}
    raw_lo_by_date: dict[pd.Timestamp, pd.Series] = {}

    for d, g in f.groupby("date"):
        g = g[["ticker", "ensemble_signal_v1_resid"]].copy()
        raw_ls_by_date[d] = _build_raw_weights_longshort(g)
        raw_lo_by_date[d] = _build_raw_weights_longonly(g)

    ls_records = _smooth_weights(
        dates=dates,
        raw_by_date=raw_ls_by_date,
        kappa=KAPPA,
    )

    lo_records = _smooth_weights(
        dates=dates,
        raw_by_date=raw_lo_by_date,
        kappa=KAPPA,
    )

    w_ls_v2 = pd.DataFrame.from_records(ls_records)
    w_lo_v2 = pd.DataFrame.from_records(lo_records)

    if not w_ls_v2.empty:
        w_ls_v2 = w_ls_v2.sort_values(["date", "ticker"]).reset_index(drop=True)
        w_ls_v2 = w_ls_v2.rename(columns={"weight": "weight_longshort_v2"})

    if not w_lo_v2.empty:
        w_lo_v2 = w_lo_v2.sort_values(["date", "ticker"]).reset_index(drop=True)
        w_lo_v2 = w_lo_v2.rename(columns={"weight": "weight_longonly_v2"})

    logger.info(
        "Built %d long-short v2 weight rows and %d long-only v2 weight rows.",
        len(w_ls_v2),
        len(w_lo_v2),
    )

    return w_ls_v2, w_lo_v2


def save_outputs(w_ls_v2: pd.DataFrame, w_lo_v2: pd.DataFrame) -> None:
    logger.info("Saving long-short v2 weights to %s", W_LS_V2_FILE)
    w_ls_v2.to_csv(W_LS_V2_FILE, index=False, encoding="utf-8")

    logger.info("Saving long-only v2 weights to %s", W_LO_V2_FILE)
    w_lo_v2.to_csv(W_LO_V2_FILE, index=False, encoding="utf-8")


def main():
    logger.info("Starting optimiser_quant_v2 run (v2.0, signal-driven).")

    f = load_factors()
    w_ls_v2, w_lo_v2 = build_v2_weights(f)
    save_outputs(w_ls_v2, w_lo_v2)

    logger.info("optimiser_quant_v2 run completed successfully.")


if __name__ == "__main__":
    main()