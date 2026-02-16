r"""
Quant v1.0 — position_sizing_engine_quant_v1_osqp.py

OSQP position sizing engine with:
- Regime-aware alpha scaling
- Sector exposure constraints
- Factor exposure constraints (linear)
- Trade-to-target L1 penalty
- Turnover constraint
- Gross exposure constraint
- Liquidity-aware cost term
- Tracking term

Governance:
- Dates/tickers normalised
- Expected returns used when present, else 0.0 with explicit logging
- All NaNs in costs/sector/factor/regime cleaned before optimisation
- OSQP failures (setup or solve) fall back to target weights
- No quadratic risk model (future covariance file)
"""

import sys
from pathlib import Path
import numpy as np
import pandas as pd
import scipy.sparse as sp
import osqp

PROJECT_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(PROJECT_ROOT / "scripts"))

from logging_quant_v1 import get_logger
logger = get_logger("position_sizing_engine_quant_v1_osqp")

# ---------------------------------------------------------------------
# File locations
# ---------------------------------------------------------------------

CURRENT_WEIGHTS_FILE = PROJECT_ROOT / "data" / "analytics" / "quant_portfolio_weights_ensemble_risk_longshort_v2_trading.csv"
TARGET_WEIGHTS_FILE = PROJECT_ROOT / "data" / "analytics" / "quant_portfolio_weights_ensemble_risk_longshort_v2_trading_neutral.csv"
LIQUIDITY_COSTS_FILE = PROJECT_ROOT / "data" / "analytics" / "quant_liquidity_costs_timeseries.csv"
EXPECTED_RETURN_FILE = PROJECT_ROOT / "data" / "analytics" / "quant_expected_returns_timeseries.csv"
FACTOR_FILE = PROJECT_ROOT / "data" / "analytics" / "quant_factors_ensemble_risk_v1.csv"

OUT_TRADABLE_WEIGHTS_FILE = PROJECT_ROOT / "data" / "analytics" / "quant_portfolio_weights_tradable_v1_osqp.csv"

# ---------------------------------------------------------------------
# Parameters
# ---------------------------------------------------------------------

LAMBDA_TRACK = 10.0
LAMBDA_COST = 1.0
LAMBDA_RETURN = 1.0
LAMBDA_TRADE_TO_TARGET = 5.0

MAX_GROSS_EXPOSURE = 2.0
MAX_TURNOVER = 3.0

SECTOR_CAP = 0.02
FACTOR_CAP = 0.02

# ---------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------

def _normalise_date_ticker(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df["date"] = pd.to_datetime(df["date"])
    df["ticker"] = df["ticker"].astype(str).str.strip().str.upper()
    return df

# ---------------------------------------------------------------------
# Loaders
# ---------------------------------------------------------------------

def load_current_weights() -> pd.DataFrame:
    df = pd.read_csv(CURRENT_WEIGHTS_FILE)
    df = _normalise_date_ticker(df)
    df = df.rename(columns={"weight_trading_v2": "weight_current"})
    return df[["date", "ticker", "weight_current"]]

def load_target_weights() -> pd.DataFrame:
    df = pd.read_csv(TARGET_WEIGHTS_FILE)
    df = _normalise_date_ticker(df)
    df = df.rename(columns={"weight_trading_v2_neutral": "weight_target"})
    return df[["date", "ticker", "weight_target"]]

def load_liquidity_costs() -> pd.DataFrame:
    df = pd.read_csv(LIQUIDITY_COSTS_FILE)
    df = _normalise_date_ticker(df)
    return df[["date", "ticker", "cost_total"]]

def load_expected_returns() -> pd.DataFrame:
    df = pd.read_csv(EXPECTED_RETURN_FILE)
    df = _normalise_date_ticker(df)

    cols = df.columns.tolist()
    if "expected_return" not in cols:
        logger.warning(
            "Expected-return column 'expected_return' not found in %s. "
            "Columns present: %s. Expected returns will be treated as 0.0.",
            EXPECTED_RETURN_FILE,
            cols,
        )
        return pd.DataFrame(columns=["date", "ticker", "ret"])

    df = df.rename(columns={"expected_return": "ret"})
    return df[["date", "ticker", "ret"]]

def load_factors() -> pd.DataFrame:
    df = pd.read_csv(FACTOR_FILE)
    df = _normalise_date_ticker(df)
    return df

# ---------------------------------------------------------------------
# Expected-return handling (non-fatal)
# ---------------------------------------------------------------------

def ensure_ret_column(df: pd.DataFrame) -> pd.DataFrame:
    """
    Ensure df has a 'ret' column.
    - If missing entirely, create ret = 0.0 and log.
    - If present but with NaNs, fill NaNs with 0.0 and log count.
    """
    df = df.copy()

    if "ret" not in df.columns:
        logger.warning(
            "Column 'ret' is missing from merged dataframe after joins. "
            "All expected returns will be treated as 0.0 for this run."
        )
        df["ret"] = 0.0
        return df

    missing_mask = df["ret"].isna()
    if missing_mask.any():
        n_missing = int(missing_mask.sum())
        logger.warning(
            "Detected %d rows with missing expected returns after merge. "
            "These will be set to 0.0 for this run.",
            n_missing,
        )
        df.loc[missing_mask, "ret"] = 0.0

    return df

# ---------------------------------------------------------------------
# Data cleaning before optimisation
# ---------------------------------------------------------------------

def clean_optimisation_slice(df_slice: pd.DataFrame) -> pd.DataFrame:
    """
    Ensure no NaNs or infs in any column used by the optimiser.
    """
    df_slice = df_slice.copy()

    # Costs
    if "cost_total" in df_slice.columns:
        if df_slice["cost_total"].notna().any():
            med = df_slice["cost_total"].median()
            if pd.isna(med):
                med = 0.0
        else:
            med = 0.0
        df_slice["cost_total"] = df_slice["cost_total"].fillna(med).replace([np.inf, -np.inf], med)
    else:
        df_slice["cost_total"] = 0.0

    # Regime score
    if "regime_score" not in df_slice.columns:
        df_slice["regime_score"] = 0.0
    df_slice["regime_score"] = df_slice["regime_score"].fillna(0.0).replace([np.inf, -np.inf], 0.0)

    # Sector columns
    sector_cols = [c for c in df_slice.columns if c.startswith("sector_")]
    for c in sector_cols:
        df_slice[c] = df_slice[c].fillna(0.0).replace([np.inf, -np.inf], 0.0)

    # Factor columns
    factor_cols = ["size_factor", "vol_factor", "liquidity_factor"]
    for c in factor_cols:
        if c not in df_slice.columns:
            df_slice[c] = 0.0
        df_slice[c] = df_slice[c].fillna(0.0).replace([np.inf, -np.inf], 0.0)

    # Weights
    df_slice["weight_current"] = df_slice["weight_current"].fillna(0.0).replace([np.inf, -np.inf], 0.0)
    df_slice["weight_target"] = df_slice["weight_target"].fillna(0.0).replace([np.inf, -np.inf], 0.0)

    # Returns
    if "ret" not in df_slice.columns:
        df_slice["ret"] = 0.0
    df_slice["ret"] = df_slice["ret"].fillna(0.0).replace([np.inf, -np.inf], 0.0)

    return df_slice

# ---------------------------------------------------------------------
# OSQP optimisation
# ---------------------------------------------------------------------

def optimise_for_date(date, df_slice: pd.DataFrame) -> pd.DataFrame:
    df_slice = clean_optimisation_slice(df_slice)

    tickers = df_slice["ticker"].values
    w_cur = df_slice["weight_current"].values
    w_tgt = df_slice["weight_target"].values
    cost = df_slice["cost_total"].values

    alpha = df_slice["ret"].values * (1 + df_slice["regime_score"].values)

    sector_cols = [c for c in df_slice.columns if c.startswith("sector_")]
    if sector_cols:
        S = df_slice[sector_cols].values
    else:
        S = np.zeros((len(df_slice), 0))

    factor_cols = ["size_factor", "vol_factor", "liquidity_factor"]
    F = df_slice[factor_cols].values

    n = len(tickers)

    # Quadratic term on w
    P_w = 2 * LAMBDA_TRACK * np.ones(n) + 2 * LAMBDA_COST * cost
    P = sp.block_diag([
        sp.diags(P_w),          # w
        sp.csr_matrix((n, n)),  # u (turnover)
        sp.csr_matrix((n, n)),  # v (gross)
        sp.csr_matrix((n, n)),  # z (trade-to-target)
    ])

    # Linear term
    q_w = (
        -2 * LAMBDA_TRACK * w_tgt
        -2 * LAMBDA_COST * cost * w_cur
        - LAMBDA_RETURN * alpha
    )
    q = np.concatenate([
        q_w,
        np.zeros(n),                         # u
        np.zeros(n),                         # v
        LAMBDA_TRADE_TO_TARGET * np.ones(n)  # z
    ])

    A_blocks = []
    l_blocks = []
    u_blocks = []

    # Turnover: u >= |w - w_cur|
    A_blocks.append(sp.hstack([sp.eye(n), -sp.eye(n), sp.csr_matrix((n, n)), sp.csr_matrix((n, n))]))
    l_blocks.append(-np.inf * np.ones(n))
    u_blocks.append(w_cur)

    A_blocks.append(sp.hstack([-sp.eye(n), -sp.eye(n), sp.csr_matrix((n, n)), sp.csr_matrix((n, n))]))
    l_blocks.append(-np.inf * np.ones(n))
    u_blocks.append(-w_cur)

    # Gross exposure: v >= |w|
    A_blocks.append(sp.hstack([sp.eye(n), sp.csr_matrix((n, n)), -sp.eye(n), sp.csr_matrix((n, n))]))
    l_blocks.append(-np.inf * np.ones(n))
    u_blocks.append(np.zeros(n))

    A_blocks.append(sp.hstack([-sp.eye(n), sp.csr_matrix((n, n)), -sp.eye(n), sp.csr_matrix((n, n))]))
    l_blocks.append(-np.inf * np.ones(n))
    u_blocks.append(np.zeros(n))

    # Trade-to-target: z >= |w - w_tgt|
    A_blocks.append(sp.hstack([sp.eye(n), sp.csr_matrix((n, n)), sp.csr_matrix((n, n)), -sp.eye(n)]))
    l_blocks.append(-np.inf * np.ones(n))
    u_blocks.append(w_tgt)

    A_blocks.append(sp.hstack([-sp.eye(n), sp.csr_matrix((n, n)), sp.csr_matrix((n, n)), -sp.eye(n)]))
    l_blocks.append(-np.inf * np.ones(n))
    u_blocks.append(-w_tgt)

    # Gross exposure cap
    A_blocks.append(sp.hstack([
        sp.csr_matrix((1, n)),
        sp.csr_matrix((1, n)),
        np.ones((1, n)),
        sp.csr_matrix((1, n)),
    ]))
    l_blocks.append(np.array([-np.inf]))
    u_blocks.append(np.array([MAX_GROSS_EXPOSURE]))

    # Turnover cap
    A_blocks.append(sp.hstack([
        sp.csr_matrix((1, n)),
        np.ones((1, n)),
        sp.csr_matrix((1, n)),
        sp.csr_matrix((1, n)),
    ]))
    l_blocks.append(np.array([-np.inf]))
    u_blocks.append(np.array([MAX_TURNOVER]))

    # Sector exposure caps
    for k in range(S.shape[1]):
        A_blocks.append(sp.hstack([
            S[:, [k]].T,
            sp.csr_matrix((1, n)),
            sp.csr_matrix((1, n)),
            sp.csr_matrix((1, n)),
        ]))
        l_blocks.append(np.array([-SECTOR_CAP]))
        u_blocks.append(np.array([SECTOR_CAP]))

    # Factor exposure caps
    for k in range(F.shape[1]):
        A_blocks.append(sp.hstack([
            F[:, [k]].T,
            sp.csr_matrix((1, n)),
            sp.csr_matrix((1, n)),
            sp.csr_matrix((1, n)),
        ]))
        l_blocks.append(np.array([-FACTOR_CAP]))
        u_blocks.append(np.array([FACTOR_CAP]))

    A = sp.vstack(A_blocks)
    l = np.concatenate(l_blocks)
    u = np.concatenate(u_blocks)

    # OSQP setup and solve with protection
    try:
        prob = osqp.OSQP()
        prob.setup(
            P=P,
            q=q,
            A=A,
            l=l,
            u=u,
            verbose=False,
            eps_abs=1e-5,
            eps_rel=1e-5,
            max_iter=10000,
            polish=True,
        )
        res = prob.solve()
    except Exception as e:
        logger.warning(
            "OSQP setup failed for %s with error %r. Falling back to target weights.",
            date.date(),
            e,
        )
        w_opt = w_tgt
        return pd.DataFrame({
            "date": date,
            "ticker": tickers,
            "weight_tradable_v1": w_opt,
        })

    if res.info.status != "solved":
        logger.warning(
            "OSQP solve failed for %s with status '%s'. Falling back to target weights.",
            date.date(),
            res.info.status,
        )
        logger.warning(
            "Diagnostics: sum|w_cur|=%.4f, sum|w_tgt|=%.4f, turnover_needed=%.4f, "
            "gross_target=%.4f, turnover_cap=%.4f, gross_cap=%.4f",
            float(np.sum(np.abs(w_cur))),
            float(np.sum(np.abs(w_tgt))),
            float(np.sum(np.abs(w_tgt - w_cur))),
            float(np.sum(np.abs(w_tgt))),
            MAX_TURNOVER,
            MAX_GROSS_EXPOSURE,
        )
        w_opt = w_tgt
    else:
        w_opt = res.x[:n]

    return pd.DataFrame({
        "date": date,
        "ticker": tickers,
        "weight_tradable_v1": w_opt,
    })

# ---------------------------------------------------------------------
# Main builder
# ---------------------------------------------------------------------

def build_tradable_weights() -> pd.DataFrame:
    logger.info("Building tradable weights using upgraded OSQP.")

    w_cur = load_current_weights()
    w_tgt = load_target_weights()
    cost = load_liquidity_costs()
    ret = load_expected_returns()
    fac = load_factors()

    df = (
        w_cur
        .merge(w_tgt, on=["date", "ticker"], how="outer")
        .merge(cost, on=["date", "ticker"], how="left")
        .merge(ret, on=["date", "ticker"], how="left")
        .merge(fac, on=["date", "ticker"], how="left")
    )

    # Basic fills; per-date cleaning happens in clean_optimisation_slice
    df["weight_current"] = df["weight_current"].fillna(0.0)
    df["weight_target"] = df["weight_target"].fillna(0.0)

    if "cost_total" not in df.columns:
        df["cost_total"] = 0.0

    if "regime_score" not in df.columns:
        df["regime_score"] = 0.0

    df = ensure_ret_column(df)

    out_list = []
    for date, df_slice in df.groupby("date"):
        logger.info("Optimising %s (%d names).", date.date(), len(df_slice))
        out = optimise_for_date(date, df_slice)
        out_list.append(out)

    out = pd.concat(out_list).sort_values(["date", "ticker"]).reset_index(drop=True)
    return out

# ---------------------------------------------------------------------
# Save
# ---------------------------------------------------------------------

def save_tradable_weights(df: pd.DataFrame) -> None:
    logger.info("Saving tradable weights to %s", OUT_TRADABLE_WEIGHTS_FILE)
    df.to_csv(OUT_TRADABLE_WEIGHTS_FILE, index=False)

# ---------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------

def main() -> None:
    logger.info("Starting upgraded OSQP Position Sizing Engine.")
    tradable = build_tradable_weights()
    save_tradable_weights(tradable)
    logger.info("Completed upgraded OSQP Position Sizing Engine successfully.")

if __name__ == "__main__":
    main()