r"""
Quant v1.0 — position_sizing_engine_quant_v1.py
Version: v1.0

Purpose:
- Construct final tradable portfolio weights by balancing:
  * expected returns (signals)
  * factor neutrality (already enforced upstream)
  * liquidity-aware trading costs
  * turnover control

Inputs:
- Neutralised target weights (from neutrality engine)
- Current weights (live portfolio)
- Liquidity costs (from liquidity_cost_model_quant_v1)
- Optional: expected return proxy (e.g., model_signal_v3 or composite_signal_v1)

Outputs:
- quant_portfolio_weights_tradable_v1.csv
"""

import sys
from pathlib import Path
import numpy as np
import pandas as pd
from scipy.optimize import minimize

PROJECT_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(PROJECT_ROOT / "scripts"))

from logging_quant_v1 import get_logger

logger = get_logger("position_sizing_engine_quant_v1")

# ---------------------------------------------------------------------
# File locations
# ---------------------------------------------------------------------

CURRENT_WEIGHTS_FILE = PROJECT_ROOT / "data" / "analytics" / "quant_portfolio_weights_ensemble_risk_longshort_v2_trading.csv"
TARGET_WEIGHTS_FILE = PROJECT_ROOT / "data" / "analytics" / "quant_portfolio_weights_ensemble_risk_longshort_v2_trading_neutral.csv"
LIQUIDITY_COSTS_FILE = PROJECT_ROOT / "data" / "analytics" / "quant_liquidity_costs_timeseries.csv"
FACTOR_EXPOSURES_FILE = PROJECT_ROOT / "data" / "analytics" / "quant_factor_exposures_timeseries.csv"

OUT_TRADABLE_WEIGHTS_FILE = PROJECT_ROOT / "data" / "analytics" / "quant_portfolio_weights_tradable_v1.csv"

# ---------------------------------------------------------------------
# Parameters (governed)
# ---------------------------------------------------------------------

# Risk/return trade-off: how much we penalise deviation from target vs cost
LAMBDA_COST = 1.0      # weight on trading cost
LAMBDA_TRACK = 10.0    # weight on tracking error vs neutral target

# Turnover constraint (per rebalance, in weight space)
MAX_TURNOVER = 0.30    # 30% of gross notional per rebalance

# Gross exposure constraint
MAX_GROSS_EXPOSURE = 1.0  # long + short <= 1.0

EPS = 1e-10


# ---------------------------------------------------------------------
# Loaders
# ---------------------------------------------------------------------

def load_current_weights() -> pd.DataFrame:
    logger.info("Loading current weights from %s", CURRENT_WEIGHTS_FILE)
    df = pd.read_csv(CURRENT_WEIGHTS_FILE)
    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df = df.dropna(subset=["date", "ticker", "weight_trading_v2"])
    df["weight_trading_v2"] = pd.to_numeric(df["weight_trading_v2"], errors="coerce")
    df = df.dropna(subset=["weight_trading_v2"])
    return df.sort_values(["date", "ticker"]).reset_index(drop=True)


def load_target_weights() -> pd.DataFrame:
    logger.info("Loading target (neutral) weights from %s", TARGET_WEIGHTS_FILE)
    df = pd.read_csv(TARGET_WEIGHTS_FILE)
    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df = df.dropna(subset=["date", "ticker", "weight_trading_v2_neutral"])
    df["weight_trading_v2_neutral"] = pd.to_numeric(df["weight_trading_v2_neutral"], errors="coerce")
    df = df.dropna(subset=["weight_trading_v2_neutral"])
    return df.sort_values(["date", "ticker"]).reset_index(drop=True)


def load_liquidity_costs() -> pd.DataFrame:
    logger.info("Loading liquidity costs from %s", LIQUIDITY_COSTS_FILE)
    df = pd.read_csv(LIQUIDITY_COSTS_FILE)
    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df = df.dropna(subset=["date", "ticker", "cost_total"])
    df["cost_total"] = pd.to_numeric(df["cost_total"], errors="coerce")
    df = df.dropna(subset=["cost_total"])
    return df.sort_values(["date", "ticker"]).reset_index(drop=True)


def load_factor_exposures() -> pd.DataFrame:
    """
    Optional: used only if we want an extra neutrality check or
    to penalise residual factor tilts in the objective.
    """
    logger.info("Loading factor exposures from %s", FACTOR_EXPOSURES_FILE)
    df = pd.read_csv(FACTOR_EXPOSURES_FILE)
    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    return df.sort_values(["date", "ticker"]).reset_index(drop=True)


# ---------------------------------------------------------------------
# Per-date optimisation
# ---------------------------------------------------------------------

def optimise_for_date(
    date: pd.Timestamp,
    df_slice: pd.DataFrame,
) -> pd.DataFrame:
    """
    Optimise tradable weights for a single date.

    df_slice columns:
        - ticker
        - weight_current
        - weight_target
        - cost_total
    """

    tickers = df_slice["ticker"].values
    w_cur = df_slice["weight_current"].values.astype(float)
    w_tgt = df_slice["weight_target"].values.astype(float)
    c_tot = df_slice["cost_total"].values.astype(float)

    n = len(tickers)

    if n == 0:
        return pd.DataFrame(columns=["date", "ticker", "weight_tradable_v1"])

    # Objective:
    #   L = LAMBDA_TRACK * ||w - w_tgt||^2 + LAMBDA_COST * sum( cost_total * |w - w_cur| )
    #
    # We solve a smooth approximation by using squared deviation for cost as well:
    #   approx_cost = cost_total * (w - w_cur)^2
    #
    # This keeps the optimisation convex and smooth.

    def objective(w):
        track_term = LAMBDA_TRACK * np.sum((w - w_tgt) ** 2)
        cost_term = LAMBDA_COST * np.sum(c_tot * (w - w_cur) ** 2)
        return track_term + cost_term

    # Constraints:
    # 1) Gross exposure <= MAX_GROSS_EXPOSURE
    #    sum(|w|) <= MAX_GROSS_EXPOSURE
    #
    # 2) Turnover <= MAX_TURNOVER
    #    sum(|w - w_cur|) <= MAX_TURNOVER
    #
    # We enforce these via inequality constraints using smooth approximations.

    def gross_exposure_constraint(w):
        return MAX_GROSS_EXPOSURE - np.sum(np.abs(w))

    def turnover_constraint(w):
        return MAX_TURNOVER - np.sum(np.abs(w - w_cur))

    constraints = [
        {"type": "ineq", "fun": gross_exposure_constraint},
        {"type": "ineq", "fun": turnover_constraint},
    ]

    # No hard bounds on individual weights for now (can add later if needed)
    bounds = [(-1.0, 1.0)] * n

    # Initial guess: neutral target weights
    w0 = w_tgt.copy()

    result = minimize(
        objective,
        w0,
        method="SLSQP",
        bounds=bounds,
        constraints=constraints,
        options={"maxiter": 500, "ftol": 1e-9, "disp": False},
    )

    if not result.success:
        logger.warning(
            "Optimisation failed for %s: %s. Falling back to target weights.",
            date.date(),
            result.message,
        )
        w_opt = w_tgt
    else:
        w_opt = result.x

    out = pd.DataFrame({
        "date": date,
        "ticker": tickers,
        "weight_tradable_v1": w_opt,
        "weight_current": w_cur,
        "weight_target": w_tgt,
    })

    return out


# ---------------------------------------------------------------------
# Main builder
# ---------------------------------------------------------------------

def build_tradable_weights(
    current_weights: pd.DataFrame,
    target_weights: pd.DataFrame,
    liquidity_costs: pd.DataFrame,
) -> pd.DataFrame:
    logger.info("Building tradable weights (Position Sizing Engine).")

    w_cur = current_weights.rename(columns={"weight_trading_v2": "weight_current"})
    w_tgt = target_weights.rename(columns={"weight_trading_v2_neutral": "weight_target"})

    # Merge current + target
    df = w_cur.merge(
        w_tgt[["date", "ticker", "weight_target"]],
        on=["date", "ticker"],
        how="outer",
        validate="one_to_one",
    )

    df["weight_current"] = df["weight_current"].fillna(0.0)
    df["weight_target"] = df["weight_target"].fillna(0.0)

    # Merge liquidity costs
    df = df.merge(
        liquidity_costs[["date", "ticker", "cost_total"]],
        on=["date", "ticker"],
        how="left",
    )

    # If any cost_total is missing, set to a small positive number (so cost term doesn't vanish)
    df["cost_total"] = df["cost_total"].fillna(df["cost_total"].median())
    df["cost_total"] = df["cost_total"].fillna(1e-4)

    df = df.sort_values(["date", "ticker"]).reset_index(drop=True)

    tradable_list = []

    for date, df_slice in df.groupby("date"):
        logger.info("Optimising tradable weights for %s (%d names).", date.date(), len(df_slice))
        res = optimise_for_date(date, df_slice)
        tradable_list.append(res)

    out = pd.concat(tradable_list, axis=0, ignore_index=True)
    out = out.sort_values(["date", "ticker"]).reset_index(drop=True)

    logger.info("Built tradable weights for %d rows.", len(out))
    return out


# ---------------------------------------------------------------------
# Save
# ---------------------------------------------------------------------

def save_tradable_weights(df: pd.DataFrame) -> None:
    logger.info("Saving tradable weights to %s", OUT_TRADABLE_WEIGHTS_FILE)
    df.to_csv(OUT_TRADABLE_WEIGHTS_FILE, index=False, encoding="utf-8")


# ---------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------

def main():
    logger.info("Starting Position Sizing Engine (Quant v1.0).")

    current_weights = load_current_weights()
    target_weights = load_target_weights()
    liquidity_costs = load_liquidity_costs()

    tradable = build_tradable_weights(
        current_weights=current_weights,
        target_weights=target_weights,
        liquidity_costs=liquidity_costs,
    )

    save_tradable_weights(tradable)

    logger.info("Completed Position Sizing Engine successfully.")


if __name__ == "__main__":
    main()