r"""
Quant v1.0 — liquidity_cost_model_quant_v1.py
Version: v1.0

Purpose:
- Compute liquidity-aware trading costs for proposed position changes.
- Provide linear + nonlinear (impact) cost estimates per ticker per date.
- Enforce ADV-based capacity constraints.
- Produce a clean, narratable cost function for the sizing engine.

Inputs:
- Current and target portfolio weights per (date, ticker).
- Market data including price, ADV, spread, and volatility.

Outputs:
- Per-ticker cost breakdown (linear, impact, total).
- Capacity flags (whether a proposed trade breaches ADV limits).
- Portfolio-level aggregate cost and turnover metrics.
"""

import sys
from pathlib import Path
import numpy as np
import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(PROJECT_ROOT / "scripts"))

from logging_quant_v1 import get_logger

logger = get_logger("liquidity_cost_model_quant_v1")

# ---------------------------------------------------------------------
# File locations (canonical, analytics layer)
# ---------------------------------------------------------------------

CURRENT_WEIGHTS_FILE = PROJECT_ROOT / "data" / "analytics" / "quant_portfolio_weights_ensemble_risk_longshort_v2_trading.csv"
TARGET_WEIGHTS_FILE = PROJECT_ROOT / "data" / "analytics" / "quant_portfolio_weights_ensemble_risk_longshort_v2_trading_neutral.csv"
MARKET_DATA_FILE = PROJECT_ROOT / "data" / "analytics" / "quant_market_liquidity_timeseries.csv"

OUT_COSTS_FILE = PROJECT_ROOT / "data" / "analytics" / "quant_liquidity_costs_timeseries.csv"

# ---------------------------------------------------------------------
# Model parameters (governed, narratable)
# ---------------------------------------------------------------------

# Maximum fraction of ADV allowed to trade in a single rebalance
MAX_ADV_PCT = 0.05  # 5%

# Linear cost: spread in decimal (spread_bps / 10000)
# We will read spread_bps from market data and convert.

# Nonlinear impact parameters (Almgren–Chriss style)
IMPACT_GAMMA = 0.1   # scale of impact
IMPACT_ALPHA = 0.5   # curvature (square-root impact)

# Small epsilon to avoid division by zero
EPS = 1e-12


# ---------------------------------------------------------------------
# Loading functions
# ---------------------------------------------------------------------

def load_current_weights() -> pd.DataFrame:
    """
    Load current portfolio weights (pre-trade).
    Expected columns: date, ticker, weight_trading_v2
    """
    logger.info("Loading current weights from %s", CURRENT_WEIGHTS_FILE)
    df = pd.read_csv(CURRENT_WEIGHTS_FILE)

    required_cols = {"date", "ticker", "weight_trading_v2"}
    missing = required_cols.difference(df.columns)
    if missing:
        msg = f"Current weights file missing required columns: {missing}"
        logger.error(msg)
        raise ValueError(msg)

    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df = df.dropna(subset=["date", "ticker", "weight_trading_v2"])
    df["weight_trading_v2"] = pd.to_numeric(df["weight_trading_v2"], errors="coerce")
    df = df.dropna(subset=["weight_trading_v2"])

    df = df.sort_values(["date", "ticker"]).reset_index(drop=True)
    logger.info("Loaded %d current weight rows.", len(df))
    return df


def load_target_weights() -> pd.DataFrame:
    """
    Load target portfolio weights (post-neutralisation or post-signal).
    Expected columns: date, ticker, weight_trading_v2_neutral
    """
    logger.info("Loading target weights from %s", TARGET_WEIGHTS_FILE)
    df = pd.read_csv(TARGET_WEIGHTS_FILE)

    required_cols = {"date", "ticker", "weight_trading_v2_neutral"}
    missing = required_cols.difference(df.columns)
    if missing:
        msg = f"Target weights file missing required columns: {missing}"
        logger.error(msg)
        raise ValueError(msg)

    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df = df.dropna(subset=["date", "ticker", "weight_trading_v2_neutral"])
    df["weight_trading_v2_neutral"] = pd.to_numeric(df["weight_trading_v2_neutral"], errors="coerce")
    df = df.dropna(subset=["weight_trading_v2_neutral"])

    df = df.sort_values(["date", "ticker"]).reset_index(drop=True)
    logger.info("Loaded %d target weight rows.", len(df))
    return df


def load_market_data() -> pd.DataFrame:
    """
    Load market liquidity data.
    Expected columns (minimum):
        - date
        - ticker
        - close_price
        - adv_20 (dollar ADV)
        - spread_bps
        - volatility_20 (optional, for diagnostics)
    """
    logger.info("Loading market data from %s", MARKET_DATA_FILE)
    df = pd.read_csv(MARKET_DATA_FILE)

    required_cols = {"date", "ticker", "close_price", "adv_20", "spread_bps"}
    missing = required_cols.difference(df.columns)
    if missing:
        msg = f"Market data file missing required columns: {missing}"
        logger.error(msg)
        raise ValueError(msg)

    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df = df.dropna(subset=["date", "ticker", "close_price", "adv_20", "spread_bps"])

    df["close_price"] = pd.to_numeric(df["close_price"], errors="coerce")
    df["adv_20"] = pd.to_numeric(df["adv_20"], errors="coerce")
    df["spread_bps"] = pd.to_numeric(df["spread_bps"], errors="coerce")

    df = df.dropna(subset=["close_price", "adv_20", "spread_bps"])
    df = df.sort_values(["date", "ticker"]).reset_index(drop=True)

    logger.info("Loaded %d market data rows.", len(df))
    return df


# ---------------------------------------------------------------------
# Core cost functions
# ---------------------------------------------------------------------

def compute_trade_notional(delta_weight: np.ndarray, price: np.ndarray, portfolio_value: float) -> np.ndarray:
    """
    Convert weight changes into dollar notional traded.
    notional = |delta_weight| * portfolio_value
    """
    # We assume portfolio_value is provided externally or normalised to 1.0.
    # For cost ratios, portfolio_value cancels out, but we keep it explicit.
    notional = np.abs(delta_weight) * portfolio_value
    return notional


def compute_linear_cost(
    delta_weight: np.ndarray,
    spread_bps: np.ndarray,
    portfolio_value: float,
) -> np.ndarray:
    """
    Linear cost based on bid/ask spread.
    C_linear = spread * |delta_weight|
    where spread = spread_bps / 10000
    """
    spread = spread_bps / 10000.0
    notional = compute_trade_notional(delta_weight, None, portfolio_value)
    # Cost in portfolio-value terms: spread * |delta_weight|
    cost_linear = spread * np.abs(delta_weight)
    return cost_linear


def compute_impact_cost(
    delta_weight: np.ndarray,
    adv_dollar: np.ndarray,
    portfolio_value: float,
    gamma: float = IMPACT_GAMMA,
    alpha: float = IMPACT_ALPHA,
) -> np.ndarray:
    """
    Nonlinear market impact cost.
    C_impact = gamma * ( |delta_notional| / ADV )^alpha

    We work in portfolio-weight space:
        delta_notional = |delta_weight| * portfolio_value
        trade_fraction = delta_notional / ADV
    """
    delta_notional = np.abs(delta_weight) * portfolio_value
    adv_safe = np.maximum(adv_dollar, EPS)
    trade_fraction = delta_notional / adv_safe

    cost_impact = gamma * np.power(trade_fraction, alpha)
    return cost_impact


def enforce_capacity(
    delta_weight: np.ndarray,
    adv_dollar: np.ndarray,
    portfolio_value: float,
    max_adv_pct: float = MAX_ADV_PCT,
) -> np.ndarray:
    """
    Capacity flag: True if trade exceeds max_adv_pct of ADV.
    |delta_notional| > max_adv_pct * ADV
    """
    delta_notional = np.abs(delta_weight) * portfolio_value
    adv_safe = np.maximum(adv_dollar, EPS)
    limit = max_adv_pct * adv_safe
    breach = delta_notional > limit
    return breach


# ---------------------------------------------------------------------
# Main builder
# ---------------------------------------------------------------------

def build_liquidity_costs(
    current_weights: pd.DataFrame,
    target_weights: pd.DataFrame,
    market_data: pd.DataFrame,
    portfolio_value: float = 1.0,
) -> pd.DataFrame:
    """
    Build per-ticker liquidity cost estimates for each date.

    Output columns:
        - date
        - ticker
        - weight_current
        - weight_target
        - delta_weight
        - spread_bps
        - adv_20
        - cost_linear
        - cost_impact
        - cost_total
        - capacity_breach (bool)
    """
    logger.info("Building liquidity-aware cost estimates.")

    # Align current and target weights
    w_cur = current_weights.rename(columns={"weight_trading_v2": "weight_current"})
    w_tgt = target_weights.rename(columns={"weight_trading_v2_neutral": "weight_target"})

    df = w_cur.merge(w_tgt, on=["date", "ticker"], how="outer", validate="one_to_one")

    # Fill missing weights with zero (no position or no change)
    df["weight_current"] = df["weight_current"].fillna(0.0)
    df["weight_target"] = df["weight_target"].fillna(0.0)

    df["delta_weight"] = df["weight_target"] - df["weight_current"]

    # Merge market data
    df = df.merge(
        market_data[["date", "ticker", "close_price", "adv_20", "spread_bps"]],
        on=["date", "ticker"],
        how="left",
    )

    # Drop rows without market data (cannot price costs)
    df = df.dropna(subset=["close_price", "adv_20", "spread_bps"]).reset_index(drop=True)

    delta_weight = df["delta_weight"].values.astype(float)
    adv_dollar = df["adv_20"].values.astype(float)
    spread_bps = df["spread_bps"].values.astype(float)

    # Compute costs
    cost_linear = compute_linear_cost(delta_weight, spread_bps, portfolio_value)
    cost_impact = compute_impact_cost(delta_weight, adv_dollar, portfolio_value)
    cost_total = cost_linear + cost_impact

    capacity_breach = enforce_capacity(delta_weight, adv_dollar, portfolio_value)

    df["cost_linear"] = cost_linear
    df["cost_impact"] = cost_impact
    df["cost_total"] = cost_total
    df["capacity_breach"] = capacity_breach

    logger.info(
        "Built liquidity costs for %d rows. Capacity breaches: %d",
        len(df),
        int(df["capacity_breach"].sum()),
    )

    return df.sort_values(["date", "ticker"]).reset_index(drop=True)


# ---------------------------------------------------------------------
# Save function
# ---------------------------------------------------------------------

def save_liquidity_costs(df: pd.DataFrame) -> None:
    logger.info("Saving liquidity costs to %s", OUT_COSTS_FILE)
    df.to_csv(OUT_COSTS_FILE, index=False, encoding="utf-8")


# ---------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------

def main():
    logger.info("Starting liquidity-aware cost model.")

    current_weights = load_current_weights()
    target_weights = load_target_weights()
    market_data = load_market_data()

    costs = build_liquidity_costs(
        current_weights=current_weights,
        target_weights=target_weights,
        market_data=market_data,
        portfolio_value=1.0,  # portfolio-value normalisation; can be scaled later
    )

    save_liquidity_costs(costs)

    logger.info("Completed liquidity-aware cost model successfully.")


if __name__ == "__main__":
    main()