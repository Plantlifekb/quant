"""
Quant v1.1 — optimiser_regime_quant_v1.py
Regime-aware optimiser with:
- Regime-scaled risk, turnover, liquidity aversion
- Regime-scaled turnover caps
- Regime-aware factor tilting
- Regime-aware exposure scaling
- OSQP quadratic optimisation
"""

import sys
from pathlib import Path
from datetime import datetime, timezone

import numpy as np
import pandas as pd
import osqp
from scipy import sparse

PROJECT_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(PROJECT_ROOT / "scripts"))

from logging_quant_v1 import get_logger  # type: ignore

logger = get_logger("optimiser_regime_quant_v1")

# ---------------------------------------------------------
# FILE PATHS
# ---------------------------------------------------------

EXPECTED_RET_FILE = PROJECT_ROOT / "data" / "analytics" / "quant_expected_returns_timeseries.csv"
RISK_REGIME_FILE = PROJECT_ROOT / "data" / "analytics" / "quant_risk_regime_v1.csv"
FACTOR_EXPOSURES_FILE = PROJECT_ROOT / "data" / "analytics" / "quant_factor_exposures_timeseries.csv"
PREV_WEIGHTS_FILE = PROJECT_ROOT / "data" / "analytics" / "quant_portfolio_weights_tradable_v1_osqp.csv"
REGIME_STATES_FILE = PROJECT_ROOT / "data" / "analytics" / "quant_regime_states_v1.csv"
PORTFOLIO_CONTROLS_FILE = PROJECT_ROOT / "data" / "analytics" / "quant_portfolio_controls_v1.csv"

OUT_FILE = PROJECT_ROOT / "data" / "analytics" / "quant_portfolio_weights_regime_v1.csv"


def iso_now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")


# ---------------------------------------------------------
# 1. REGIME SCALARS (risk, turnover, liquidity, turnover cap)
# ---------------------------------------------------------

def regime_scalars(regime_label: str) -> dict:
    regime_label = (regime_label or "").lower()

    core = regime_label.split("_")[0]

    if core in {"calm", "normal"}:
        return dict(risk=0.9, turnover=0.9, liquidity=0.9, to_cap=1.2, exposure=1.00)
    if core in {"volatile"}:
        return dict(risk=1.2, turnover=1.2, liquidity=1.2, to_cap=0.8, exposure=0.60)
    if core in {"crisis", "stress"}:
        return dict(risk=1.5, turnover=1.5, liquidity=1.5, to_cap=0.5, exposure=0.20)

    return dict(risk=1.0, turnover=1.0, liquidity=1.0, to_cap=1.0, exposure=1.00)


# ---------------------------------------------------------
# 2. REGIME-AWARE FACTOR TILTING
# ---------------------------------------------------------

FACTOR_TILTS = {
    "calm":     {"value": 1.1, "momentum": 1.1, "quality": 1.0, "low_vol": 0.9},
    "normal":   {"value": 1.1, "momentum": 1.1, "quality": 1.0, "low_vol": 0.9},
    "volatile": {"value": 0.9, "momentum": 0.8, "quality": 1.2, "low_vol": 1.2},
    "crisis":   {"value": 0.7, "momentum": 0.6, "quality": 1.4, "low_vol": 1.4},
    "stress":   {"value": 0.7, "momentum": 0.6, "quality": 1.4, "low_vol": 1.4},
}

def apply_factor_tilts(exposures_df: pd.DataFrame, regime_label: str) -> pd.DataFrame:
    core = regime_label.split("_")[0]
    tilts = FACTOR_TILTS.get(core, {})

    df = exposures_df.copy()
    for factor, mult in tilts.items():
        if factor in df.columns:
            df[factor] *= mult

    return df


# ---------------------------------------------------------
# 3. LOADERS
# ---------------------------------------------------------

def load_expected_returns(target_date: pd.Timestamp) -> pd.DataFrame:
    df = pd.read_csv(EXPECTED_RET_FILE)
    df.columns = [c.lower() for c in df.columns]
    df["date"] = pd.to_datetime(df["date"], utc=True)
    df = df[df["date"] == target_date]
    if df.empty:
        raise ValueError(f"No expected returns for {target_date.date()}")
    return df[["date", "ticker", "expected_return"]]


def load_factor_exposures(target_date: pd.Timestamp) -> pd.DataFrame:
    df = pd.read_csv(FACTOR_EXPOSURES_FILE)
    df.columns = [c.lower() for c in df.columns]
    df["date"] = pd.to_datetime(df["date"], utc=True)
    df = df[df["date"] == target_date]
    if df.empty:
        raise ValueError(f"No factor exposures for {target_date.date()}")
    return df


def load_prev_weights(target_date: pd.Timestamp) -> pd.DataFrame:
    df = pd.read_csv(PREV_WEIGHTS_FILE)
    df.columns = [c.lower() for c in df.columns]
    df["date"] = pd.to_datetime(df["date"], utc=True)

    prev_date = df["date"].max()
    if prev_date >= target_date:
        prev_date = target_date - pd.Timedelta(days=1)

    df = df[df["date"] == prev_date]
    if df.empty:
        return pd.DataFrame(columns=["ticker", "weight"])

    return df[["ticker", "weight_tradable_v1"]].rename(columns={"weight_tradable_v1": "weight"})


def load_regime_state(target_date: pd.Timestamp) -> str:
    df = pd.read_csv(REGIME_STATES_FILE)
    df.columns = [c.lower() for c in df.columns]
    df["date"] = pd.to_datetime(df["date"], utc=True)
    df = df[df["date"] <= target_date].sort_values("date")
    if df.empty:
        return "unknown"
    return str(df.iloc[-1]["regime_label"])


def load_portfolio_controls(target_date: pd.Timestamp) -> dict:
    df = pd.read_csv(PORTFOLIO_CONTROLS_FILE)
    df.columns = [c.lower() for c in df.columns]
    df["date"] = pd.to_datetime(df["date"], utc=True)
    df = df[df["date"] <= target_date].sort_values("date")
    if df.empty:
        raise ValueError(f"No portfolio controls for {target_date.date()}")

    row = df.iloc[-1]
    return {
        "gross_limit": float(row.get("gross_limit", 2.0)),
        "long_limit": float(row.get("long_limit", 1.0)),
        "short_limit": float(row.get("short_limit", -1.0)),
        "max_turnover": float(row.get("max_turnover", 0.3)),
        "base_risk_aversion": float(row.get("base_risk_aversion", 1.0)),
        "base_turnover_aversion": float(row.get("base_turnover_aversion", 1.0)),
        "base_liquidity_aversion": float(row.get("base_liquidity_aversion", 1.0)),
    }


def load_risk_regime(target_date: pd.Timestamp) -> pd.DataFrame:
    df = pd.read_csv(RISK_REGIME_FILE)
    df.columns = [c.lower() for c in df.columns]
    df["date"] = pd.to_datetime(df["date"], utc=True)
    df = df[df["date"] <= target_date]
    if df.empty:
        raise ValueError(f"No risk model for {target_date.date()}")
    last_date = df["date"].max()
    df = df[df["date"] == last_date]
    return df[["factor_name", "factor_var_regime", "regime_label"]]


# ---------------------------------------------------------
# 4. OSQP PROBLEM
# ---------------------------------------------------------

def build_osqp_problem(
    mu, exposures, factor_var, w_prev,
    long_limit, short_limit,
    risk_aversion, turnover_aversion
):
    n = len(mu)

    Sigma_diag = np.sum((exposures ** 2) * factor_var.reshape(1, -1), axis=1)
    Sigma_diag = np.maximum(Sigma_diag, 1e-8)

    P = sparse.diags(2.0 * risk_aversion * Sigma_diag)
    q = -mu.copy()

    P = P + sparse.eye(n) * (2.0 * turnover_aversion)
    q = q - 2.0 * turnover_aversion * w_prev

    A = sparse.eye(n)
    l = np.full(n, short_limit)
    u = np.full(n, long_limit)

    prob = osqp.OSQP()
    prob.setup(P=P, q=q, A=A, l=l, u=u, verbose=False)

    return prob


# ---------------------------------------------------------
# 5. MAIN OPTIMISATION
# ---------------------------------------------------------

def run_optimisation_for_date(target_date: pd.Timestamp) -> pd.DataFrame:
    logger.info("Running optimiser for %s", target_date.date())

    exp_ret_df = load_expected_returns(target_date)
    exposures_df = load_factor_exposures(target_date)
    prev_w_df = load_prev_weights(target_date)
    risk_df = load_risk_regime(target_date)
    controls = load_portfolio_controls(target_date)
    regime_label = load_regime_state(target_date)

    scalars = regime_scalars(regime_label)

    risk_aversion = controls["base_risk_aversion"] * scalars["risk"]
    turnover_aversion = controls["base_turnover_aversion"] * scalars["turnover"]
    max_turnover_regime = controls["max_turnover"] * scalars["to_cap"]
    exposure_scale = scalars["exposure"]

    tickers = sorted(set(exp_ret_df["ticker"]).intersection(exposures_df["ticker"]))
    exp_ret_df = exp_ret_df.set_index("ticker").loc[tickers]
    exposures_df = exposures_df.set_index("ticker").loc[tickers]

    exposures_df = apply_factor_tilts(exposures_df, regime_label)

    factor_names = risk_df["factor_name"].unique().tolist()
    factor_names = [f for f in factor_names if f in exposures_df.columns]
    risk_df = risk_df[risk_df["factor_name"].isin(factor_names)]

    X = exposures_df[factor_names].values
    factor_var = risk_df.set_index("factor_name").loc[factor_names]["factor_var_regime"].values
    mu = exp_ret_df["expected_return"].values

    prev_w_df = prev_w_df.set_index("ticker").reindex(tickers).fillna(0.0)
    w_prev = prev_w_df["weight"].values

    prob = build_osqp_problem(
        mu=mu,
        exposures=X,
        factor_var=factor_var,
        w_prev=w_prev,
        long_limit=controls["long_limit"] * exposure_scale,
        short_limit=controls["short_limit"] * exposure_scale,
        risk_aversion=risk_aversion,
        turnover_aversion=turnover_aversion,
    )

    res = prob.solve()
    if res.info.status_val not in (1, 2):
        raise RuntimeError(f"OSQP failed: {res.info.status}")

    w_opt = res.x

    out = pd.DataFrame({
        "date": target_date,
        "ticker": tickers,
        "weight": w_opt,
        "expected_return": mu,
        "regime_label": regime_label,
        "risk_aversion_regime": risk_aversion,
        "turnover_aversion_regime": turnover_aversion,
        "exposure_scale": exposure_scale,
    })

    return out


def save_portfolio(df: pd.DataFrame, run_date: str) -> None:
    df = df.copy()
    df["optimiser_run_date"] = run_date

    OUT_FILE.parent.mkdir(parents=True, exist_ok=True)

    if OUT_FILE.exists():
        existing = pd.read_csv(OUT_FILE)
        existing["date"] = pd.to_datetime(existing["date"], utc=True)
        combined = pd.concat([existing, df], ignore_index=True)
        combined = combined.drop_duplicates(subset=["date", "ticker"], keep="last")
        combined = combined.sort_values(["date", "ticker"])
        combined.to_csv(OUT_FILE, index=False)
    else:
        df.to_csv(OUT_FILE, index=False)


def main() -> None:
    logger.info("Starting optimiser_regime_quant_v1.1")
    run_date = iso_now()

    df_er = pd.read_csv(EXPECTED_RET_FILE)
    df_er["date"] = pd.to_datetime(df_er["date"], utc=True)
    target_date = df_er["date"].max()

    portfolio = run_optimisation_for_date(target_date)
    save_portfolio(portfolio, run_date)

    logger.info("Completed optimiser_regime_quant_v1.1")


if __name__ == "__main__":
    main()