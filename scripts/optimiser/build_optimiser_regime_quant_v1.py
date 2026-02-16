"""
build_optimiser_regime_quant_v1.py

Quant v1.0 — Directional Long-Short, Risk-Controlled, Regime-Aware Optimiser

Inputs:
    C:\Quant\data\signals\expected_returns_quant_v1.parquet
    C:\Quant\data\ingestion\risk_model.parquet
    C:\Quant\data\reference\securities_master.parquet

Output:
    C:\Quant\data\analytics\optimiser_regime_quant_v1.parquet
"""

import sys
from pathlib import Path

import numpy as np
import pandas as pd
import cvxpy as cp


BASE = Path(r"C:\Quant")
EXPECTED_RETURNS = BASE / "data" / "signals" / "expected_returns_quant_v1.parquet"
RISK_MODEL = BASE / "data" / "ingestion" / "risk_model.parquet"
SEC_MASTER = BASE / "data" / "reference" / "securities_master.parquet"
OUT = BASE / "data" / "analytics" / "optimiser_regime_quant_v1.parquet"


def fail(msg: str):
    print(f"\n❌ FAIL: {msg}\n")
    sys.exit(1)


def ok(msg: str):
    print(f"✔ {msg}")


def load_inputs():
    if not EXPECTED_RETURNS.exists():
        fail(f"Missing expected returns: {EXPECTED_RETURNS}")
    if not RISK_MODEL.exists():
        fail(f"Missing risk model: {RISK_MODEL}")
    if not SEC_MASTER.exists():
        fail(f"Missing securities master: {SEC_MASTER}")

    er = pd.read_parquet(EXPECTED_RETURNS)
    rm = pd.read_parquet(RISK_MODEL)
    sm = pd.read_parquet(SEC_MASTER)

    return er, rm, sm


def get_regime_params(regime: str):
    """
    Regime-aware controls:
        - gross_leverage: cap on sum(|w|)
        - net_exposure_bounds: (min, max) on sum(w)
        - lambda_risk: risk aversion
        - gamma_turnover: turnover penalty
    """
    regime = str(regime).lower()

    if regime in ["bull", "expansion"]:
        return {
            "gross_leverage": 1.8,
            "net_exposure_bounds": (0.20, 0.40),
            "lambda_risk": 5.0,
            "gamma_turnover": 0.5,
        }
    elif regime in ["bear", "contraction"]:
        return {
            "gross_leverage": 1.2,
            "net_exposure_bounds": (-0.10, 0.10),
            "lambda_risk": 10.0,
            "gamma_turnover": 1.0,
        }
    elif regime in ["high_vol", "stress"]:
        return {
            "gross_leverage": 1.0,
            "net_exposure_bounds": (-0.05, 0.05),
            "lambda_risk": 12.0,
            "gamma_turnover": 1.2,
        }
    else:  # neutral / default
        return {
            "gross_leverage": 1.5,
            "net_exposure_bounds": (0.10, 0.25),
            "lambda_risk": 7.0,
            "gamma_turnover": 0.7,
        }


def build_covariance(rm: pd.DataFrame, tickers: list[str]) -> np.ndarray:
    """
    Expect risk_model.parquet to contain:
        - ticker
        - factor_1, factor_2, ...
        - idio_var
        - factor_cov_* (flattened or separate table)
    For Quant v1.0, we assume a simple diagonal covariance from idio_var
    plus an optional scalar factor variance if present.
    """
    rm = rm.copy()
    rm["ticker"] = rm["ticker"].astype(str).str.upper()
    rm = rm[rm["ticker"].isin(tickers)].set_index("ticker")

    if "idio_var" not in rm.columns:
        fail("risk_model.parquet missing 'idio_var' column")

    idio = rm.loc[tickers, "idio_var"].values
    cov = np.diag(idio)

    return cov


def optimise_one_date(date, df_date, cov, params, prev_weights=None):
    """
    Solve:
        max_w   mu^T w - λ w^T Σ w - γ ||w - w_prev||_1
        s.t.    sum(|w|) <= gross_leverage
                net_min <= sum(w) <= net_max
                |w_i| <= 0.05 (5% per name cap)
    """
    tickers = df_date["ticker"].tolist()
    mu = df_date["expected_return"].values

    n = len(tickers)
    if n == 0:
        return None

    w = cp.Variable(n)

    gross_leverage = params["gross_leverage"]
    net_min, net_max = params["net_exposure_bounds"]
    lambda_risk = params["lambda_risk"]
    gamma_turnover = params["gamma_turnover"]

    if prev_weights is None or len(prev_weights) != n:
        prev_weights = np.zeros(n)

    risk_term = cp.quad_form(w, cov)
    turnover_term = cp.norm1(w - prev_weights)

    objective = cp.Maximize(mu @ w - lambda_risk * risk_term - gamma_turnover * turnover_term)

    constraints = []
    constraints.append(cp.norm1(w) <= gross_leverage)
    constraints.append(cp.sum(w) >= net_min)
    constraints.append(cp.sum(w) <= net_max)
    constraints.append(w <= 0.05)
    constraints.append(w >= -0.05)

    prob = cp.Problem(objective, constraints)
    try:
        prob.solve(solver=cp.OSQP, verbose=False)
    except Exception as e:
        print(f"[{date}] Optimisation error: {e}")
        return None

    if w.value is None:
        print(f"[{date}] Optimisation failed to find a solution.")
        return None

    weights = np.array(w.value).flatten()
    return pd.DataFrame(
        {
            "date": [date] * n,
            "ticker": tickers,
            "weight": weights,
        }
    )


def main():
    print("\n=== BUILDING REGIME-AWARE OPTIMISER (Quant v1.0) ===\n")

    er, rm, sm = load_inputs()

    # Normalise columns
    er["ticker"] = er["ticker"].astype(str).str.upper()
    sm["ticker"] = sm["ticker"].astype(str).str.upper()

    required_er = {"date", "ticker", "expected_return", "regime"}
    missing = required_er - set(er.columns)
    if missing:
        fail(f"expected_returns_quant_v1.parquet missing required columns: {missing}")

    # Sort by date for stable turnover handling
    er["date"] = pd.to_datetime(er["date"])
    er = er.sort_values(["date", "ticker"])

    all_dates = er["date"].drop_duplicates().tolist()
    results = []
    prev_weights_map = {}

    for d in all_dates:
        df_d = er[er["date"] == d].copy()
        regime = df_d["regime"].iloc[0]
        params = get_regime_params(regime)

        tickers = df_d["ticker"].tolist()
        cov = build_covariance(rm, tickers)

        prev_w = prev_weights_map.get(d, None)

        print(f"[{d.date()}] Optimising {len(tickers)} names under regime '{regime}' with params {params}...")
        df_w = optimise_one_date(d, df_d, cov, params, prev_weights=prev_w)

        if df_w is None:
            print(f"[{d.date()}] WARNING: optimisation failed, skipping date.")
            continue

        prev_weights_map[d] = df_w["weight"].values
        df_w["regime"] = regime
        df_w["gross_leverage_target"] = params["gross_leverage"]
        df_w["net_exposure_min"] = params["net_exposure_bounds"][0]
        df_w["net_exposure_max"] = params["net_exposure_bounds"][1]
        df_w["lambda_risk"] = params["lambda_risk"]
        df_w["gamma_turnover"] = params["gamma_turnover"]

        results.append(df_w)

    if not results:
        fail("No optimisation results produced.")

    final = pd.concat(results, ignore_index=True)

    OUT.parent.mkdir(parents=True, exist_ok=True)
    final.to_parquet(OUT, index=False)

    ok(f"Wrote optimiser_regime_quant_v1.parquet → {OUT}")
    print("\n🎉 Regime-aware optimiser layer built successfully.\n")


if __name__ == "__main__":
    main()