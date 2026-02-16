"""
Quant v2.0 — Alpha-driven Factor-based Portfolio Engine (soft-neutral)

Objective:
    Maximise ex-ante Sharpe proxy using:
        • alpha_z as expected returns (from alpha engine)
        • factor risk model for covariance and penalties

Uses:
    • Alpha:             C:\Quant\data\signals\alpha_quant_v2.parquet
    • Factor exposures:  C:\Quant\data\risk\factor_exposures_quant_v2.parquet
    • Factor returns:    C:\Quant\data\risk\factor_returns_quant_v2.parquet
    • Factor covariance: C:\Quant\data\risk\factor_covariance_quant_v2.parquet
    • Universe & prices: C:\Quant\data\ingestion\fundamentals.parquet

Features:
    • Alpha-driven objective (alpha_z)
    • Factor-based risk (B, Sigma_f → Sigma_a)
    • SOFT sector & industry neutrality (penalised)
    • SOFT factor risk control (penalised)
    • Long-only, sum-to-one
"""

from pathlib import Path
import numpy as np
import pandas as pd
from scipy.optimize import minimize

BASE = Path(r"C:\Quant")

FUNDAMENTALS = BASE / "data" / "ingestion" / "fundamentals.parquet"
FACTOR_EXPOSURES = BASE / "data" / "risk" / "factor_exposures_quant_v2.parquet"
FACTOR_RETURNS = BASE / "data" / "risk" / "factor_returns_quant_v2.parquet"
FACTOR_COV = BASE / "data" / "risk" / "factor_covariance_quant_v2.parquet"
ALPHA_PATH = BASE / "data" / "signals" / "alpha_quant_v2.parquet"

PORTFOLIO_OUT = BASE / "data" / "analytics" / "portfolio_quant_v2.parquet"

# Risk aversion parameter for mean-variance proxy to Sharpe
RISK_AVERSION = 5.0

# Soft penalty strengths
SECTOR_NEUTRALITY_PENALTY = 10.0
INDUSTRY_NEUTRALITY_PENALTY = 5.0
FACTOR_RISK_PENALTY = 50.0

# Optional style neutrality ( *_neutral factors )
ENABLE_STYLE_NEUTRALITY = False
STYLE_NEUTRALITY_PENALTY = 2.0


def _load_latest_universe():
    if not FUNDAMENTALS.exists():
        raise FileNotFoundError(f"Missing fundamentals file: {FUNDAMENTALS}")

    fnd = pd.read_parquet(FUNDAMENTALS)
    fnd["date"] = pd.to_datetime(fnd["date"])
    fnd["ticker"] = fnd["ticker"].astype(str).str.upper()

    latest_date = fnd["date"].max()
    universe = fnd[fnd["date"] == latest_date].copy()

    if universe.empty:
        raise RuntimeError("No fundamentals for latest date.")

    return latest_date, universe


def _load_latest_factor_data(latest_date: pd.Timestamp):
    # Exposures
    if not FACTOR_EXPOSURES.exists():
        raise FileNotFoundError(f"Missing factor exposures file: {FACTOR_EXPOSURES}")
    exp = pd.read_parquet(FACTOR_EXPOSURES)
    exp["date"] = pd.to_datetime(exp["date"])
    exp["ticker"] = exp["ticker"].astype(str).str.upper()
    exp = exp[exp["date"] == latest_date].copy()
    if exp.empty:
        raise RuntimeError(f"No factor exposures for latest date {latest_date.date()}")

    # Factor returns: use most recent available date <= latest_date
    if not FACTOR_RETURNS.exists():
        raise FileNotFoundError(f"Missing factor returns file: {FACTOR_RETURNS}")
    fr = pd.read_parquet(FACTOR_RETURNS)
    fr["date"] = pd.to_datetime(fr["date"])
    fr = fr[fr["date"] <= latest_date].copy()
    if fr.empty:
        raise RuntimeError("No factor returns available up to latest date.")
    fr = fr.sort_values("date").iloc[-1]  # last row
    factor_return_date = fr["date"]
    factor_return_series = fr.drop(labels=["date"])

    # Factor covariance: use most recent available date <= latest_date
    if not FACTOR_COV.exists():
        raise FileNotFoundError(f"Missing factor covariance file: {FACTOR_COV}")
    cov = pd.read_parquet(FACTOR_COV)
    cov["date"] = pd.to_datetime(cov["date"])
    cov = cov[cov["date"] <= latest_date].copy()
    if cov.empty:
        raise RuntimeError("No factor covariance available up to latest date.")
    cov = cov.sort_values("date").iloc[-1]  # last row
    cov_date = cov["date"]
    cov_series = cov.drop(labels=["date"])

    return exp, factor_return_series, factor_return_date, cov_series, cov_date


def _load_latest_alpha(latest_date: pd.Timestamp):
    if not ALPHA_PATH.exists():
        raise FileNotFoundError(f"Missing alpha file: {ALPHA_PATH}")

    alpha = pd.read_parquet(ALPHA_PATH)
    alpha["date"] = pd.to_datetime(alpha["date"])
    alpha["ticker"] = alpha["ticker"].astype(str).str.upper()

    alpha_latest = alpha[alpha["date"] == latest_date].copy()
    if alpha_latest.empty:
        raise RuntimeError(f"No alpha for latest date {latest_date.date()}")

    return alpha_latest[["ticker", "alpha_z"]].rename(columns={"alpha_z": "alpha"})


def _build_factor_matrices(exp: pd.DataFrame, factor_return_series: pd.Series, cov_series: pd.Series):
    # Factor list from factor returns
    factor_names = factor_return_series.index.tolist()

    # Build asset-by-factor exposure matrix B (N x K)
    missing_factors = [f for f in factor_names if f not in exp.columns]
    if missing_factors:
        raise RuntimeError(f"Exposures missing factors: {missing_factors}")

    tickers = exp["ticker"].tolist()
    B = exp[factor_names].astype(float).values  # N x K

    # Factor covariance matrix (K x K)
    K = len(factor_names)
    Sigma_f = np.zeros((K, K), dtype=float)
    for i, fi in enumerate(factor_names):
        for j, fj in enumerate(factor_names):
            key = f"cov_{fi}_{fj}"
            if key not in cov_series.index:
                raise RuntimeError(f"Missing covariance entry: {key}")
            Sigma_f[i, j] = float(cov_series[key])

    return tickers, factor_names, B, Sigma_f


def _optimise_weights(mu_a: np.ndarray, Sigma_a: np.ndarray, B: np.ndarray, factor_names: list):
    """
    Mean-variance proxy to Sharpe with SOFT factor controls:

        max_w [ w' mu_a - λ * w' Σ_a w
                - α * ||sector_exposure||^2
                - β * ||industry_exposure||^2
                - γ * factor_risk ]

    subject to:
        sum(w) = 1
        w >= 0
    """
    n = len(mu_a)

    # Indices for factor groups
    sector_idx = [i for i, f in enumerate(factor_names) if f.startswith("SEC_")]
    industry_idx = [i for i, f in enumerate(factor_names) if f.startswith("IND_")]
    style_idx = [i for i, f in enumerate(factor_names) if f.endswith("_neutral")]

    def objective(w):
        # Base mean-variance term
        ret = w @ mu_a
        risk = w @ Sigma_a @ w
        obj = ret - RISK_AVERSION * risk

        # Factor exposures: f = w' B  (K,)
        f = w @ B  # shape (K,)

        # Sector exposure penalty
        if sector_idx:
            sec_exp = f[sector_idx]
            obj -= SECTOR_NEUTRALITY_PENALTY * float(sec_exp @ sec_exp)

        # Industry exposure penalty
        if industry_idx:
            ind_exp = f[industry_idx]
            obj -= INDUSTRY_NEUTRALITY_PENALTY * float(ind_exp @ ind_exp)

        # Optional style neutrality penalty
        if ENABLE_STYLE_NEUTRALITY and style_idx:
            sty_exp = f[style_idx]
            obj -= STYLE_NEUTRALITY_PENALTY * float(sty_exp @ sty_exp)

        # Factor risk penalty (overall variance)
        obj -= FACTOR_RISK_PENALTY * float(risk)

        # We minimise, so return negative of objective
        return -obj

    # Constraint: sum(w) = 1
    cons = ({
        "type": "eq",
        "fun": lambda w: np.sum(w) - 1.0,
    },)

    # Bounds: long-only [0, 1]
    bounds = tuple((0.0, 1.0) for _ in range(n))

    w0 = np.full(n, 1.0 / n)

    res = minimize(
        objective,
        w0,
        method="SLSQP",
        bounds=bounds,
        constraints=cons,
        options={"maxiter": 1000, "ftol": 1e-9},
    )

    if not res.success:
        raise RuntimeError(f"Optimisation failed: {res.message}")

    return res.x


def main():
    print("\n=== BUILDING PORTFOLIO (Quant v2.0, alpha-driven, factor-risk) ===\n")

    latest_date, universe = _load_latest_universe()
    print(f"• Latest universe date: {latest_date.date()}, names: {len(universe)}")

    exp, fr_series, fr_date, cov_series, cov_date = _load_latest_factor_data(latest_date)
    print(f"• Using factor returns as of: {fr_date.date()}")
    print(f"• Using factor covariance as of: {cov_date.date()}")

    alpha_latest = _load_latest_alpha(latest_date)
    print("• Loaded alpha for latest date")

    # Align universe, exposures, and alpha
    merged = (
        universe.merge(exp, on=["date", "ticker"], how="inner", suffixes=("", "_exp"))
                .merge(alpha_latest, on="ticker", how="inner")
    )

    if merged.empty:
        raise RuntimeError("No overlap between fundamentals, factor exposures, and alpha.")

    print(f"• Overlapping universe size (with alpha): {len(merged)}")

    tickers, factor_names, B, Sigma_f = _build_factor_matrices(
        merged, fr_series, cov_series
    )

    # Asset expected returns: use alpha as μ_a
    mu_a = merged["alpha"].astype(float).values  # N,

    # Asset covariance: Sigma_a = B * Sigma_f * B'
    Sigma_a = B @ Sigma_f @ B.T  # N x N

    # Optimise weights with soft factor controls
    print("• Optimising portfolio weights (alpha-driven, soft factor-neutral, risk-penalised) ...")
    w_opt = _optimise_weights(mu_a, Sigma_a, B, factor_names)

    # Build output
    out = pd.DataFrame(
        {
            "date": latest_date,
            "ticker": tickers,
            "weight": w_opt,
        }
    ).sort_values("weight", ascending=False).reset_index(drop=True)

    # Diagnostics
    port_ret = float(w_opt @ mu_a)
    port_var = float(w_opt @ Sigma_a @ w_opt)
    port_vol = float(np.sqrt(max(port_var, 0.0)))
    port_sharpe = port_ret / port_vol if port_vol > 0 else np.nan

    # Factor exposures of final portfolio
    f_port = w_opt @ B  # K,
    sector_idx = [i for i, f in enumerate(factor_names) if f.startswith("SEC_")]
    industry_idx = [i for i, f in enumerate(factor_names) if f.startswith("IND_")]

    print(f"\n• Portfolio expected alpha:                {port_ret:.6f}")
    print(f"• Portfolio volatility (factor-based):     {port_vol:.6f}")
    print(f"• Portfolio Sharpe (alpha / vol):         {port_sharpe:.3f}")

    if sector_idx:
        sec_exp = f_port[sector_idx]
        print(f"• Sector exposure L2 norm:                {np.linalg.norm(sec_exp):.6f}")
    if industry_idx:
        ind_exp = f_port[industry_idx]
        print(f"• Industry exposure L2 norm:              {np.linalg.norm(ind_exp):.6f}")

    PORTFOLIO_OUT.parent.mkdir(parents=True, exist_ok=True)
    out.to_parquet(PORTFOLIO_OUT, index=False)

    print(f"\n✔ Wrote portfolio to: {PORTFOLIO_OUT}")
    print("\n🎉 Portfolio Engine (Quant v2.0, alpha-driven, factor-risk) completed successfully.\n")


if __name__ == "__main__":
    main()