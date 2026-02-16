"""
risk_engine_v1.py
Quant v1.1 — Risk Engine
------------------------------------------------------------

Consumes:
    - C:\Quant\data\analytics\summary\quant_summary_v1.csv
    - C:\Quant\data\analytics\attribution_outputs_v1\attribution_rolling.csv

Produces:
    - C:\Quant\data\analytics\risk\quant_risk_daily_v1.csv
    - C:\Quant\data\analytics\risk\quant_risk_model_v1.csv
"""

import pandas as pd
from pathlib import Path
from datetime import datetime, timezone
import logging
import numpy as np

# ------------------------------------------------------------
# Logging
# ------------------------------------------------------------
LOG_DIR = Path(r"C:\Quant\logs\risk_engine")
LOG_DIR.mkdir(parents=True, exist_ok=True)

logging.basicConfig(
    filename=LOG_DIR / "risk_engine_v1.log",
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger("risk_engine_v1")


def iso_now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")


# ------------------------------------------------------------
# Input paths
# ------------------------------------------------------------
SUMMARY_FILE = Path(r"C:\Quant\data\analytics\summary\quant_summary_v1.csv")
ATTR_ROLLING_FILE = Path(
    r"C:\Quant\data\analytics\attribution_outputs_v1\attribution_rolling.csv"
)

# ------------------------------------------------------------
# Output paths
# ------------------------------------------------------------
RISK_DIR = Path(r"C:\Quant\data\analytics\risk")
RISK_DIR.mkdir(parents=True, exist_ok=True)

RISK_DAILY_FILE = RISK_DIR / "quant_risk_daily_v1.csv"
RISK_MODEL_FILE = RISK_DIR / "quant_risk_model_v1.csv"


# ------------------------------------------------------------
# Helpers
# ------------------------------------------------------------
def load_csv(path: Path) -> pd.DataFrame:
    if not path.exists():
        logger.warning(f"Missing input file: {path}")
        return pd.DataFrame()
    try:
        return pd.read_csv(path)
    except Exception as e:
        logger.error(f"Failed to load {path}: {e}")
        return pd.DataFrame()


def safe_var(series: pd.Series) -> float:
    s = series.dropna()
    if s.empty:
        return np.nan
    return float(s.var())


def safe_cov(x: pd.Series, y: pd.Series) -> float:
    df = pd.concat([x, y], axis=1).dropna()
    if df.empty:
        return np.nan
    return float(df.cov().iloc[0, 1])


# ------------------------------------------------------------
# Rolling beta (stable, no rolling.apply)
# ------------------------------------------------------------
def compute_rolling_beta(df: pd.DataFrame, window: int = 21) -> pd.Series:
    """
    Computes rolling beta using a stable expanding-window slice per index.
    Avoids rolling.apply entirely.
    """

    betas = []
    ret = df["return_close_to_close"]
    fac = df["predicted_factor_return"]

    for i in range(len(df)):
        start = max(0, i - window + 1)
        window_ret = ret.iloc[start:i+1]
        window_fac = fac.iloc[start:i+1]

        if len(window_ret.dropna()) < 5:
            betas.append(np.nan)
            continue

        cov = safe_cov(window_ret, window_fac)
        var_f = safe_var(window_fac)

        if var_f in (0, np.nan):
            betas.append(np.nan)
        else:
            betas.append(cov / var_f)

    return pd.Series(betas, index=df.index)


# ------------------------------------------------------------
# Main
# ------------------------------------------------------------
def main():
    logger.info("------------------------------------------------------------")
    logger.info("Starting Risk Engine v1.1")
    logger.info(f"Run timestamp: {iso_now()}")
    logger.info("------------------------------------------------------------")

    df_summary = load_csv(SUMMARY_FILE)
    df_roll = load_csv(ATTR_ROLLING_FILE)

    if df_roll.empty:
        logger.warning("Rolling attribution file missing — aborting risk engine.")
        return

    # Normalize date
    df_roll["date"] = pd.to_datetime(df_roll["date"], errors="coerce").dt.date

    # --------------------------------------------------------
    # 1) FULL ROLLING RISK MODEL
    # --------------------------------------------------------
    df_model = df_roll.copy()

    # Rolling volatility
    if "net_contribution_rolling_21d" in df_model.columns:
        df_model["rolling_volatility"] = (
            df_model["net_contribution_rolling_21d"]
            .rolling(window=21, min_periods=5)
            .std()
        )
    else:
        df_model["rolling_volatility"] = (
            df_model["return_close_to_close"]
            .rolling(window=21, min_periods=5)
            .std()
        )

    # Rolling factor risk
    df_model["rolling_factor_risk"] = (
        df_model["predicted_factor_return"]
        .rolling(window=21, min_periods=5)
        .var()
    )

    # Rolling beta (stable)
    df_model["rolling_beta"] = compute_rolling_beta(df_model, window=21)

    # Liquidity / turnover pressure
    df_model["rolling_turnover_pressure"] = df_model.get("turnover_pressure", np.nan)
    df_model["rolling_liquidity_pressure"] = df_model.get("liquidity_score", np.nan)

    df_model["regime_label"] = df_model.get("regime_label", "unknown")

    df_model.insert(0, "SECTION", "RISK_MODEL")
    df_model.to_csv(RISK_MODEL_FILE, index=False)
    logger.info(f"Full rolling risk model written to {RISK_MODEL_FILE}")

    # --------------------------------------------------------
    # 2) DAILY RISK SUMMARY
    # --------------------------------------------------------
    rows = []
    for dt, g in df_roll.groupby("date"):

        daily_vol = g["return_close_to_close"].std()

        factor_var = safe_var(g["predicted_factor_return"])
        factor_risk = np.sqrt(factor_var) if not np.isnan(factor_var) else np.nan

        cov_rf = safe_cov(g["return_close_to_close"], g["predicted_factor_return"])
        var_f = safe_var(g["predicted_factor_return"])
        beta = cov_rf / var_f if var_f not in (0, np.nan) else np.nan

        liquidity_risk = g.get("liquidity_cost_rolling_21d", g.get("cost_total", np.nan)).sum()
        turnover_risk = g.get("turnover_cost_rolling_21d", np.nan).sum()

        regime_risk = np.nan
        if "regime_label" in g.columns and "net_contribution_rolling_21d" in g.columns:
            regime_risk = g.groupby("regime_label")["net_contribution_rolling_21d"].var().sum()

        rows.append(
            {
                "SECTION": "RISK_DAILY",
                "date": dt,
                "daily_volatility": daily_vol,
                "factor_risk": factor_risk,
                "beta": beta,
                "liquidity_risk": liquidity_risk,
                "turnover_risk": turnover_risk,
                "regime_risk": regime_risk,
            }
        )

    df_daily = pd.DataFrame(rows)
    df_daily.to_csv(RISK_DAILY_FILE, index=False)
    logger.info(f"Daily risk summary written to {RISK_DAILY_FILE}")

    logger.info("Risk Engine v1.1 completed.")
    logger.info("------------------------------------------------------------")


if __name__ == "__main__":
    main()