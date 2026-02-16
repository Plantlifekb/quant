import pandas as pd
import numpy as np

WEIGHTS_IN = r"C:\Quant\data\analytics\quant_portfolio_weights_tradable_v1_osqp.csv"
REGIME_PATH = r"C:\Quant\data\analytics\quant_regime_states_v1.csv"
WEIGHTS_OUT = r"C:\Quant\data\analytics\quant_portfolio_weights_tradable_v1_osqp_regime.csv"

EXPOSURE_MAP = {
    "risk_on": 1.00,
    "cautious": 0.60,
    "risk_off": 0.20,
    "stress": 0.00,
    "unknown": 1.00,
}

w = pd.read_csv(WEIGHTS_IN, parse_dates=["date"])
reg = pd.read_csv(REGIME_PATH, parse_dates=["date"])

w["date"] = pd.to_datetime(w["date"]).dt.tz_localize(None)
reg["date"] = pd.to_datetime(reg["date"]).dt.tz_localize(None)

reg = reg.rename(columns={"regime_label": "regime"})
reg = reg.sort_values("date").reset_index(drop=True)
w = w.sort_values("date").reset_index(drop=True)

w_reg = pd.merge_asof(
    w.sort_values("date"),
    reg[["date", "regime"]].sort_values("date"),
    on="date",
    direction="backward",
)

w_reg["regime"] = w_reg["regime"].fillna("unknown")
w_reg["exposure"] = w_reg["regime"].map(EXPOSURE_MAP).fillna(1.0)

# scale weights by exposure, preserve sign
w_reg["weight_regime"] = w_reg["weight"] * w_reg["exposure"]

# optional: renormalise within each date to keep gross = exposure
def renorm(group):
    gross = group["weight_regime"].abs().sum()
    if gross > 0:
        group["weight_regime"] = group["weight_regime"] * group["exposure"].iloc[0] / gross
    return group

w_reg = w_reg.groupby("date", group_keys=False).apply(renorm)

w_reg.to_csv(WEIGHTS_OUT, index=False)
print("Written:", WEIGHTS_OUT)