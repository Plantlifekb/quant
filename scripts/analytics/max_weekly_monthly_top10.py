import pandas as pd
from pathlib import Path

REALIZED = Path(r"C:\Quant\data\analytics\realized_returns.parquet")

def load_realized():
    df = pd.read_parquet(REALIZED)
    df["date"] = pd.to_datetime(df["date"])
    df["week_start"] = df["date"] - pd.to_timedelta(df["date"].dt.weekday, unit="D")
    return df

def compute_max_top_bottom(realized, N=10):
    weekly = realized.groupby(["week_start", "ticker"])["realized_return"].sum().reset_index()

    def top_n(group):
        return group.nlargest(N, "realized_return")["realized_return"].mean()

    def bottom_n(group):
        return group.nsmallest(N, "realized_return")["realized_return"].mean()

    max_long = (
        weekly.groupby("week_start")
        .apply(top_n)
        .rename("weekly_return")
        .reset_index()
    )
    max_long["strategy"] = f"MAX_LONG_{N}"

    max_short = (
        weekly.groupby("week_start")
        .apply(bottom_n)
        .rename("weekly_return")
        .reset_index()
    )
    max_short["weekly_return"] = -max_short["weekly_return"]
    max_short["strategy"] = f"MAX_SHORT_{N}"

    max_ls = max_long.copy()
    max_ls["weekly_return"] = max_long["weekly_return"] + max_short["weekly_return"]
    max_ls["strategy"] = f"MAX_LONG_SHORT_{N}"

    out = pd.concat([max_long, max_short, max_ls], ignore_index=True)
    out = out.sort_values(["strategy", "week_start"])

    # 4‑week rolling average of weekly returns (your “Weekly (4‑week avg)”)
    out["weekly_4w_avg"] = (
        out.groupby("strategy")["weekly_return"]
        .rolling(4)
        .mean()
        .reset_index(level=0, drop=True)
    )

    # Dashboard‑style “Monthly”: convert that 4‑week avg to an equivalent monthly return
    out["monthly_equiv"] = (1 + out["weekly_4w_avg"])**4 - 1

    return out

def main():
    realized = load_realized()
    out = compute_max_top_bottom(realized, N=10)

    print("\n=== MAX WEEKLY (4W AVG) & MONTHLY (EQUIV) RETURNS (TOP/BOTTOM 10) ===")
    print(
        out.groupby("strategy")[["weekly_4w_avg", "monthly_equiv"]]
        .max()
    )

if __name__ == "__main__":
    main()