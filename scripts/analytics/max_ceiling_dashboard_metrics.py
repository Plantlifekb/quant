import pandas as pd
from pathlib import Path

REALIZED = Path(r"C:\Quant\data\analytics\realized_returns.parquet")

def load_realized():
    df = pd.read_parquet(REALIZED)
    df["date"] = pd.to_datetime(df["date"])
    df["week_start"] = df["date"] - pd.to_timedelta(df["date"].dt.weekday, unit="D")
    return df

def compute_weekly_returns(df):
    return (
        df.groupby(["week_start", "ticker"])["realized_return"]
        .sum()
        .reset_index()
    )

def main():
    df = load_realized()
    weekly = compute_weekly_returns(df)

    # Compute top10 and bottom10 averages for each week
    def top10_avg(g):
        return g.nlargest(10, "realized_return")["realized_return"].mean()

    def bottom10_avg(g):
        return g.nsmallest(10, "realized_return")["realized_return"].mean()

    weekly_ceiling = weekly.groupby("week_start").apply(
        lambda g: pd.Series({
            "max_L_10": top10_avg(g),
            "max_S_10": -bottom10_avg(g),
            "max_LS_10": top10_avg(g) - bottom10_avg(g)
        })
    ).reset_index()

    # Identify the weeks that produced the maxima
    week_max_L_10 = weekly_ceiling.loc[weekly_ceiling["max_L_10"].idxmax(), "week_start"]
    week_max_LS_10 = weekly_ceiling.loc[weekly_ceiling["max_LS_10"].idxmax(), "week_start"]

    # Extract the actual tickers for those weeks
    week_L = weekly[weekly["week_start"] == week_max_L_10].copy()
    week_LS = weekly[weekly["week_start"] == week_max_LS_10].copy()

    L_top_picks = week_L.nlargest(10, "realized_return")[["ticker", "realized_return"]]
    LS_top_longs = week_LS.nlargest(10, "realized_return")[["ticker", "realized_return"]]
    LS_top_shorts = week_LS.nsmallest(10, "realized_return")[["ticker", "realized_return"]]

    print("\n=== MAX CEILING METRICS FOR DASHBOARD ===\n")

    print(">> max_L_10 (best long-only week):")
    print(weekly_ceiling.loc[weekly_ceiling["max_L_10"].idxmax()])
    print("\nTop 10 longs for that week:")
    print(L_top_picks.to_string(index=False))

    print("\n>> max_LS_10 (best long–short week):")
    print(weekly_ceiling.loc[weekly_ceiling["max_LS_10"].idxmax()])
    print("\nTop 10 longs for that week:")
    print(LS_top_longs.to_string(index=False))
    print("\nBottom 10 shorts for that week:")
    print(LS_top_shorts.to_string(index=False))

if __name__ == "__main__":
    main()