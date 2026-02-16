import pandas as pd
from pathlib import Path

REALIZED = Path(r"C:\Quant\data\analytics\realized_returns.parquet")

def load_realized():
    df = pd.read_parquet(REALIZED)
    df["date"] = pd.to_datetime(df["date"])
    df["week_start"] = df["date"] - pd.to_timedelta(df["date"].dt.weekday, unit="D")
    return df

def main():
    df = load_realized()

    # Collapse to weekly returns per ticker
    weekly = (
        df.groupby(["week_start", "ticker"])["realized_return"]
        .sum()
        .reset_index()
    )

    # For each week, show top 10 and bottom 10
    all_weeks = sorted(weekly["week_start"].unique())

    print("\n=== TOP/BOTTOM 10 WEEKLY RETURNS (PER WEEK) ===\n")

    for week in all_weeks:
        w = weekly[weekly["week_start"] == week].copy()

        top10 = w.nlargest(10, "realized_return")
        bottom10 = w.nsmallest(10, "realized_return")

        print(f"Week: {week.date()}")
        print("\nTop 10 (best performers):")
        print(top10[["ticker", "realized_return"]].to_string(index=False))

        print("\nBottom 10 (worst performers):")
        print(bottom10[["ticker", "realized_return"]].to_string(index=False))

        print("\nAverages:")
        print(f"  MAX_LONG_10 weekly avg:  {top10['realized_return'].mean():.4f}")
        print(f"  MAX_SHORT_10 weekly avg: {-bottom10['realized_return'].mean():.4f}")
        print(f"  MAX_LS_10 weekly avg:    {top10['realized_return'].mean() - bottom10['realized_return'].mean():.4f}")

        print("\n" + "-"*80 + "\n")

if __name__ == "__main__":
    main()