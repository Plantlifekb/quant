import pandas as pd
base = r"C:\Quant\data\analytics"
perf = pd.read_parquet(base + r"\perf_weekly.parquet")
merged = pd.read_parquet(base + r"\merged_attribution.parquet")

# 1) Inspect perf columns and types
print("perf columns:", perf.columns.tolist())
print(perf.dtypes)

# 2) Summary of suspicious flags and values
if "flag_suspicious" in perf.columns:
    print("flag_suspicious count (rows flagged):", int(perf["flag_suspicious"].sum()))
else:
    print("No boolean flag_suspicious column found")

print("suspicious_count column summary:")
print(perf["suspicious_count"].describe())

# 3) Show top extreme weekly period_return values
extreme = perf.sort_values("period_return", ascending=False).head(30)
print("Top 30 period_return extremes:\n", extreme[["date","period_return","period_return_pct","suspicious_count"]].to_string(index=False))

# 4) For the top 10 extreme weeks, show merged contributors and check duplicates per week
top_dates = pd.to_datetime(extreme["date"]).dt.date.tolist()[:10]
for d in top_dates:
    start = pd.to_datetime(d)
    week_rows = merged[(merged["date"] >= start) & (merged["date"] < start + pd.Timedelta(days=7))].copy()
    print("\n=== Week starting", start.date(), "rows:", len(week_rows))
    print("duplicates in week (date,ticker):", int(week_rows.duplicated(subset=["date","ticker"]).sum()))
    print("realized_return stats:", week_rows["realized_return"].describe().to_dict())
    print("weights stats:", week_rows["_weight_used"].describe().to_dict())
    top = week_rows.assign(abs_contrib=week_rows["contrib_total"].abs()).sort_values("abs_contrib", ascending=False).head(20)
    print("Top contributors:\n", top[["ticker","_weight_used","realized_return","contrib_total"]].to_string(index=False))