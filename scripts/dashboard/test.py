# inspect_perf.py
from dashboard_app import compute_performance
import pandas as pd

# compute weekly perf and persist
perf = compute_performance(freq="W", persist=True)

# summary
suspicious_total = int(perf["suspicious_count"].sum()) if "suspicious_count" in perf.columns else 0
print("suspicious_count:", suspicious_total)

# show flagged rows (first 20)
if "flag_suspicious" in perf.columns:
    flagged = perf[perf["flag_suspicious"]]
    if not flagged.empty:
        print("\nFlagged periods (sample):")
        print(flagged[["period_return","period_return_pct","flag_suspicious","unit_decision"]].head(20).to_string(index=False))
    else:
        print("No flagged periods.")
else:
    print("No flag_suspicious column in perf.")