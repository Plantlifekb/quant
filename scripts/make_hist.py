import os, sys
import pandas as pd
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

csv = r"C:\Quant\analysis\regenerated_realized_longonly.csv"
out = r"C:\Quant\analysis\realized_return_hist_longonly.png"

if not os.path.exists(csv):
    print("ERROR: CSV not found:", csv)
    sys.exit(4)

df = pd.read_csv(csv)
if "realized_return" not in df.columns:
    print("ERROR: column realized_return missing in CSV")
    sys.exit(5)

vals = df["realized_return"].dropna()
plt.figure(figsize=(8,4))
vals.hist(bins=80)
plt.title("Realized return distribution longonly")
plt.xlabel("weekly return")
plt.ylabel("count")
plt.tight_layout()
plt.savefig(out, dpi=150)
print("SAVED", out)
