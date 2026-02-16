#!/usr/bin/env python3
import sys
from pathlib import Path
import pandas as pd

base = Path(r"C:\Quant\data\analytics")
try:
    m = pd.read_parquet(base / "merged_attribution.parquet")
    p = pd.read_parquet(base / "perf_weekly.parquet")
except Exception as e:
    print("CI: failed to read artifacts:", e)
    sys.exit(2)

dups = m.duplicated(subset=['date','ticker']).sum()
print("CI: merged duplicates:", dups)
if dups != 0:
    print("CI FAIL: duplicates in merged_attribution")
    sys.exit(1)

m['_recomputed_contrib'] = m['_weight_used'].astype(float) * m['realized_return'].astype(float)
diff = (m['contrib_total'] - m['_recomputed_contrib']).abs().max()
print("CI: max contrib diff:", diff)
if diff > 1e-8:
    print("CI FAIL: contrib_total mismatch")
    sys.exit(1)

max_pct = p['period_return_pct'].abs().max()
print("CI: max period_return_pct:", max_pct)
if max_pct > 10000.0:
    print("CI FAIL: extreme period_return_pct")
    sys.exit(1)

print("CI: all checks passed")
sys.exit(0)