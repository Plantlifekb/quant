import json, sys
from pathlib import Path
import importlib.util
import pandas as pd

ROOT = Path(r"C:\Quant")
REALIZED = ROOT / "analysis" / "realized_weekly_from_ingest.csv"
MODULE = Path(r"C:\Quant\scripts\regen_and_verify.py")

out = {"status":"failed","artifacts":[],"issues":[],"message":""}

if not MODULE.exists():
    out["issues"].append("regen_and_verify.py missing")
    print(json.dumps(out)); sys.exit(1)
if not REALIZED.exists():
    out["issues"].append("realized file missing")
    print(json.dumps(out)); sys.exit(1)

spec = importlib.util.spec_from_file_location("regen_mod", str(MODULE))
mod = importlib.util.module_from_spec(spec)
spec.loader.exec_module(mod)

realized = pd.read_csv(REALIZED)
if "week" not in realized.columns and "date" in realized.columns:
    realized["week"] = pd.to_datetime(realized["date"], errors="coerce").dt.normalize()
elif "week" in realized.columns:
    realized["week"] = pd.to_datetime(realized["week"], errors="coerce").dt.normalize()
else:
    out["issues"].append("realized missing week/date")
    print(json.dumps(out)); sys.exit(1)

# ensure realized_return exists
if "realized_return" not in realized.columns:
    if "close_end" in realized.columns and "close_start" in realized.columns:
        realized["realized_return"] = realized["close_end"] / realized["close_start"] - 1.0
    else:
        out["issues"].append("realized_return missing and cannot be computed")
        print(json.dumps(out)); sys.exit(1)

artifacts = []
runs = []
for strat in ("longonly","longshort"):
    pick_path = mod.find_first_existing(mod.PICK_CANDIDATES[strat])
    if not pick_path:
        runs.append({"strategy":strat,"status":"no_picks"})
        out["issues"].append(f"No pick file for {strat}")
        continue
    picks = mod.load_picks(pick_path)
    if picks is None or picks.empty:
        runs.append({"strategy":strat,"status":"no_picks_or_empty"})
        out["issues"].append(f"Picks empty for {strat}")
        continue
    picks_matched = mod.safe_match(picks, realized)
    res = mod.compute_and_save(strat, picks_matched)
    runs.append(res)
    artifacts.extend([mod.OUT.get("regen_"+strat), mod.OUT.get("summary_"+strat), mod.OUT.get("diag_"+strat), mod.OUT.get("png_"+strat)])

out["status"] = "success"
out["artifacts"] = [str(a) for a in artifacts if a]
out["runs"] = runs
out["issues"] = out["issues"]
out["message"] = "verification run complete (used prebuilt realized file)"
print(json.dumps(out))
