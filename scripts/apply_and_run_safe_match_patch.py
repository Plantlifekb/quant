from pathlib import Path
import re, shutil, sys, subprocess

REGEN = Path(r"C:\Quant\scripts\regen_and_verify.py")
BACKUP = REGEN.with_suffix(".py.bak")
RUNNER = Path(r"C:\Quant\scripts\run_verification_using_realized.py")
PY = r"C:\Users\Keith\AppData\Local\Programs\Python\Python312\python.exe"

if not REGEN.exists():
    print("ERROR: regen_and_verify.py not found"); sys.exit(1)

# backup once
if not BACKUP.exists():
    shutil.copy2(REGEN, BACKUP)
    print("backup_created")
else:
    print("backup_exists")

s = REGEN.read_text(encoding="utf8")

# replacement safe_match function
new_func = r'''
def safe_match(left, realized):
    import pandas as pd
    # defensive copy
    m2 = realized.copy()
    # normalize week column if present
    if "week" in m2.columns:
        m2["week"] = pd.to_datetime(m2["week"], errors="coerce").dt.normalize()
    # find a realized-return column or compute it from close_end/close_start
    rr_col = next((c for c in ["realized_return", "realized", "return", "ret"] if c in m2.columns), None)
    if rr_col is None:
        if "close_end" in m2.columns and "close_start" in m2.columns:
            m2["realized_return"] = m2["close_end"] / m2["close_start"] - 1.0
            rr_col = "realized_return"
        else:
            raise KeyError("realized_return column missing in realized frame")
    # index realized by week[, ticker]
    if "ticker" in m2.columns:
        m2 = m2.set_index(["week", "ticker"])[rr_col]
    else:
        m2 = m2.set_index("week")[rr_col]
    # prepare left (picks) and normalize week
    left2 = left.copy()
    if "week" in left2.columns:
        left2["week"] = pd.to_datetime(left2["week"], errors="coerce").dt.normalize()
    # lookup function that tolerates missing keys
    def _lookup(r):
        try:
            if "ticker" in left2.columns and isinstance(m2.index, pd.MultiIndex):
                key = (r.get("week"), str(r.get("ticker")))
            else:
                key = r.get("week")
            return m2.loc[key]
        except Exception:
            return None
    # apply and return
    left2["realized_return"] = left2.apply(_lookup, axis=1)
    return left2
'''

# replace existing safe_match block (greedy until next top-level def or EOF)
pattern = re.compile(r'(^def\s+safe_match\s*\(.*?\):\n)(?:.*?)(?=^def\s|\Z)', re.S | re.M)
if pattern.search(s):
    s2 = pattern.sub(new_func, s, count=1)
    REGEN.write_text(s2, encoding="utf8")
    print("patched_safe_match")
else:
    # append if not found
    REGEN.write_text(s + "\n\n" + new_func, encoding="utf8")
    print("appended_safe_match")

# run the runner and capture output to file
log = Path(r"C:\Quant\scripts\run_verification_log.txt")
proc = subprocess.run([PY, str(RUNNER)], capture_output=True, text=True)
log.write_text(proc.stdout + "\n\n=== STDERR ===\n\n" + proc.stderr, encoding="utf8")
print("runner_exitcode:" + str(proc.returncode))
print("log_written:" + str(log))
