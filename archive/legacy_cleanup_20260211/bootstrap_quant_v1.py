# ============================================================
# Module: bootstrap_quant_v1.py
# Quant Version: v1.0
# Purpose:
#   Create the governed Quant v1.0 directory structure with
#   correct subfolders, empty placeholders, and deterministic
#   layout. No drift permitted.
#
# Creates:
#   C:\Quant\data\ingestion
#   C:\Quant\data\enriched
#   C:\Quant\data\ranking
#   C:\Quant\data\backtest
#   C:\Quant\data\dashboard
#   C:\Quant\scripts\enrichment
#   C:\Quant\scripts\ranking
#   C:\Quant\scripts\backtest
#   C:\Quant\scripts\dashboard
#   C:\Quant\scripts\validation
#   C:\Quant\scripts\logs
#   C:\Quant\scripts\patches
#   C:\Quant\logs
#   C:\Quant\archive
#   C:\Quant\runbook
#
# Governance Rules:
#   - No extra folders.
#   - No missing folders.
#   - Deterministic structure only.
#   - Any modification requires version bump.
#
# Dependencies:
#   - pathlib
#
# Provenance:
#   - Canonical environment initializer for Quant v1.0.
# ============================================================

from pathlib import Path

BASE = Path(r"C:\Quant")

DIRS = [
    BASE / "data" / "ingestion",
    BASE / "data" / "enriched",
    BASE / "data" / "ranking",
    BASE / "data" / "backtest",
    BASE / "data" / "dashboard",
    BASE / "scripts" / "enrichment",
    BASE / "scripts" / "ranking",
    BASE / "scripts" / "backtest",
    BASE / "scripts" / "dashboard",
    BASE / "scripts" / "validation",
    BASE / "scripts" / "logs",
    BASE / "scripts" / "patches",
    BASE / "logs",
    BASE / "archive",
    BASE / "runbook",
]

PLACEHOLDERS = [
    BASE / "data" / "ingestion" / ".keep",
    BASE / "data" / "enriched" / ".keep",
    BASE / "data" / "ranking" / ".keep",
    BASE / "data" / "backtest" / ".keep",
    BASE / "data" / "dashboard" / ".keep",
    BASE / "logs" / ".keep",
    BASE / "archive" / ".keep",
    BASE / "runbook" / ".keep",
]

def main():
    for d in DIRS:
        d.mkdir(parents=True, exist_ok=True)
    for p in PLACEHOLDERS:
        p.write_text("")

if __name__ == "__main__":
    main()