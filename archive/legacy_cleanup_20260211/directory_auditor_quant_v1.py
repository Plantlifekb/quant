# ============================================================
# Module: directory_auditor_quant_v1.py
# Quant Version: v1.0
# Purpose:
#   Enforce governed directory structure for Quant v1.0.
#   Detect:
#       - unexpected files
#       - unexpected folders
#       - legacy artefacts
#       - schema‑breaking additions
#       - drift in C:\Quant
#
# Inputs:
#   C:\Quant\
#
# Outputs:
#   None (raises on failure)
#
# Governance Rules:
#   - No ungoverned files.
#   - No unapproved directories.
#   - No legacy artefacts.
#   - Deterministic behaviour.
#
# Dependencies:
#   - pathlib, json
#
# Provenance:
#   - Part of governed Quant v1.0 validation pipeline.
#   - Any modification requires version bump.
# ============================================================

from pathlib import Path

BASE = Path(r"C:\Quant")

ALLOWED_DIRS = {
    "scripts",
    "data",
    "logs",
    "archive",
}

ALLOWED_DATA_SUBDIRS = {
    "ingestion",
    "enriched",
    "ranking",
    "backtest",
    "dashboard",
}

ALLOWED_SCRIPT_SUBDIRS = {
    "enrichment",
    "ranking",
    "backtest",
    "dashboard",
    "validation",
    "logs",
    "patches",
}

FORBIDDEN_EXTENSIONS = {
    ".tmp",
    ".bak",
    ".old",
    ".xlsx",
    ".xls",
    ".json",
    ".pkl",
}

LEGACY_FORBIDDEN = {
    "quant_backtest.csv",
    "quant_backtest_legacy_pre_v1.csv",
}

def fail(msg: str):
    raise RuntimeError(f"Quant v1.0 directory audit failure:\n{msg}")

def audit_root():
    for item in BASE.iterdir():
        if item.is_dir():
            if item.name not in ALLOWED_DIRS:
                fail(f"Unexpected directory: {item}")
        else:
            if item.name not in LEGACY_FORBIDDEN:
                fail(f"Unexpected file in root: {item}")

def audit_data():
    data_dir = BASE / "data"
    for item in data_dir.iterdir():
        if item.is_dir():
            if item.name not in ALLOWED_DATA_SUBDIRS:
                fail(f"Unexpected data subdirectory: {item}")
        else:
            fail(f"Unexpected file in data/: {item}")

def audit_scripts():
    scripts_dir = BASE / "scripts"
    for item in scripts_dir.iterdir():
        if item.is_dir():
            if item.name not in ALLOWED_SCRIPT_SUBDIRS:
                fail(f"Unexpected scripts subdirectory: {item}")
        else:
            if item.suffix in FORBIDDEN_EXTENSIONS:
                fail(f"Forbidden file type in scripts/: {item}")

def audit_legacy():
    for legacy in LEGACY_FORBIDDEN:
        if (BASE / legacy).exists():
            fail(f"Legacy artefact detected: {legacy}")

def main():
    audit_root()
    audit_data()
    audit_scripts()
    audit_legacy()

if __name__ == "__main__":
    main()