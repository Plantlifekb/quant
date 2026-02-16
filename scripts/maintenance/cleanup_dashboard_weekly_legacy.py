"""
Quant v1.0 — Dashboard Legacy Cleanup Script
Removes all references to the deprecated Weekly Picks subsystem.

Actions:
    - Delete legacy weekly dashboard modules
    - Delete archive directory
    - Remove legacy imports and strings from all dashboard pages
    - Clean main Streamlit app of weekly references
"""

from pathlib import Path
import shutil

BASE = Path(r"C:\Quant\scripts\dashboard")

LEGACY_FILES = [
    BASE / "dashboard_10stock_weekly.py",
    BASE / "archive" / "dashboard_10stock_weekly.py",
]

LEGACY_PATTERNS = [
    "weekly_selection",
    "dashboard_10stock_weekly",
    "quant_weekly_10x10",
]

PAGES_DIR = BASE / "pages"
MAIN_APP = BASE / "streamlit_quant_dashboard_v1.py"
ARCHIVE_DIR = BASE / "archive"


def remove_file(path: Path):
    if path.exists():
        path.unlink()
        print(f"✔ Deleted legacy file: {path}")


def remove_archive(path: Path):
    if path.exists() and path.is_dir():
        shutil.rmtree(path)
        print(f"✔ Deleted archive directory: {path}")


def clean_file(path: Path):
    if not path.exists():
        return

    text = path.read_text(errors="ignore")
    original = text

    for pattern in LEGACY_PATTERNS:
        text = text.replace(pattern, "")

    if text != original:
        path.write_text(text)
        print(f"✔ Cleaned legacy references in: {path}")


def main():
    print("\n=== CLEANING LEGACY WEEKLY DASHBOARD FILES (Quant v1.0) ===\n")

    # 1. Delete legacy modules
    for f in LEGACY_FILES:
        remove_file(f)

    # 2. Delete archive directory
    remove_archive(ARCHIVE_DIR)

    # 3. Clean dashboard pages
    if PAGES_DIR.exists():
        for py in PAGES_DIR.rglob("*.py"):
            clean_file(py)

    # 4. Clean main Streamlit app
    clean_file(MAIN_APP)

    print("\n🎉 Dashboard cleanup complete — all legacy weekly references removed.\n")


if __name__ == "__main__":
    main()