import os
import shutil
from pathlib import Path

# Root of your repo
ROOT = Path(r"C:\Quant\src\quant")

# Where to archive legacy files
ARCHIVE_ROOT = Path(r"C:\archive")

# File names that should be archived
TARGET_FILES = {
    "requirements.txt",
    "entrypoint.sh",
    "Dockerfile",
}

def should_archive(file_path: Path) -> bool:
    return file_path.name in TARGET_FILES

def archive_file(file_path: Path):
    rel_path = file_path.relative_to(ROOT)
    archive_path = ARCHIVE_ROOT / rel_path

    archive_path.parent.mkdir(parents=True, exist_ok=True)
    shutil.move(str(file_path), str(archive_path))
    print(f"Archived: {file_path} -> {archive_path}")

def main():
    print("Archiving legacy microservice artifacts...\n")

    for dirpath, dirnames, filenames in os.walk(ROOT):
        for filename in filenames:
            file_path = Path(dirpath) / filename
            if should_archive(file_path):
                archive_file(file_path)

    print("\nArchive complete.")

if __name__ == "__main__":
    main()