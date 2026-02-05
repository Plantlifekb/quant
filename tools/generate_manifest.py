#!/usr/bin/env python3
"""
Generate a JSON manifest for files under a given directory.

Produces a list of entries with:
- path (relative to root)
- size (bytes)
- last_modified (ISO 8601 UTC)
- sha256 (hex) [optional, enabled with --hash]

Usage:
    python tools/generate_manifest.py <root_dir> [--out manifest.json] [--hash]
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Iterable, Optional

DEFAULT_OUT = "manifest.json"


def iter_files(root: Path) -> Iterable[Path]:
    """Yield files under root (recursive), skipping hidden files and directories."""
    for p in root.rglob("*"):
        if p.is_file():
            # skip hidden files and directories (leading dot)
            if any(part.startswith(".") for part in p.relative_to(root).parts):
                continue
            yield p


def file_sha256(path: Path, chunk_size: int = 8192) -> str:
    """Compute SHA-256 hex digest for a file."""
    h = hashlib.sha256()
    with path.open("rb") as fh:
        for chunk in iter(lambda: fh.read(chunk_size), b""):
            h.update(chunk)
    return h.hexdigest()


def file_entry(root: Path, path: Path, include_hash: bool = False) -> Dict[str, Optional[str]]:
    """Return manifest entry for a single file."""
    rel = str(path.relative_to(root).as_posix())
    size = path.stat().st_size
    # Use os.path.getmtime to get mtime and convert to timezone-aware UTC
    mtime = os.path.getmtime(path)
    last_modified = datetime.fromtimestamp(mtime, timezone.utc).isoformat()
    entry: Dict[str, Optional[str]] = {
        "path": rel,
        "size": size,
        "last_modified": last_modified,
    }
    if include_hash:
        entry["sha256"] = file_sha256(path)
    return entry


def build_manifest(root: Path, include_hash: bool = False) -> Dict[str, object]:
    """Build manifest dictionary for the given root directory."""
    files = sorted(iter_files(root), key=lambda p: str(p.relative_to(root)))
    entries = [file_entry(root, p, include_hash=include_hash) for p in files]
    manifest = {
        "root": str(root.resolve()),
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "file_count": len(entries),
        "files": entries,
    }
    return manifest


def write_manifest(manifest: Dict[str, object], out_path: Path) -> None:
    """Write manifest dict to JSON file with pretty formatting."""
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with out_path.open("w", encoding="utf-8") as fh:
        json.dump(manifest, fh, indent=2, ensure_ascii=False)
        fh.write("\n")


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Generate a JSON manifest for a directory")
    p.add_argument("root", type=Path, help="Root directory to scan")
    p.add_argument("--out", "-o", type=Path, default=Path(DEFAULT_OUT), help="Output manifest path")
    p.add_argument("--hash", action="store_true", help="Include SHA-256 hashes for files")
    return p.parse_args()


def main() -> None:
    args = parse_args()
    root: Path = args.root
    if not root.exists() or not root.is_dir():
        raise SystemExit(f"Error: root directory does not exist or is not a directory: {root}")
    manifest = build_manifest(root, include_hash=args.hash)
    write_manifest(manifest, args.out)
    print(f"Wrote manifest for {manifest['file_count']} files to {args.out}")


if __name__ == "__main__":
    main()