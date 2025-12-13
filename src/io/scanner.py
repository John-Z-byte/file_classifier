# src/io/scanner.py
from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import List, Sequence


@dataclass(frozen=True)
class DiscoveredFile:
    path: Path
    size_bytes: int
    modified_ts: float  # epoch seconds


def _norm_exts(exts: Sequence[str]) -> set[str]:
    out = set()
    for e in exts:
        e = e.strip()
        if not e:
            continue
        out.add(e.lower() if e.startswith(".") else f".{e.lower()}")
    return out


def scan_files(input_root: str | Path, extensions: Sequence[str]) -> List[DiscoveredFile]:
    """
    Recursively scan input_root for files matching extensions.
    Returns a stable, sorted list (by path).
    """
    root = Path(input_root).expanduser().resolve()
    if not root.exists() or not root.is_dir():
        raise FileNotFoundError(f"input_root not found or not a directory: {root}")

    exts = _norm_exts(extensions)
    results: List[DiscoveredFile] = []

    for p in root.rglob("*"):
        if not p.is_file():
            continue
        if p.suffix.lower() not in exts:
            continue

        try:
            st = p.stat()
        except OSError:
            continue

        results.append(
            DiscoveredFile(
                path=p,
                size_bytes=int(st.st_size),
                modified_ts=float(st.st_mtime),
            )
        )

    results.sort(key=lambda x: str(x.path).lower())
    return results
