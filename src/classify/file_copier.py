# src/classify/file_copier.py
from __future__ import annotations

import shutil
from dataclasses import dataclass
from pathlib import Path
from typing import Optional, Iterable


@dataclass(frozen=True)
class CopyResult:
    src: Path
    dst: Path
    status: str  # copied | skipped_exists | error
    error_message: Optional[str] = None


def _safe_rm_tree(path: Path) -> None:
    """Remove a directory tree if it exists."""
    if path.exists() and path.is_dir():
        shutil.rmtree(path)


def prepare_snapshot_folders(output_root: Path, labels_in_run: Iterable[str]) -> None:
    """
    Snapshot mode:
    For each label present in the current run, wipe its gold folder so
    the run writes a fresh snapshot (latest version).
    """
    for label in sorted(set(labels_in_run)):
        label_dir = output_root / label
        _safe_rm_tree(label_dir)
        label_dir.mkdir(parents=True, exist_ok=True)


def copy_file(src: str | Path, dst: str | Path, overwrite: bool = False) -> CopyResult:
    s = Path(src)
    d = Path(dst)
    d.parent.mkdir(parents=True, exist_ok=True)

    try:
        if d.exists() and not overwrite:
            return CopyResult(src=s, dst=d, status="skipped_exists")

        # copy2 preserves metadata (mtime)
        shutil.copy2(s, d)
        return CopyResult(src=s, dst=d, status="copied")
    except Exception as e:
        return CopyResult(
            src=s,
            dst=d,
            status="error",
            error_message=f"{type(e).__name__}: {e}",
        )
