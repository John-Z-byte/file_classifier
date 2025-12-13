# src/io/preview_reader.py
from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, List, Optional

import pandas as pd


@dataclass(frozen=True)
class TabularPreview:
    path: Path
    sheet_name: str
    rows: List[List[Any]]  # matrix INCLUDING header row
    max_rows: int
    status: str  # ok | unreadable
    error_message: Optional[str] = None


def read_excel_preview(path: str | Path, max_rows: int) -> TabularPreview:
    """
    Read only the first `max_rows` rows from the first Excel sheet.
    Avoids broken UsedRange metadata.
    """
    p = Path(path).expanduser().resolve()

    try:
        df = pd.read_excel(
            p,
            sheet_name=0,
            nrows=max_rows,
            dtype=object,
        )

        header = list(df.columns)
        rows = [header]
        for _, r in df.iterrows():
            rows.append(list(r.values))

        sheet_name = df.columns.name if df.columns.name else "Sheet1"

        return TabularPreview(
            path=p,
            sheet_name=str(sheet_name),
            rows=rows,
            max_rows=max_rows,
            status="ok",
        )

    except Exception as e:
        return TabularPreview(
            path=p,
            sheet_name="",
            rows=[],
            max_rows=max_rows,
            status="unreadable",
            error_message=f"{type(e).__name__}: {e}",
        )


def read_csv_preview(path: str | Path, max_rows: int) -> TabularPreview:
    """
    Read only the first `max_rows` rows from a CSV file.
    Handles BOM, delimiter detection, and messy exports.
    """
    p = Path(path).expanduser().resolve()

    try:
        df = pd.read_csv(
            p,
            nrows=max_rows,
            dtype=object,
            encoding="utf-8-sig",   # handles BOM
            sep=None,               # auto-detect delimiter
            engine="python",
            keep_default_na=False,
        )

        header = list(df.columns)
        rows = [header]
        for _, r in df.iterrows():
            rows.append(list(r.values))

        return TabularPreview(
            path=p,
            sheet_name=p.name,   # CSV has no sheets; filename is the source name
            rows=rows,
            max_rows=max_rows,
            status="ok",
        )

    except Exception as e:
        return TabularPreview(
            path=p,
            sheet_name="",
            rows=[],
            max_rows=max_rows,
            status="unreadable",
            error_message=f"{type(e).__name__}: {e}",
        )
