from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
import pandas as pd
import re
import unicodedata


def to_snake(s: str) -> str:
    if s is None:
        return ""
    s = str(s).strip()
    s = unicodedata.normalize("NFKD", s)
    s = "".join(ch for ch in s if not unicodedata.combining(ch))
    s = s.lower()
    s = re.sub(r"[^\w]+", "_", s)
    s = re.sub(r"_+", "_", s).strip("_")
    return s


@dataclass
class ReadResult:
    df: pd.DataFrame
    used_header_row_index: int | None
    matched_catalog: bool


def _match_header_row_index(
    catalog_df: pd.DataFrame,
    *,
    schema_hash: str,
    original_filename: str,
) -> int | None:
    """
    We match classified file back to catalog using:
    - schema_hash
    - original filename (the part after '<hash>__')
    If multiple matches exist, take the newest by modified_ts.
    """
    c = catalog_df.copy()
    if "modified_ts" in c.columns:
        c["modified_ts"] = pd.to_datetime(c["modified_ts"], errors="coerce")

    c["_orig_name"] = c["path"].apply(lambda p: Path(str(p)).name)

    hits = c[(c["schema_hash"] == schema_hash) & (c["_orig_name"] == original_filename)]
    if hits.empty:
        return None

    hits = hits.sort_values("modified_ts", ascending=False)
    val = hits.iloc[0].get("header_row_index")
    try:
        return int(val) if pd.notna(val) else None
    except Exception:
        return None


def read_wellsky_xlsx_full(
    xlsx_path: Path,
    *,
    header_row_index: int | None,
) -> ReadResult:
    """
    Read XLSX using pandas. If we have header_row_index, use it as header row.
    """
    matched = header_row_index is not None
    if header_row_index is None:
        # Fallback: read with first row as header. We'll still normalize columns later.
        df = pd.read_excel(xlsx_path)
        return ReadResult(df=df, used_header_row_index=None, matched_catalog=False)

    df = pd.read_excel(xlsx_path, header=header_row_index)
    return ReadResult(df=df, used_header_row_index=header_row_index, matched_catalog=matched)


def consolidate_schema_from_classified(
    *,
    classified_dir: Path,
    catalog_df: pd.DataFrame,
    label: str,
    schema_hash: str,
) -> pd.DataFrame:
    """
    Consolidate all files for (label, schema_hash) from:
      data/classified/<label>/<schema_hash>__*.xlsx
      data/classified/<label>/<schema_hash>__*.csv

    Adds metadata cols:
      - label
      - schema_hash
      - source_file
    Normalizes columns to snake_case.
    """
    label_dir = classified_dir / label
    patterns = [f"{schema_hash}__*.xlsx", f"{schema_hash}__*.csv"]

    files = []
    for pat in patterns:
        files.extend(label_dir.glob(pat))
    files = sorted(files)

    if not files:
        raise FileNotFoundError(
            f"No files found for {label=} {schema_hash=} in {label_dir} (patterns={patterns})"
        )

    frames: list[pd.DataFrame] = []

    for p in files:
        name = p.name
        original_name = name.split("__", 1)[1] if "__" in name else name

        if p.suffix.lower() == ".xlsx":
            header_idx = _match_header_row_index(
                catalog_df,
                schema_hash=schema_hash,
                original_filename=original_name,
            )
            rr = read_wellsky_xlsx_full(p, header_row_index=header_idx)
            df = rr.df.copy()

        elif p.suffix.lower() == ".csv":
            # CSV already has a header row typically; treat as standard
            df = pd.read_csv(p).copy()

        else:
            # should not happen given glob patterns, but keep safe
            continue

        df.columns = [to_snake(c) for c in df.columns]

        # Metadata
        df["label"] = label
        df["schema_hash"] = schema_hash
        df["source_file"] = original_name

        frames.append(df)

    out = pd.concat(frames, ignore_index=True)
    return out
