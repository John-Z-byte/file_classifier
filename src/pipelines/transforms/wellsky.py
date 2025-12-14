from __future__ import annotations

import re
import unicodedata
import pandas as pd

NAME_MAP = {
    149: "Green Bay", 203: "Appleton", 238: "Sheboygan", 363: "Madison", 391: "Cedarburg",
    427: "Racine", 850: "Burlington", 858: "Stevens Point", 237: "Nashville", 434: "Bowling Green",
    629: "Frankfort", 668: "Clarksville", 772: "Franklin", 780: "Gadsden", 827: "Goodlettsville"
}

ACRO_MAP = {
    149: "GB", 203: "Apl", 238: "Sheb", 363: "Mad", 391: "Ced", 427: "Rac", 850: "Burl", 858: "SP",
    237: "Nash", 434: "BG", 629: "FT", 668: "Clar", 772: "Fran", 780: "Gad", 827: "Good"
}


def to_snake(s: str) -> str:
    if s is None:
        return ""
    s = str(s).strip()
    s = unicodedata.normalize("NFKD", s)
    s = "".join(ch for ch in s if not unicodedata.combining(ch))
    s = s.lower()
    s = re.sub(r"[^\w]+", "_", s)  # spaces/punct -> _
    s = re.sub(r"_+", "_", s).strip("_")
    return s


def add_franchise_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    WellSky contract:
    - Normalize columns to snake_case
    - Derive franchise fields from a location-like column when present
    - Always return columns:
        franchise (Int64), franchise_name (string-ish), franchise_acro (string-ish)
    - Force location-like columns to text to avoid Parquet type issues
    """
    df = df.copy()
    df.columns = [to_snake(c) for c in df.columns]

    # Location-like column (after snake_case)
    loc_col = next((c for c in ("location", "client_location", "office", "branch", "site") if c in df.columns), None)

    # Always create required columns (even if we can't derive)
    if "franchise" not in df.columns:
        df["franchise"] = pd.Series([pd.NA] * len(df), dtype="Int64")
    else:
        # ensure stable dtype
        df["franchise"] = pd.to_numeric(df["franchise"], errors="coerce").astype("Int64")

    if "franchise_name" not in df.columns:
        df["franchise_name"] = pd.NA

    if "franchise_acro" not in df.columns:
        df["franchise_acro"] = pd.NA

    if not loc_col:
        return df

    # Critical: force to text so mixed int/str doesn't break parquet
    df[loc_col] = df[loc_col].astype("string")

    # Extract 3-digit franchise id from the location-like string
    f = df[loc_col].str.extract(r"(\d{3})", expand=False)
    f = pd.to_numeric(f, errors="coerce").astype("Int64")

    df["franchise"] = f
    df["franchise_name"] = f.map(NAME_MAP)
    df["franchise_acro"] = f.map(ACRO_MAP)

    return df
