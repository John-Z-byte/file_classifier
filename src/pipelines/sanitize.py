from __future__ import annotations
import pandas as pd
import numpy as np
from pandas.api.types import is_object_dtype


TEXT_ALWAYS = {
    "location",
    "client_location",
}

def sanitize_for_parquet(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()

    # 1) Force certain columns to text (string dtype)
    for c in TEXT_ALWAYS:
        if c in df.columns:
            # keep NA as <NA>, make everything else string
            df[c] = df[c].astype("string")

    # 2) Convert timedelta columns to numeric days (float)
    # Detect timedelta-like values even if column is object
    for c in df.columns:
        s = df[c]
        if is_object_dtype(s):
            # quick sample check to avoid scanning huge columns
            sample = s.dropna().head(50)
            if not sample.empty and sample.map(lambda x: isinstance(x, pd.Timedelta)).any():
                td = pd.to_timedelta(s, errors="coerce")
                df[c] = td.dt.total_seconds() / 86400.0  # days as float

    return df
