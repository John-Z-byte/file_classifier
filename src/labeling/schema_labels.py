# src/labeling/schema_labels.py
from __future__ import annotations

from pathlib import Path
from typing import Dict
import yaml


def load_schema_labels(path: str | Path) -> Dict[str, str]:
    p = Path(path)
    if not p.exists():
        return {}
    with p.open("r", encoding="utf-8") as f:
        data = yaml.safe_load(f) or {}
    if not isinstance(data, dict):
        raise ValueError("schema_labels.yaml must be a mapping: {schema_hash: label}")
    # normalize
    out: Dict[str, str] = {}
    for k, v in data.items():
        if k and v:
            out[str(k).strip().lower()] = str(v).strip()
    return out
