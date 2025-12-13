# src/config_loader.py
from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, Optional
import yaml


def _deep_merge(base: Dict[str, Any], override: Dict[str, Any]) -> Dict[str, Any]:
    out = dict(base)
    for k, v in override.items():
        if isinstance(v, dict) and isinstance(out.get(k), dict):
            out[k] = _deep_merge(out[k], v)
        else:
            out[k] = v
    return out


def load_config(path: str | Path, overrides: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    p = Path(path)
    if not p.exists():
        raise FileNotFoundError(f"Config not found: {p}")

    with p.open("r", encoding="utf-8") as f:
        cfg = yaml.safe_load(f) or {}

    if not isinstance(cfg, dict):
        raise ValueError("settings.yaml root must be a mapping (dict).")

    # minimal defaults (matches your existing behavior)
    defaults: Dict[str, Any] = {
        "input_root": "./datalake",
        "output_root": "./data",
        "extensions": [".xlsx"],
        "excel": {
            "header_search_rows": 200,   # you currently use 200
        },
        "header_detection": {
            "min_header_confidence": 0.60,
        },
        "copy": {
            "mode": "copy",              # copy | move (move later if you want)
            "overwrite": False,
            "dry_run": False,
        },
        "paths": {},  # filled below
        "logging": {
            "level": "INFO",
            "file": "./logs/run.log",
        },
    }

    cfg = _deep_merge(defaults, cfg)

    # derive managed dirs from output_root if not provided
    out = Path(cfg["output_root"])
    cfg.setdefault("paths", {})
    cfg["paths"].setdefault("staging_dir", str(out / "staging"))
    cfg["paths"].setdefault("classified_dir", str(out / "classified"))
    cfg["paths"].setdefault("quarantine_dir", str(out / "quarantine"))

    if overrides:
        cfg = _deep_merge(cfg, overrides)

    return cfg
