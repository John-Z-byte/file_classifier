# src/fingerprint/header_normalizer.py
from __future__ import annotations

import re
import unicodedata
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Optional, Sequence

import yaml


_NON_ALNUM_UNDERSCORE = re.compile(r"[^a-z0-9_]+")
_MULTI_UNDERSCORE = re.compile(r"_+")


@dataclass(frozen=True)
class HeaderNormalizationResult:
    raw_headers: List[str]
    normalized_headers: List[str]
    applied_aliases: Dict[str, str]  # original_norm -> aliased_norm (only when changed)


def load_header_aliases(path: str | Path) -> Dict[str, str]:
    """
    Load alias map from YAML. Expected format:
      some_header_norm: canonical_header_norm
    Keys and values should already be normalized; we normalize anyway for safety.
    """
    p = Path(path).expanduser().resolve()
    if not p.exists():
        return {}

    data = yaml.safe_load(p.read_text(encoding="utf-8")) or {}
    if not isinstance(data, dict):
        raise ValueError("header_aliases.yaml must be a mapping/dict")

    out: Dict[str, str] = {}
    for k, v in data.items():
        if k is None or v is None:
            continue
        kn = normalize_header(str(k))
        vn = normalize_header(str(v))
        if kn and vn:
            out[kn] = vn
    return out


def normalize_headers(
    raw_headers: Sequence[str],
    aliases: Optional[Dict[str, str]] = None,
) -> HeaderNormalizationResult:
    aliases = aliases or {}
    normalized: List[str] = []
    applied: Dict[str, str] = {}

    # normalize + alias mapping
    for h in raw_headers:
        hn = normalize_header(h)
        if not hn:
            continue

        if hn in aliases and aliases[hn] != hn:
            applied[hn] = aliases[hn]
            hn = aliases[hn]

        normalized.append(hn)

    # dedupe within file: name, name__2, name__3...
    deduped: List[str] = []
    counts: Dict[str, int] = {}
    for h in normalized:
        if h not in counts:
            counts[h] = 1
            deduped.append(h)
        else:
            counts[h] += 1
            deduped.append(f"{h}__{counts[h]}")

    return HeaderNormalizationResult(
        raw_headers=list(raw_headers),
        normalized_headers=deduped,
        applied_aliases=applied,
    )


def normalize_header(header: str) -> str:
    """
    Normalize a single header according to project rules.
    """
    if header is None:
        return ""

    s = str(header).strip().lower()
    if not s:
        return ""

    # remove accents
    s = unicodedata.normalize("NFKD", s)
    s = "".join(ch for ch in s if not unicodedata.combining(ch))

    # replace separators with underscore
    s = s.replace(" ", "_").replace("-", "_").replace("/", "_").replace(".", "_")

    # remove everything except [a-z0-9_]
    s = _NON_ALNUM_UNDERSCORE.sub("_", s)

    # collapse multiple underscores + trim underscores
    s = _MULTI_UNDERSCORE.sub("_", s).strip("_")

    return s
