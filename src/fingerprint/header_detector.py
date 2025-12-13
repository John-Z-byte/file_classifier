# src/fingerprint/header_detector.py
from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Sequence, Tuple


@dataclass(frozen=True)
class HeaderDetectionResult:
    header_row_index: Optional[int]  # 0-based
    confidence: float                # 0..1
    raw_headers: List[str]
    score_breakdown: Dict[str, float]


def detect_header_row(
    rows: Sequence[Sequence[Any]],
    min_confidence: float,
) -> HeaderDetectionResult:
    """
    Given a preview matrix (top N rows), choose the best header row candidate.
    If confidence < min_confidence => header_row_index=None (quarantine decision upstream).
    """
    if not rows:
        return HeaderDetectionResult(None, 0.0, [], {"reason": 0.0})

    best_idx = None
    best_score = -1.0
    best_breakdown: Dict[str, float] = {}

    # Score each row as candidate header
    for idx in range(len(rows)):
        score, breakdown = _score_row_as_header(rows, idx)
        if score > best_score:
            best_score = score
            best_idx = idx
            best_breakdown = breakdown

    # Convert score to confidence in 0..1 (score already 0..1-ish, clamp)
    confidence = max(0.0, min(1.0, best_score))

    if best_idx is None or confidence < min_confidence:
        return HeaderDetectionResult(None, confidence, [], best_breakdown)

    raw_headers = _extract_raw_headers(rows[best_idx])
    return HeaderDetectionResult(best_idx, confidence, raw_headers, best_breakdown)


def _score_row_as_header(rows: Sequence[Sequence[Any]], idx: int) -> Tuple[float, Dict[str, float]]:
    row = rows[idx]
    vals = [_clean_cell(v) for v in row]
    nonempty = [v for v in vals if v is not None]

    if not vals:
        return 0.0, {"nonempty_density": 0.0, "reason": 0.0}

    nonempty_density = len(nonempty) / max(1, len(vals))
    if len(nonempty) == 0:
        return 0.0, {"nonempty_density": 0.0, "reason": 0.0}

    # text vs numeric ratio
    text_count = sum(1 for v in nonempty if _is_texty(v))
    numeric_count = sum(1 for v in nonempty if _is_numeric(v))
    text_ratio = text_count / max(1, len(nonempty))

    # short string dominance (headers tend to be short)
    short_count = sum(1 for v in nonempty if isinstance(v, str) and 1 <= len(v) <= 40)
    short_ratio = short_count / max(1, len(nonempty))

    # uniqueness: headers likely mostly unique
    norm_strings = [str(v).strip().lower() for v in nonempty if str(v).strip() != ""]
    unique_ratio = len(set(norm_strings)) / max(1, len(norm_strings))

    # coherence with following rows: after header, data rows often have fewer strings-as-labels
    follow_score = _following_rows_coherence(rows, idx)

    # Weighted sum (tuned for messy excel)
    score = (
        0.22 * nonempty_density
        + 0.28 * text_ratio
        + 0.18 * short_ratio
        + 0.20 * unique_ratio
        + 0.12 * follow_score
    )

    breakdown = {
        "nonempty_density": nonempty_density,
        "text_ratio": text_ratio,
        "short_ratio": short_ratio,
        "unique_ratio": unique_ratio,
        "follow_score": follow_score,
        "score": score,
    }
    return max(0.0, min(1.0, score)), breakdown


def _following_rows_coherence(rows: Sequence[Sequence[Any]], idx: int) -> float:
    """
    Heuristic: a header row tends to be followed by rows with:
    - similar non-empty density
    - more numeric / mixed values than pure label strings
    """
    next_rows = rows[idx + 1 : idx + 6]  # lookahead up to 5 rows
    if not next_rows:
        return 0.3  # unknown, mild support

    header_vals = [_clean_cell(v) for v in rows[idx]]
    header_nonempty = sum(1 for v in header_vals if v is not None)
    header_len = max(1, len(header_vals))
    header_density = header_nonempty / header_len

    scores: List[float] = []
    for r in next_rows:
        vals = [_clean_cell(v) for v in r]
        nonempty = [v for v in vals if v is not None]
        density = len(nonempty) / max(1, len(vals))

        numeric_ratio = (sum(1 for v in nonempty if _is_numeric(v)) / max(1, len(nonempty))) if nonempty else 0.0
        text_ratio = (sum(1 for v in nonempty if _is_texty(v)) / max(1, len(nonempty))) if nonempty else 0.0

        # prefer similar density, and not "all text labels" like metadata blocks
        density_sim = 1.0 - min(1.0, abs(density - header_density) / 0.6)
        mixedness = min(1.0, numeric_ratio + 0.5 * (1.0 - text_ratio))

        scores.append(0.55 * density_sim + 0.45 * mixedness)

    return sum(scores) / max(1, len(scores))


def _extract_raw_headers(row: Sequence[Any]) -> List[str]:
    out: List[str] = []
    for v in row:
        v2 = _clean_cell(v)
        if v2 is None:
            continue
        s = str(v2).strip()
        if s == "":
            continue
        out.append(s)
    return out


def _clean_cell(v: Any) -> Optional[Any]:
    if v is None:
        return None
    if isinstance(v, str):
        s = v.strip()
        return None if s == "" else s
    return v


def _is_numeric(v: Any) -> bool:
    return isinstance(v, (int, float)) and not isinstance(v, bool)


def _is_texty(v: Any) -> bool:
    # treat dates as non-texty; openpyxl returns datetime/date objects sometimes
    return isinstance(v, str)
