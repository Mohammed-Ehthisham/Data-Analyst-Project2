"""
Simple rules to detect the requested output format from questions.txt.
No LLM, just regex-based heuristics.
"""
from __future__ import annotations

import re
from typing import Dict, List, Optional

ARRAY_HINT_RE = re.compile(r"json\s+array\s+of\s+(strings|items)", re.IGNORECASE)
ARRAY_COUNT_RE = re.compile(r"(?:array|return|respond)\s+(?:with|of|exactly)\s*(\d+)\s*(?:items|elements|strings)?", re.IGNORECASE)
OBJECT_HINT_RE = re.compile(r"json\s+object", re.IGNORECASE)
PLOT_HINT_RE = re.compile(r"plot|chart|image", re.IGNORECASE)
PNG_HINT_RE = re.compile(r"png", re.IGNORECASE)
JPG_HINT_RE = re.compile(r"jpg|jpeg", re.IGNORECASE)
SIZE_RE = re.compile(r"(\d{2,3}[,]?\d{3})\s*bytes|([1-9]\d*)\s*k(?:b|ib)?", re.IGNORECASE)

# Keys extraction: quoted tokens that look like keys, or lines starting with - key:
QUOTED_KEY_RE = re.compile(r"\"([A-Za-z0-9_ \-]+)\"")
LINE_KEY_RE = re.compile(r"^[\s*-]*([A-Za-z0-9_\-]+)\s*:\s*$", re.MULTILINE)
KEYS_LIST_RE = re.compile(r"keys?\s*:\s*\"([^\"]+)\"(?:\s*,\s*\"([^\"]+)\")*", re.IGNORECASE)


def _extract_array_count(text: str) -> Optional[int]:
    m = ARRAY_COUNT_RE.search(text)
    if m:
        try:
            return int(m.group(1))
        except Exception:
            return None
    return None


def _extract_object_keys(text: str) -> List[str]:
    keys: List[str] = []
    # 1) Quoted keys appearing near words like keys or object
    for m in QUOTED_KEY_RE.finditer(text):
        candidate = m.group(1).strip()
        if candidate and len(candidate) <= 64:
            keys.append(candidate)
    # 2) Lines like '- field:' or 'field:'
    for m in LINE_KEY_RE.finditer(text):
        candidate = m.group(1).strip()
        if candidate and candidate not in keys:
            keys.append(candidate)
    # Deduplicate preserving order
    dedup: List[str] = []
    seen = set()
    for k in keys:
        if k not in seen:
            dedup.append(k)
            seen.add(k)
    return dedup


def _extract_plot_prefs(text: str) -> Dict[str, object]:
    needs_plot = PLOT_HINT_RE.search(text) is not None
    mime = "image/png"
    if JPG_HINT_RE.search(text) and not PNG_HINT_RE.search(text):
        mime = "image/jpeg"
    max_bytes = 100_000
    m = SIZE_RE.search(text)
    if m:
        if m.group(1):
            try:
                max_bytes = int(m.group(1).replace(",", ""))
            except Exception:
                pass
        elif m.group(2):
            try:
                max_bytes = int(m.group(2)) * 1000
            except Exception:
                pass
    return {"needs_plot": needs_plot, "plot_mime": mime, "plot_max_bytes": max_bytes}


def parse_questions(text: str) -> Dict[str, object]:
    text_norm = text.strip()
    is_array = ARRAY_HINT_RE.search(text_norm) is not None
    is_object = OBJECT_HINT_RE.search(text_norm) is not None and not is_array

    plan: Dict[str, object] = {
        "type": "array" if is_array else ("object" if is_object else "unknown"),
        "object_keys": [],
        "array_count": None,
        "needs_plot": False,
        "plot_mime": "image/png",
        "plot_max_bytes": 100_000,
    }

    prefs = _extract_plot_prefs(text_norm)
    plan.update(prefs)

    if plan["type"] == "array":
        plan["array_count"] = _extract_array_count(text_norm)
    elif plan["type"] == "object":
        plan["object_keys"] = _extract_object_keys(text_norm)

    return plan
