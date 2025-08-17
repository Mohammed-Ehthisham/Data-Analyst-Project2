"""
Simple rules to detect the requested output format and chart specs from questions.txt.
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

# Chart keywords
BAR_RE = re.compile(r"\b(bar|bars|bar\s*chart)\b", re.IGNORECASE)
LINE_RE = re.compile(r"\bline\s*(chart|plot)\b", re.IGNORECASE)
HIST_RE = re.compile(r"\b(hist|histogram)\b", re.IGNORECASE)
SCATTER_RE = re.compile(r"\bscatter\b", re.IGNORECASE)
COLOR_RE = re.compile(r"\b(blue|green|red|orange|purple|black)\b", re.IGNORECASE)
DATA_URI_RE = re.compile(r"data\s*uri|data:image/\w+;base64", re.IGNORECASE)
RAW_B64_RE = re.compile(r"raw\s*base64|base64\s*(png|only)\b", re.IGNORECASE)

# Keys extraction: explicit bullets or quoted list after 'keys:' only
BULLET_KEY_RE = re.compile(r"^\s*-\s*([A-Za-z0-9_\-]+)\s*:\s*$", re.MULTILINE)
KEYS_LIST_LINE_RE = re.compile(r"keys?\s*:\s*(.+)$", re.IGNORECASE | re.MULTILINE)


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
    # 1) Bulleted keys like '- field:' only (ignore plain headings like 'Charts:')
    for m in BULLET_KEY_RE.finditer(text):
        candidate = m.group(1).strip()
        if candidate and candidate not in keys:
            keys.append(candidate)
    # 2) Explicit quoted list on a 'keys:' line
    for m in KEYS_LIST_LINE_RE.finditer(text):
        line = m.group(1)
        for q in re.findall(r'"([^"]+)"', line):
            qv = q.strip()
            if qv and qv not in keys:
                keys.append(qv)
    return keys


def _extract_global_max_bytes(text: str) -> int:
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
    return max_bytes


def parse_required_shape(text: str) -> Dict[str, object]:
    t = text.strip()
    is_array = ARRAY_HINT_RE.search(t) is not None
    is_object = OBJECT_HINT_RE.search(t) is not None and not is_array

    if is_array:
        return {"type": "array", "array_len": _extract_array_count(t), "object_keys": None}
    # Default to object if not explicitly array
    keys = _extract_object_keys(t) if is_object else []
    return {"type": "object", "array_len": None, "object_keys": keys or None}


def _find_color_near(idx: int, tokens: List[str]) -> Optional[str]:
    # Look backward and forward a few tokens for a color mention
    for delta in range(1, 4):
        for j in (idx - delta, idx + delta):
            if 0 <= j < len(tokens) and COLOR_RE.fullmatch(tokens[j]):
                return tokens[j].lower()
    return None


def detect_chart_specs(text: str) -> List[Dict[str, object]]:
    t = text.strip()
    tokens = re.findall(r"[a-zA-Z]+", t.lower())
    specs: List[Dict[str, object]] = []
    max_bytes = _extract_global_max_bytes(t)

    # Scan tokens to detect chart types and nearby colors
    for i, tok in enumerate(tokens):
        if BAR_RE.fullmatch(tok):
            color = _find_color_near(i, tokens) or "blue"
            specs.append({"slot_key": "bar_chart", "type": "bar", "color": color, "max_bytes": max_bytes})
        elif LINE_RE.fullmatch(tok):
            color = _find_color_near(i, tokens) or "red"
            specs.append({"slot_key": "line_chart", "type": "line", "color": color, "max_bytes": max_bytes})
        elif HIST_RE.fullmatch(tok):
            color = _find_color_near(i, tokens) or "orange"
            specs.append({"slot_key": "histogram", "type": "hist", "color": color, "max_bytes": max_bytes})
        elif SCATTER_RE.fullmatch(tok):
            # If 'red' appears anywhere, assume red regression line
            color = "red" if re.search(r"dotted\s+red\s+regression\s+line", t, re.IGNORECASE) or re.search(r"\bred\b", t, re.IGNORECASE) else "blue"
            specs.append({"slot_key": "scatter_plot", "type": "scatter", "color": color, "max_bytes": max_bytes})

    # Deduplicate by slot_key while preserving order
    seen = set()
    dedup: List[Dict[str, object]] = []
    for s in specs:
        key = (s["slot_key"], s["type"], s["color"])
        if key in seen:
            continue
        seen.add(key)
        dedup.append(s)
    return dedup


def wants_raw_base64(text: str) -> bool:
    t = text.strip()
    if DATA_URI_RE.search(t):
        return False
    if RAW_B64_RE.search(t):
        return True
    # If mentions PNG/base64 without data URI, lean to raw
    if re.search(r"base64\s+png", t, re.IGNORECASE):
        return True
    return False


def parse_plan(text: str) -> Dict[str, object]:
    shape = parse_required_shape(text)
    charts = detect_chart_specs(text)
    raw = wants_raw_base64(text)
    return {
        "response_type": shape.get("type", "object"),
        "array_len": shape.get("array_len"),
        "object_keys": shape.get("object_keys") or [],
        "charts": charts,
        "raw_base64_images": raw,
    }


def parse_questions(text: str) -> Dict[str, object]:
    """Backward-compatible basic parser used by earlier steps.
    Returns keys: type, object_keys, array_count, needs_plot, plot_mime, plot_max_bytes.
    """
    t = text.strip()
    is_array = ARRAY_HINT_RE.search(t) is not None
    is_object = OBJECT_HINT_RE.search(t) is not None and not is_array
    plan_type = "array" if is_array else ("object" if is_object else "unknown")

    # Plot hints
    needs_plot = PLOT_HINT_RE.search(t) is not None
    mime = "image/png"
    if JPG_HINT_RE.search(t) and not PNG_HINT_RE.search(t):
        mime = "image/jpeg"
    max_bytes = _extract_global_max_bytes(t)

    return {
        "type": plan_type,
        "object_keys": _extract_object_keys(t) if plan_type == "object" else [],
        "array_count": _extract_array_count(t) if plan_type == "array" else None,
        "needs_plot": needs_plot,
        "plot_mime": mime,
        "plot_max_bytes": max_bytes,
    }
