"""
Helpers to always return a valid JSON structure, even when time is short.
"""
from __future__ import annotations

from typing import Dict, List, Any


def ensure_array_of_strings(n: int) -> List[str]:
    if n < 0:
        n = 0
    return ["" for _ in range(n)]


def ensure_object_with_keys(keys: List[str]) -> Dict[str, Any]:
    uniq = []
    seen = set()
    for k in keys:
        if k not in seen:
            uniq.append(k)
            seen.add(k)
    return {k: "N/A" for k in uniq}


def best_effort(payload: Any) -> Any:
    # For now, assume payload is JSON-serializable dict/list already.
    # Later steps may coerce more complex results.
    return payload
