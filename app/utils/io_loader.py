"""
Robust file loaders for CSV, JSON, Parquet, and Images.
These functions accept FastAPI UploadFile-like objects or file-like objects.
They should never raise unhandled exceptions; instead, raise ValueError with a concise message.
"""
from __future__ import annotations

import io
from typing import Any, Dict, Tuple, Union

import pandas as pd

try:
    import pyarrow as pa  # noqa: F401
    import pyarrow.parquet as pq  # noqa: F401
except Exception:
    # Parquet support is optional at Step 2; we'll attempt via pandas too.
    pa = None  # type: ignore
    pq = None  # type: ignore

try:
    from PIL import Image
except Exception:  # pragma: no cover
    Image = None  # type: ignore


# ---------- Helpers ----------

def _ensure_bytes(file_obj: Any) -> bytes:
    """Read all bytes from a file-like or FastAPI UploadFile.
    If it's an UploadFile, use .read(); if it's bytes already, return as-is.
    """
    if file_obj is None:
        raise ValueError("No file provided")

    # FastAPI UploadFile has a .read coroutine; but in our sync helpers we accept bytes too
    if isinstance(file_obj, (bytes, bytearray)):
        return bytes(file_obj)

    # Try duck-typing for file-like
    if hasattr(file_obj, "read"):
        data = file_obj.read()
        if isinstance(data, str):
            data = data.encode("utf-8", errors="ignore")
        return data

    raise ValueError("Unsupported file object type")


# ---------- Readers ----------

def read_csv(file_obj: Any, encoding: str | None = None) -> pd.DataFrame:
    """Read CSV into DataFrame. Tries utf-8 first, falls back to latin-1.
    Raises ValueError with a short message on failure.
    """
    data = _ensure_bytes(file_obj)
    for enc in ([encoding] if encoding else []) + ["utf-8", "utf-8-sig", "latin-1"]:
        try:
            return pd.read_csv(io.BytesIO(data), encoding=enc)
        except Exception:
            continue
    raise ValueError("Failed to parse CSV")


def read_json(file_obj: Any, encoding: str | None = None) -> Union[pd.DataFrame, Dict[str, Any]]:
    """Read JSON. If it's a list/dict of records, try DataFrame; otherwise return dict.
    Raises ValueError on failure.
    """
    data = _ensure_bytes(file_obj)
    text: str | None = None
    for enc in ([encoding] if encoding else []) + ["utf-8", "utf-8-sig", "latin-1"]:
        try:
            text = data.decode(enc)
            break
        except Exception:
            continue
    if text is None:
        raise ValueError("Failed to decode JSON")

    import json

    try:
        obj = json.loads(text)
    except Exception:
        raise ValueError("Invalid JSON")

    # Try to coerce to DataFrame for common shapes
    try:
        if isinstance(obj, list):
            return pd.DataFrame(obj)
        if isinstance(obj, dict):
            # If dict of lists with equal lengths
            if all(isinstance(v, list) for v in obj.values()):
                return pd.DataFrame(obj)
            return obj
    except Exception:
        pass

    return obj


def read_parquet(file_obj: Any) -> pd.DataFrame:
    """Read Parquet into DataFrame. Tries pandas.read_parquet.
    Raises ValueError on failure.
    """
    data = _ensure_bytes(file_obj)
    try:
        # pandas will use pyarrow or fastparquet if available
        return pd.read_parquet(io.BytesIO(data))
    except Exception:
        # Fallback: use pyarrow directly if available
        try:
            if pq is not None:
                table = pq.read_table(io.BytesIO(data))
                return table.to_pandas()
        except Exception:
            pass
        raise ValueError("Failed to parse Parquet")


def read_image(file_obj: Any):
    """Read image into PIL.Image. Raises ValueError on failure.
    """
    if Image is None:
        raise ValueError("Pillow not installed")
    data = _ensure_bytes(file_obj)
    try:
        return Image.open(io.BytesIO(data)).convert("RGBA")
    except Exception:
        raise ValueError("Failed to load image")


# ---------- Routing by filename/content-type ----------

def classify_and_read(filename: str | None, content_type: str | None, file_bytes: bytes) -> Tuple[str, Any]:
    """Return (kind, value) where kind in {"dataframe","image","raw"}.
    Never raises; on parse errors returns ("raw", file_bytes).
    """
    name = (filename or "").lower()
    ctype = (content_type or "").lower()

    # Try by extension first
    try:
        if name.endswith(".csv") or "text/csv" in ctype:
            return ("dataframe", read_csv(io.BytesIO(file_bytes)))
        if name.endswith(".json") or "application/json" in ctype:
            return ("dataframe", read_json(io.BytesIO(file_bytes)))
        if name.endswith(".parquet") or "parquet" in ctype:
            return ("dataframe", read_parquet(io.BytesIO(file_bytes)))
        if any(name.endswith(ext) for ext in (".png", ".jpg", ".jpeg")) or ctype.startswith("image/"):
            return ("image", read_image(io.BytesIO(file_bytes)))
    except Exception:
        # Fall through to raw if parsing fails
        pass

    # Default: raw bytes
    return ("raw", file_bytes)
