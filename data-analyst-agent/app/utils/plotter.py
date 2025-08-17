"""
Plot helpers using matplotlib (no seaborn) with base64 size-limited encoding.
"""
from __future__ import annotations

import base64
from io import BytesIO
from typing import Iterable, Tuple

import numpy as np
from PIL import Image
import matplotlib.pyplot as plt


def plot_scatter_with_regression(
    x: Iterable[float],
    y: Iterable[float],
    x_label: str,
    y_label: str,
    dotted_red: bool = True,
    point_color: str = "#1f77b4",
    line_color: str = "red",
):
    """Create a scatter plot with a red regression line.

    Returns a matplotlib Figure. Caller is responsible for closing it.
    """
    x_arr = np.array(list(x), dtype=float)
    y_arr = np.array(list(y), dtype=float)

    fig, ax = plt.subplots(figsize=(5, 3.5), dpi=120)
    ax.scatter(x_arr, y_arr, s=18, c=point_color, alpha=0.85)

    # Regression line via least squares
    if len(x_arr) >= 2 and np.ptp(x_arr) != 0:
        slope, intercept = np.polyfit(x_arr, y_arr, 1)
        xs = np.linspace(x_arr.min(), x_arr.max(), 100)
        ys = slope * xs + intercept
    ax.plot(xs, ys, color= line_color, linestyle=":" if dotted_red else "-", linewidth=1.6)

    ax.set_xlabel(x_label)
    ax.set_ylabel(y_label)
    ax.grid(alpha=0.25, linestyle=":", linewidth=0.7)
    fig.tight_layout()
    return fig


def plot_bar(values: Iterable[float], labels: Iterable[str], color: str = "#1f77b4", title: str | None = None):
    vals = list(values)
    labs = list(labels)
    fig, ax = plt.subplots(figsize=(5, 3.5), dpi=120)
    ax.bar(labs, vals, color=color)
    if title:
        ax.set_title(title)
    ax.grid(axis="y", alpha=0.25, linestyle=":", linewidth=0.7)
    fig.tight_layout()
    return fig


def plot_line(y: Iterable[float], x: Iterable[float] | None = None, color: str = "red", title: str | None = None):
    yv = list(y)
    xv = list(x) if x is not None else list(range(len(yv)))
    fig, ax = plt.subplots(figsize=(5, 3.5), dpi=120)
    ax.plot(xv, yv, color=color, linestyle="-", linewidth=1.6)
    if title:
        ax.set_title(title)
    ax.grid(alpha=0.25, linestyle=":", linewidth=0.7)
    fig.tight_layout()
    return fig


def _fig_to_bytes(fig, format_: str = "png", dpi: int = 120) -> bytes:
    buf = BytesIO()
    fig.savefig(buf, format=format_, dpi=dpi, bbox_inches="tight")
    return buf.getvalue()


def _encode_b64_uri(data: bytes, mime: str) -> str:
    b64 = base64.b64encode(data).decode("ascii")
    return f"data:{mime};base64,{b64}"


def _tiny_fallback_image_bytes(mime: str) -> bytes:
    """Return a minimal 1x1 pixel image as bytes for fallback."""
    mode = "RGBA" if "png" in mime.lower() else "RGB"
    color = (255, 255, 255, 0) if mode == "RGBA" else (255, 255, 255)
    img = Image.new(mode, (1, 1), color)
    out = BytesIO()
    if "png" in mime.lower():
        img.save(out, format="PNG", optimize=True)
    else:
        img.save(out, format="JPEG", quality=60, optimize=True)
    return out.getvalue()


def _encode_under_limit_bytes(
    fig,
    mime: str = "image/png",
    max_bytes: int = 100_000,
    min_width: int = 240,
    min_height: int = 180,
) -> bytes:
    """Core encoder that returns raw bytes under size limit (or best effort)."""
    fmt = "png" if "png" in mime.lower() else "jpeg"

    # First attempt: direct from Matplotlib
    img_bytes = _fig_to_bytes(fig, format_=fmt, dpi=120)
    if len(img_bytes) <= max_bytes:
        return img_bytes

    # Open with Pillow and downscale progressively
    img = Image.open(BytesIO(img_bytes)).convert("RGBA" if fmt == "png" else "RGB")

    # Prepare parameters for loop
    quality = 85 if fmt == "jpeg" else None
    width, height = img.size

    best = img_bytes
    for _ in range(10):  # up to 10 attempts
        # Resize by ~85% each iteration (floor at min dims)
        new_w = max(min_width, int(width * 0.85))
        new_h = max(min_height, int(height * 0.85))
        if (new_w, new_h) == (width, height) and (width, height) == (min_width, min_height):
            break
        if (new_w, new_h) != (width, height):
            img = img.resize((new_w, new_h), Image.LANCZOS)
            width, height = img.size

        out = BytesIO()
        if fmt == "png":
            # Palette quantization can reduce size
            try:
                img_to_save = img.convert("P", palette=Image.ADAPTIVE, colors=256)
            except Exception:
                img_to_save = img
            img_to_save.save(out, format="PNG", optimize=True)
        else:  # jpeg
            q = max(40, quality if quality is not None else 85)
            img.save(out, format="JPEG", quality=q, optimize=True)
            if quality is not None:
                quality = max(40, quality - 10)

        data = out.getvalue()
        if len(data) < len(best):
            best = data
        if len(data) <= max_bytes:
            return data

    # Final aggressive attempt
    out = BytesIO()
    if fmt == "png":
        try:
            img_to_save = img.convert("P", palette=Image.ADAPTIVE, colors=128)
        except Exception:
            img_to_save = img
        img_to_save.save(out, format="PNG", optimize=True)
    else:
        img.save(out, format="JPEG", quality=40, optimize=True)
    data = out.getvalue()
    return data if len(data) <= len(best) else best


def encode_image_under_limit(
    fig,
    mime: str = "image/png",
    max_bytes: int = 100_000,
    min_width: int = 240,
    min_height: int = 180,
) -> str:
    """Backward-compatible API: returns a data URI under size limit."""
    data = _encode_under_limit_bytes(fig, mime=mime, max_bytes=max_bytes, min_width=min_width, min_height=min_height)
    return _encode_b64_uri(data, mime)


def encode_fig(
    fig,
    mime: str = "image/png",
    max_bytes: int = 100_000,
    mode: str = "data_uri",  # "data_uri" | "raw_base64"
    min_width: int = 240,
    min_height: int = 180,
) -> str:
    """Generalized encoder to return either data URI or raw base64 string.

    If the output still exceeds max_bytes after best effort, returns a tiny 1x1 fallback
    in the requested mode to guarantee a valid response.
    """
    data = _encode_under_limit_bytes(fig, mime=mime, max_bytes=max_bytes, min_width=min_width, min_height=min_height)
    if len(data) > max_bytes:
        data = _tiny_fallback_image_bytes(mime)

    b64 = base64.b64encode(data).decode("ascii")
    if mode == "raw_base64":
        return b64
    return f"data:{mime};base64,{b64}"
