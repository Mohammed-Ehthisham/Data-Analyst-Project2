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
):
    """Create a scatter plot with a red regression line.

    Returns a matplotlib Figure. Caller is responsible for closing it.
    """
    x_arr = np.array(list(x), dtype=float)
    y_arr = np.array(list(y), dtype=float)

    fig, ax = plt.subplots(figsize=(5, 3.5), dpi=120)
    ax.scatter(x_arr, y_arr, s=18, c="#1f77b4", alpha=0.85)

    # Regression line via least squares
    if len(x_arr) >= 2 and np.ptp(x_arr) != 0:
        slope, intercept = np.polyfit(x_arr, y_arr, 1)
        xs = np.linspace(x_arr.min(), x_arr.max(), 100)
        ys = slope * xs + intercept
        ax.plot(xs, ys, color="red", linestyle=":" if dotted_red else "-", linewidth=1.6)

    ax.set_xlabel(x_label)
    ax.set_ylabel(y_label)
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


def encode_image_under_limit(
    fig,
    mime: str = "image/png",
    max_bytes: int = 100_000,
    min_width: int = 240,
    min_height: int = 180,
) -> str:
    """Encode the figure as base64 data URI under the given size.

    Strategy:
    - Save via matplotlib first.
    - If too large, downscale with Pillow in steps until under limit or hitting minimum size.
    - For JPEG, also reduce quality progressively.
    """
    fmt = "png" if "png" in mime.lower() else "jpeg"

    # First attempt: direct from Matplotlib
    img_bytes = _fig_to_bytes(fig, format_=fmt, dpi=120)
    if len(img_bytes) <= max_bytes:
        return _encode_b64_uri(img_bytes, mime)

    # Open with Pillow and downscale progressively
    img = Image.open(BytesIO(img_bytes)).convert("RGBA" if fmt == "png" else "RGB")

    # Prepare parameters for loop
    quality = 85 if fmt == "jpeg" else None
    width, height = img.size

    for _ in range(10):  # up to 10 attempts
        # Resize by 85% each iteration (floor at min dims)
        new_w = max(min_width, int(width * 0.85))
        new_h = max(min_height, int(height * 0.85))
        if (new_w, new_h) == (width, height) and (width, height) == (min_width, min_height):
            # Can't reduce further
            break
        if (new_w, new_h) != (width, height):
            img = img.resize((new_w, new_h), Image.LANCZOS)
            width, height = img.size

        out = BytesIO()
        if fmt == "png":
            # Try palette quantization to reduce size
            try:
                img_to_save = img.convert("P", palette=Image.ADAPTIVE, colors=256)
            except Exception:
                img_to_save = img
            img_to_save.save(out, format="PNG", optimize=True)
        else:  # jpeg
            q = max(40, quality if quality is not None else 85)
            img.save(out, format="JPEG", quality=q, optimize=True)
            # Next iteration reduce quality further
            if quality is not None:
                quality = max(40, quality - 10)

        data = out.getvalue()
        if len(data) <= max_bytes:
            return _encode_b64_uri(data, mime)

    # Final fallback: return the smallest we produced (even if still over limit)
    # but try one last aggressive save
    out = BytesIO()
    if fmt == "png":
        try:
            img_to_save = img.convert("P", palette=Image.ADAPTIVE, colors=128)
        except Exception:
            img_to_save = img
        img_to_save.save(out, format="PNG", optimize=True)
    else:
        img.save(out, format="JPEG", quality=40, optimize=True)
    return _encode_b64_uri(out.getvalue(), mime)
