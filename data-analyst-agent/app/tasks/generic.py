from __future__ import annotations

from typing import Any, Dict, List

import pandas as pd

from ..utils.plotter import plot_scatter_with_regression, encode_image_under_limit, encode_fig
from ..utils.formats import parse_plan


def summarize_dataframes(dfs: List[pd.DataFrame]) -> Dict[str, Any]:
    out: Dict[str, Any] = {"num_dataframes": len(dfs), "summaries": []}
    for df in dfs[:3]:  # limit to first 3
        info = {
            "columns": list(map(str, df.columns[:50])),
            "rows": int(df.shape[0]),
            "describe": df.describe(include="all").fillna("").astype(str).to_dict(),
        }
        out["summaries"].append(info)
    return out


def try_scatter(df: pd.DataFrame, x_col: str, y_col: str, max_bytes: int, mode: str) -> str | None:
    if x_col in df.columns and y_col in df.columns:
        x = pd.to_numeric(df[x_col], errors="coerce").dropna()
        y = pd.to_numeric(df.loc[x.index, y_col], errors="coerce").dropna()
        x = x.loc[y.index]
        if len(x) >= 2:
            fig = plot_scatter_with_regression(x.values, y.values, x_col, y_col)
            try:
                encoded = encode_fig(fig, mime="image/png", max_bytes=max_bytes, mode=mode)
                return encoded
            finally:
                try:
                    import matplotlib.pyplot as plt
                    plt.close(fig)
                except Exception:
                    pass
    return None


def run_generic(question_text: str, inputs: Dict[str, Any]) -> Dict[str, Any]:
    """Fallback analysis over attached CSV/JSON/Parquet dataframes.
    Returns a dict; caller will shape it according to the requested format.
    """
    dfs: List[pd.DataFrame] = inputs.get("dfs", [])
    result: Dict[str, Any] = {"notes": "generic fallback", "counts": {"dfs": len(dfs)}}

    if not dfs:
        return result

    # Basic summary
    result["summary"] = summarize_dataframes(dfs)

    # Determine encoding preferences
    plan = parse_plan(question_text)
    mode = "raw_base64" if plan.get("raw_base64_images") else "data_uri"
    max_bytes = 100_000
    # If charts list carries max_bytes, use the min of them
    chart_limits = [c.get("max_bytes") for c in plan.get("charts", []) if isinstance(c, dict) and c.get("max_bytes")]
    if chart_limits:
        try:
            max_bytes = int(min(chart_limits))
        except Exception:
            pass

    # Heuristic plot: look for two numeric columns
    for df in dfs:
        numeric_cols = [c for c in df.columns if pd.api.types.is_numeric_dtype(df[c])]
        if len(numeric_cols) >= 2:
            img = try_scatter(df, numeric_cols[0], numeric_cols[1], max_bytes=max_bytes, mode=mode)
            if img:
                # name respects mode
                result["plot_image"] = img
                break

    return result
