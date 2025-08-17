from __future__ import annotations

from typing import Any, Dict, List, Optional

import pandas as pd

from ..utils.plotter import plot_scatter_with_regression, encode_image_under_limit, encode_fig, plot_bar, plot_line
from ..utils.formats import parse_plan
from ..utils.llm_client import ask_openai_json


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


def try_scatter(df: pd.DataFrame, x_col: str, y_col: str, max_bytes: int, mode: str, color: str = "red") -> Optional[str]:
    if x_col in df.columns and y_col in df.columns:
        x = pd.to_numeric(df[x_col], errors="coerce").dropna()
        y = pd.to_numeric(df.loc[x.index, y_col], errors="coerce").dropna()
        x = x.loc[y.index]
        if len(x) >= 2:
            fig = plot_scatter_with_regression(x.values, y.values, x_col, y_col, point_color="#1f77b4", line_color=color)
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


def try_bar(df: pd.DataFrame, cat_col: str, num_col: str, max_bytes: int, mode: str, color: str = "#1f77b4") -> Optional[str]:
    if cat_col in df.columns and num_col in df.columns:
        sample = df[[cat_col, num_col]].dropna().head(8)
        if not sample.empty:
            fig = plot_bar(sample[num_col].tolist(), sample[cat_col].astype(str).tolist(), color=color)
            try:
                return encode_fig(fig, mime="image/png", max_bytes=max_bytes, mode=mode)
            finally:
                try:
                    import matplotlib.pyplot as plt
                    plt.close(fig)
                except Exception:
                    pass
    return None


def try_line(df: pd.DataFrame, num_col: str, max_bytes: int, mode: str, color: str = "red") -> Optional[str]:
    if num_col in df.columns:
        vals = pd.to_numeric(df[num_col], errors="coerce").dropna().tolist()
        if len(vals) >= 2:
            fig = plot_line(vals, color=color)
            try:
                return encode_fig(fig, mime="image/png", max_bytes=max_bytes, mode=mode)
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

    # Preferred colors per type
    color_map = {
        "scatter": next((c.get("color") for c in plan.get("charts", []) if c.get("type") == "scatter"), "red"),
        "bar": next((c.get("color") for c in plan.get("charts", []) if c.get("type") == "bar"), "#1f77b4"),
        "line": next((c.get("color") for c in plan.get("charts", []) if c.get("type") == "line"), "red"),
    }

    # Heuristic plot: look for two numeric columns; else ask LLM for hints
    for df in dfs:
        numeric_cols = [c for c in df.columns if pd.api.types.is_numeric_dtype(df[c])]
        if len(numeric_cols) >= 2:
            img = try_scatter(df, numeric_cols[0], numeric_cols[1], max_bytes=max_bytes, mode=mode, color=color_map["scatter"])
            if img:
                # name respects mode
                result["plot_image"] = img
                break
        # LLM assist: suggest keys, plot type, and columns
        try:
            sample = df.head(5).to_dict(orient="list")
            llm = ask_openai_json(
                f"""
User question:
{question_text}

Attached file columns:
{list(map(str, df.columns))}

Sample rows (first 5):
{sample}

Suggest a JSON with fields:
- object_keys: array of strings
- plot_type: one of bar|line|scatter
- x_col: name (for bar/scatter)
- y_col: name (for bar/scatter) or num_col for line
- summary: short sentence
Return JSON only.
                """
            )
        except Exception:
            llm = {}

        if isinstance(llm, dict):
            # Adopt summary if provided
            if isinstance(llm.get("summary"), str) and not result.get("summary"):
                result["summary"] = llm["summary"].strip()

            # Try to plot as per suggestion
            ptype = (llm.get("plot_type") or "").lower()
            if ptype == "scatter" and llm.get("x_col") and llm.get("y_col"):
                img = try_scatter(df, str(llm["x_col"]), str(llm["y_col"]), max_bytes=max_bytes, mode=mode, color=color_map["scatter"])
                if img:
                    result["plot_image"] = img
                    break
            elif ptype == "bar" and llm.get("x_col") and llm.get("y_col"):
                img = try_bar(df, str(llm["x_col"]), str(llm["y_col"]), max_bytes=max_bytes, mode=mode, color=color_map["bar"])
                if img:
                    result["plot_image"] = img
                    break
            elif ptype == "line" and llm.get("num_col"):
                img = try_line(df, str(llm["num_col"]), max_bytes=max_bytes, mode=mode, color=color_map["line"])
                if img:
                    result["plot_image"] = img
                    break

    return result
