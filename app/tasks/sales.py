from __future__ import annotations

from typing import Any, Dict, List

import pandas as pd

from ..utils.formats import parse_plan
from ..utils.plotter import plot_bar, encode_fig


def run_sales(question_text: str, inputs: Dict[str, Any]) -> Dict[str, Any]:
    dfs: List[pd.DataFrame] = inputs.get("dfs", [])
    plan = parse_plan(question_text)
    mode = "raw_base64" if plan.get("raw_base64_images") else "data_uri"
    max_bytes = min([c.get("max_bytes", 100_000) for c in plan.get("charts", [])], default=100_000)

    total_sales = 0.0
    bar_img: str | None = None

    # preferred color from plan for bar chart
    bar_color = next((c.get("color") for c in plan.get("charts", []) if c.get("type") == "bar"), "#1f77b4")

    for df in dfs:
        cols = {c.lower(): c for c in df.columns}
        if "sales" in cols:
            s = pd.to_numeric(df[cols["sales"]], errors="coerce").fillna(0)
            total_sales += float(s.sum())
        # build a tiny bar from first few rows if we have category + numeric
        cat_col = None
        num_col = None
        for c in df.columns:
            if pd.api.types.is_numeric_dtype(df[c]) and num_col is None:
                num_col = c
            if df[c].dtype == object and cat_col is None:
                cat_col = c
        if cat_col and num_col and bar_img is None:
            sample = df[[cat_col, num_col]].dropna().head(5)
            if not sample.empty:
                fig = plot_bar(sample[num_col].tolist(), sample[cat_col].astype(str).tolist(), color=bar_color)
                try:
                    bar_img = encode_fig(fig, mime="image/png", max_bytes=max_bytes, mode=mode)
                finally:
                    try:
                        import matplotlib.pyplot as plt
                        plt.close(fig)
                    except Exception:
                        pass

    return {
        "summary": "sales analysis",
        "total_sales": total_sales,
        "bar_chart": bar_img,
    }
