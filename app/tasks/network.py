from __future__ import annotations

from typing import Any, Dict, List

import pandas as pd

from ..utils.formats import parse_plan
from ..utils.plotter import plot_line, encode_fig


def run_network(question_text: str, inputs: Dict[str, Any]) -> Dict[str, Any]:
    dfs: List[pd.DataFrame] = inputs.get("dfs", [])
    plan = parse_plan(question_text)
    mode = "raw_base64" if plan.get("raw_base64_images") else "data_uri"
    max_bytes = min([c.get("max_bytes", 100_000) for c in plan.get("charts", [])], default=100_000)

    avg_latency = 0.0
    count = 0
    line_img: str | None = None

    line_color = next((c.get("color") for c in plan.get("charts", []) if c.get("type") == "line"), "red")

    for df in dfs:
        # Heuristic: columns named latency(ms) or latency
        cand = None
        for c in df.columns:
            lc = str(c).lower()
            if "latency" in lc:
                cand = c
                break
        if cand is not None:
            vals = pd.to_numeric(df[cand], errors="coerce").dropna()
            if not vals.empty:
                avg_latency += float(vals.mean())
                count += 1
                if line_img is None:
                    fig = plot_line(vals.tolist(), color=line_color)
                    try:
                        line_img = encode_fig(fig, mime="image/png", max_bytes=max_bytes, mode=mode)
                    finally:
                        try:
                            import matplotlib.pyplot as plt
                            plt.close(fig)
                        except Exception:
                            pass

    if count:
        avg_latency /= count

    return {
        "summary": "network analysis",
        "avg_latency_ms": avg_latency,
        "line_chart": line_img,
    }
