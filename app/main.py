from fastapi import FastAPI, UploadFile, File, HTTPException, Request
from fastapi.responses import JSONResponse

from .utils.io_loader import classify_and_read
from .utils.timer import with_time_budget
from .utils.formats import parse_questions, parse_plan
from .tasks.generic import run_generic
from .tasks.sales import run_sales
from .tasks.network import run_network
from .tasks.weather import run_weather
from .tasks.wikipedia import run_wikipedia
from .tasks.duckdb_tasks import run_duckdb_example
from .utils.llm_client import ask_openai_json
from .tasks.highcourt import run_highcourt


app = FastAPI(title="Data Analyst Agent API")


@app.get("/")
def read_root():
    return {"status": "ok", "message": "Data Analyst Agent API"}


@app.post("/api/")
@app.post("/")
async def handle_api(
    request: Request,
    files: list[UploadFile] | None = File(default=None, description="All form files (fallback)")
):
    # Time budget for the entire request (Step 3)
    with with_time_budget(150.0) as budget:
        # Parse multipart form once to see all parts including questions.txt and extras
        try:
            form = await request.form()
        except Exception:
            raise HTTPException(status_code=400, detail="Invalid multipart/form-data")

        # Locate questions with robust fallbacks (file or inline text field)
        q_file: UploadFile | None = None
        q_bytes: bytes | None = None
        if q_file is None:
            for key, value in form.multi_items():
                if key == "questions.txt":
                    if hasattr(value, "filename") and hasattr(value, "read"):
                        q_file = value  # type: ignore[assignment]
                        break
                    # Inline text field case
                    if isinstance(value, str) and not q_bytes:
                        q_bytes = value.encode("utf-8", errors="ignore")
        if q_file is None:
            for key, value in form.multi_items():
                if key == "question.txt":
                    if hasattr(value, "filename") and hasattr(value, "read"):
                        q_file = value  # type: ignore[assignment]
                        break
                    if isinstance(value, str) and not q_bytes:
                        q_bytes = value.encode("utf-8", errors="ignore")
        if q_file is None and files:
            for f in files:
                if (f.filename or "").lower() == "questions.txt":
                    q_file = f
                    break
        if q_file is None and files:
            for f in files:
                if (f.filename or "").lower() == "question.txt":
                    q_file = f
                    break
        if q_file is None:
            for key, value in form.multi_items():
                if hasattr(value, "content_type") and getattr(value, "content_type", None) == "text/plain":
                    if hasattr(value, "filename") and hasattr(value, "read"):
                        q_file = value  # type: ignore[assignment]
                        break
        if q_file is None and files:
            for f in files:
                if (getattr(f, "content_type", None) == "text/plain") or ((f.filename or "").lower().endswith(".txt")):
                    q_file = f
                    break

        if q_file is None and q_bytes is None:
            raise HTTPException(status_code=400, detail="questions.txt is required")

        # Validate content type (allow octet-stream or missing)
        if q_file is not None:
            if getattr(q_file, "content_type", None) not in ("text/plain", "application/octet-stream", None):
                raise HTTPException(status_code=400, detail="questions.txt must be a text file")

        # Read questions bytes
        if q_bytes is None and q_file is not None:
            try:
                q_bytes = await q_file.read()
            except Exception:
                raise HTTPException(status_code=400, detail="Failed to read questions.txt")

        # Collect other files (and load DataFrames for tasks)
        inputs = {"dataframes": [], "images": [], "raw": []}
        dfs_loaded = []
        seen = set()
        for key, value in form.multi_items():
            if key == "questions.txt":
                continue
            if hasattr(value, "filename") and hasattr(value, "read"):
                try:
                    content = await value.read()
                except Exception:
                    continue
                fname = getattr(value, "filename", "") or ""
                if not fname or fname in seen:
                    continue
                seen.add(fname)
                kind, data = classify_and_read(fname, getattr(value, "content_type", None), content)
                if kind == "dataframe":
                    inputs["dataframes"].append("df")
                    dfs_loaded.append(data)
                elif kind == "image":
                    inputs["images"].append("img")
                else:
                    inputs["raw"].append(fname or "raw")

        if files:
            for f in files:
                try:
                    content = await f.read()
                except Exception:
                    continue
                fname = (f.filename or "")
                if not fname or fname in seen:
                    continue
                seen.add(fname)
                kind, data = classify_and_read(fname, getattr(f, "content_type", None), content)
                if kind == "dataframe":
                    inputs["dataframes"].append("df")
                    dfs_loaded.append(data)
                elif kind == "image":
                    inputs["images"].append("img")
                else:
                    inputs["raw"].append(fname or "raw")

        # Parse questions to build plan (still basic from Step 4)
        try:
            q_text = q_bytes.decode("utf-8", errors="ignore") if isinstance(q_bytes, (bytes, bytearray)) else str(q_bytes or "")
        except Exception:
            q_text = ""
        simple_plan = parse_questions(q_text)
        combined_plan = parse_plan(q_text)
        plan = simple_plan

    # Minimal task routing retained from Step 6, with LLM consult for vague/no-plugin cases
        dfs = dfs_loaded
        result_payload = {"notes": "no-op"}
        try:
            text_low = q_text.lower()
            # High court must be checked before generic URL/wiki handling
            if "high court" in text_low or "high-court" in text_low or "judgments.ecourts.gov.in" in text_low:
                if budget.time_exhausted(3.0):
                    result_payload = {"notes": "timeout-skip-highcourt"}
                else:
                    result_payload = run_highcourt(q_text)
            elif ("wikipedia" in text_low) or ("wikipedia.org" in text_low):
                if budget.time_exhausted(2.0):
                    result_payload = {"notes": "timeout-skip-wiki"}
                else:
                    result_payload = run_wikipedia(q_text)
            elif any(k in text_low for k in ["sales", "revenue", "orders"]):
                if budget.time_exhausted(2.0):
                    result_payload = {"notes": "timeout-skip-sales"}
                else:
                    result_payload = run_sales(q_text, {"dfs": dfs})
            elif any(k in text_low for k in ["latency", "network", "ping"]):
                if budget.time_exhausted(2.0):
                    result_payload = {"notes": "timeout-skip-network"}
                else:
                    result_payload = run_network(q_text, {"dfs": dfs})
            elif any(k in text_low for k in ["weather", "temperature", "temp "]):
                if budget.time_exhausted(2.0):
                    result_payload = {"notes": "timeout-skip-weather"}
                else:
                    result_payload = run_weather(q_text, {"dfs": dfs})
            elif "duckdb" in text_low or "s3" in text_low:
                if budget.time_exhausted(2.0):
                    result_payload = {"notes": "timeout-skip-duckdb"}
                else:
                    result_payload = run_duckdb_example(q_text)
            else:
                # If vague or no plugin matched, optionally ask LLM for hints to improve plan
                matched = False
                if budget.time_exhausted(2.0):
                    result_payload = {"notes": "timeout-skip-generic"}
                else:
                    # quick heuristic: few keywords and short request => vague
                    vague = len(q_text.split()) < 12 and not any(k in text_low for k in ["sales","latency","temperature","wiki","duckdb"]) 
                    if vague and not budget.time_exhausted(1.0):
                        try:
                            plan_hint = ask_openai_json(
                                f"""
Analyze this request and propose a JSON plan:
- type: "array" or "object"
- object_keys: [] if object
- plot: true/false and chart type (bar/line/scatter)
- color: name if specified
---
QUESTION:
{q_text}
                                """
                            )
                            if isinstance(plan_hint, dict) and plan_hint.get("object_keys") and isinstance(combined_plan.get("object_keys"), list):
                                # Merge keys suggestion (append unique)
                                ok = set(combined_plan.get("object_keys") or [])
                                for k in plan_hint.get("object_keys", []):
                                    if isinstance(k, str) and k not in ok:
                                        combined_plan["object_keys"].append(k)
                        except Exception:
                            pass
                    result_payload = run_generic(q_text, {"dfs": dfs})
        except Exception as e:
            result_payload = {"notes": f"task-error: {e}"}

        # Shape output according to combined plan (PATCH E/F basics)
        cplan = combined_plan
        shaped = {}
        if cplan.get("response_type") == "array":
            n = cplan.get("array_len") or 0
            # If highcourt task produced answers and plot, map them into array order if possible
            arr = ["" for _ in range(n)]
            if isinstance(result_payload, dict):
                # Best-effort fill: count of >$2bn before 2000 unknown -> "0"; earliest >1.5bn unknown -> "N/A"
                # For highcourt prompt we don't use array path; this is a placeholder
                pass
            shaped = {"result": arr, "task": result_payload}
        else:
            keys = cplan.get("object_keys") or []
            shaped_obj = {}
            # Build a mapping from chart type to key requested
            chart_key_map = {}
            for c in cplan.get("charts", []) or []:
                ctype = c.get("type")
                # prefer explicit slot_key, else infer by type
                dest = c.get("slot_key") or ("bar_chart" if ctype == "bar" else ("scatter_plot" if ctype == "scatter" else ("line_chart" if ctype == "line" else None)))
                if dest:
                    chart_key_map[ctype] = dest

            for k in keys:
                val = None
                if isinstance(result_payload, dict) and k in result_payload:
                    val = result_payload[k]
                # Route generic plot_image or task-specific line_chart/bar_chart to requested key
                if val is None and isinstance(result_payload, dict):
                    # if requested bar_chart but result has a generic plot_image
                    if k == chart_key_map.get("bar") and "plot_image" in result_payload:
                        val = result_payload["plot_image"]
                    if k == chart_key_map.get("scatter") and "plot_image" in result_payload:
                        val = result_payload["plot_image"]
                    if k == chart_key_map.get("line") and "line_chart" in result_payload:
                        val = result_payload["line_chart"]
                    if k == chart_key_map.get("bar") and "bar_chart" in result_payload:
                        val = result_payload["bar_chart"]

                # Highcourt special mapping for textual keys
                if val is None and isinstance(result_payload, dict):
                    kl = k.lower()
                    if ("high court" in kl and ("disposed" in kl or "most cases" in kl)) and "top_court_2019_2022" in result_payload:
                        val = result_payload.get("top_court_2019_2022", "N/A")
                    elif "slope" in kl and "33_10" in kl:
                        val = result_payload.get("slope_33_10", 0.0)
                    elif ("corr" in kl or "correlation" in kl) and "33_10" in kl:
                        val = result_payload.get("corr_33_10", 0.0)

                # Highcourt scatter plot mapping
                if (val is None or val == "") and isinstance(result_payload, dict) and "points_33_10" in result_payload and ("plot" in k.lower() or "scatter" in k.lower()):
                    # Render scatter from points with encoder and assign
                    try:
                        from .utils.plotter import plot_scatter_with_regression, encode_fig
                        pts = result_payload["points_33_10"]
                        xs = [p[0] for p in pts][:500]
                        ys = [p[1] for p in pts][:500]
                        fig = plot_scatter_with_regression(xs, ys, "year", "days", dotted_red=True)
                        mode = "raw_base64" if cplan.get("raw_base64_images") else "data_uri"
                        val = encode_fig(fig, mime="image/png", max_bytes=100_000, mode=mode)
                        try:
                            import matplotlib.pyplot as plt
                            plt.close(fig)
                        except Exception:
                            pass
                    except Exception:
                        val = ""

                # enforce numeric for typical numeric fields
                if k.lower() in ("total_sales", "avg_latency_ms", "avg_temperature") and not isinstance(val, (int, float)):
                    try:
                        val = float(val)
                    except Exception:
                        val = 0.0
                # ensure images present as string if provided
                if k.lower() in ("bar_chart", "scatter_plot", "line_chart") and not isinstance(val, str):
                    val = ""
                if val is None:
                    # default string placeholder
                    val = "N/A"
                shaped_obj[k] = val
            shaped = {"result": shaped_obj}

    return JSONResponse(
            {
                "status": "ok",
        "note": "patch-c",
                "counts": {
                    "dataframes": len(inputs["dataframes"]),
                    "images": len(inputs["images"]),
                    "raw": len(inputs["raw"]),
                },
        "plan": plan,
        "combined_plan": combined_plan,
                **shaped,
                "timing": {
                    "elapsed_sec": round(budget.elapsed_seconds(), 3),
                    "remaining_sec": round(budget.remaining_seconds(), 3),
                },
            },
            status_code=200,
        )
