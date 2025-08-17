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


app = FastAPI(title="Data Analyst Agent API")


@app.get("/")
def read_root():
    return {"status": "ok", "message": "Data Analyst Agent API"}


@app.post("/api/")
@app.post("/")
async def handle_api(
    request: Request,
    questions_file: UploadFile | None = File(default=None, alias="questions.txt", description="Questions text file (required)"),
    files: list[UploadFile] | None = File(default=None, description="All form files (fallback)")
):
    # Time budget for the entire request (Step 3)
    with with_time_budget(150.0) as budget:
        # Parse multipart form once to see all parts including questions.txt and extras
        try:
            form = await request.form()
        except Exception:
            raise HTTPException(status_code=400, detail="Invalid multipart/form-data")

        # Locate questions file with robust fallbacks
        q_file = questions_file
        if q_file is None:
            for key, value in form.multi_items():
                if key == "questions.txt" and hasattr(value, "filename") and hasattr(value, "read"):
                    q_file = value  # type: ignore[assignment]
                    break
        if q_file is None:
            for key, value in form.multi_items():
                if key == "question.txt" and hasattr(value, "filename") and hasattr(value, "read"):
                    q_file = value  # type: ignore[assignment]
                    break
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

        if q_file is None or not getattr(q_file, "filename", None):
            raise HTTPException(status_code=400, detail="questions.txt is required")

        # Validate content type (allow octet-stream or missing)
        if getattr(q_file, "content_type", None) not in ("text/plain", "application/octet-stream", None):
            raise HTTPException(status_code=400, detail="questions.txt must be a text file")

        # Read questions bytes
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
            q_text = q_bytes.decode("utf-8", errors="ignore") if isinstance(q_bytes, (bytes, bytearray)) else str(q_bytes)
        except Exception:
            q_text = ""
        simple_plan = parse_questions(q_text)
        combined_plan = parse_plan(q_text)
        plan = simple_plan

        # Minimal task routing retained from Step 6 (no change for PATCH A)
        dfs = dfs_loaded
        result_payload = {"notes": "no-op"}
        try:
            text_low = q_text.lower()
            if "wikipedia" in text_low or "wiki" in text_low or "http://" in text_low or "https://" in text_low:
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
                if budget.time_exhausted(2.0):
                    result_payload = {"notes": "timeout-skip-generic"}
                else:
                    result_payload = run_generic(q_text, {"dfs": dfs})
        except Exception as e:
            result_payload = {"notes": f"task-error: {e}"}

        # Shape output according to combined plan (PATCH E/F basics)
        cplan = combined_plan
        shaped = {}
        if cplan.get("response_type") == "array":
            n = cplan.get("array_len") or 0
            shaped = {"result": ["" for _ in range(n)], "task": result_payload}
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
