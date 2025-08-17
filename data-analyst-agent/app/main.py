from fastapi import FastAPI, UploadFile, File, HTTPException, Request
from fastapi.responses import JSONResponse

from .utils.io_loader import classify_and_read
from .utils.timer import with_time_budget
from .utils.formats import parse_questions, parse_plan
from .tasks.generic import run_generic
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

        # Collect other files (counts only for now)
        inputs = {"dataframes": [], "images": [], "raw": []}
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
                kind, _ = classify_and_read(fname, getattr(value, "content_type", None), content)
                if kind == "dataframe":
                    inputs["dataframes"].append("df")
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
                kind, _ = classify_and_read(fname, getattr(f, "content_type", None), content)
                if kind == "dataframe":
                    inputs["dataframes"].append("df")
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
        dfs = []
        result_payload = {"notes": "no-op"}
        try:
            text_low = q_text.lower()
            if "wikipedia" in text_low or "wiki" in text_low or "http://" in text_low or "https://" in text_low:
                if budget.time_exhausted(2.0):
                    result_payload = {"notes": "timeout-skip-wiki"}
                else:
                    result_payload = run_wikipedia(q_text)
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

        # Shape output (unchanged from Step 4/6)
        if plan.get("type") == "array":
            count = plan.get("array_count") or 0
            shaped = {"result": ["" for _ in range(count)], "task": result_payload}
        elif plan.get("type") == "object":
            keys = plan.get("object_keys") or []
            shaped_obj = {k: result_payload.get(k, "N/A") if isinstance(result_payload, dict) else "N/A" for k in keys}
            if "notes" in keys and isinstance(result_payload, dict) and "notes" in result_payload:
                shaped_obj["notes"] = result_payload["notes"]
            shaped = {"result": shaped_obj}
        else:
            shaped = {"result": result_payload}

        return JSONResponse(
            {
                "status": "ok",
              "note": "patch-b",
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
