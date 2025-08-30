from fastapi import FastAPI, UploadFile, File, HTTPException, Request
from fastapi.responses import JSONResponse

from .enhanced_agent import EnhancedLLMAgent
from .utils.io_loader import classify_and_read
from .utils.timer import with_time_budget
from .utils.formats import parse_questions, parse_plan

# Initialize enhanced LLM agent
enhanced_agent = EnhancedLLMAgent()

app = FastAPI(title="Enhanced LLM-Driven Data Analyst Agent API", version="2.0.0")


@app.get("/")
def read_root():
    return {"status": "ok", "message": "Data Analyst Agent API"}


@app.post("/api/")
@app.post("/")
async def handle_api(
    request: Request,
    files: list[UploadFile] | None = File(default=None, description="All form files (fallback)")
):
    # Time budget for the entire request (updated to 4 minutes)
    with with_time_budget(240.0) as budget:
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
        inputs = {"dataframes": [], "images": [], "raw": [], "dfs": []}
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

        # Parse questions to build plan
        try:
            q_text = q_bytes.decode("utf-8", errors="ignore") if isinstance(q_bytes, (bytes, bytearray)) else str(q_bytes or "")
        except Exception:
            q_text = ""
        
        # Add DataFrames to inputs for enhanced agent
        inputs["dfs"] = dfs_loaded
        
        # Use enhanced LLM agent for analysis
        try:
            if budget.time_exhausted(5.0):
                result_payload = {"error": "timeout", "note": "Request timeout"}
            else:
                result_payload = await enhanced_agent.analyze(q_text, inputs)
                
        except Exception as e:
            result_payload = {"error": f"analysis-error: {e}"}

        # Parse plans for compatibility
        simple_plan = parse_questions(q_text)
        combined_plan = parse_plan(q_text)

        # Return enhanced response
        return JSONResponse(
            {
                "status": "ok",
                "note": "enhanced-llm-agent",
                "counts": {
                    "dataframes": len(inputs["dataframes"]),
                    "images": len(inputs["images"]),
                    "raw": len(inputs["raw"]),
                },
                "plan": simple_plan,
                "combined_plan": combined_plan,
                "result": result_payload,
                "timing": {
                    "elapsed_sec": round(budget.elapsed_seconds(), 3),
                    "remaining_sec": round(budget.remaining_seconds(), 3),
                },
            },
            status_code=200,
        )
