from fastapi import FastAPI, UploadFile, File, HTTPException, Request
from fastapi.responses import JSONResponse
from typing import List

from .utils.io_loader import classify_and_read

app = FastAPI(title="Data Analyst Agent (Step 1)")


@app.get("/")
def read_root():
    return {"status": "ok", "message": "Data Analyst Agent API - Step 1 skeleton"}


@app.post("/api/")
async def handle_api(
    request: Request,
    questions_file: UploadFile | None = File(default=None, alias="questions.txt", description="Questions text file (required)")
):
    # Parse multipart form once to see all parts including questions.txt and extras
    try:
        form = await request.form()
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid multipart/form-data")

    q_file = questions_file
    inputs = {"dataframes": [], "images": [], "raw": []}
    seen = set()

    # Fallback: if binding didn't provide questions_file, locate it in the form by duck-typing
    if q_file is None:
        for key, value in form.multi_items():
            if key == "questions.txt" and hasattr(value, "filename") and hasattr(value, "read"):
                q_file = value  # type: ignore[assignment]
                break

    if q_file is None or not q_file.filename:
        raise HTTPException(status_code=400, detail="questions.txt is required")

    # Optional: validate content type (allow octet-stream or missing)
    if getattr(q_file, "content_type", None) not in ("text/plain", "application/octet-stream", None):
        raise HTTPException(status_code=400, detail="questions.txt must be a text file")

    # Touch-read questions to ensure accessible
    try:
        await q_file.read()
    except Exception:
        raise HTTPException(status_code=400, detail="Failed to read questions.txt")

    # Second pass: process all other files
    for key, value in form.multi_items():
        if key == "questions.txt":
            continue
        if hasattr(value, "filename") and hasattr(value, "read"):
            try:
                content = await value.read()
            except Exception:
                continue
            fname = getattr(value, "filename", "") or ""
            if fname in seen:
                continue
            seen.add(fname)
            kind, _parsed = classify_and_read(fname, getattr(value, "content_type", None), content)
            if kind == "dataframe":
                inputs["dataframes"].append("df")
            elif kind == "image":
                inputs["images"].append("img")
            else:
                inputs["raw"].append(fname or "raw")

    # Return dummy JSON with counts
    return JSONResponse(
        {
            "status": "ok",
            "note": "step-2",
            "counts": {
                "dataframes": len(inputs["dataframes"]),
                "images": len(inputs["images"]),
                "raw": len(inputs["raw"]),
            },
        },
        status_code=200,
    )
