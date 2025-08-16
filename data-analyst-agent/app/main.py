from fastapi import FastAPI, UploadFile, File, HTTPException, Request
from fastapi.responses import JSONResponse

from .utils.io_loader import classify_and_read
from .utils.timer import with_time_budget


app = FastAPI(title="Data Analyst Agent API")


@app.get("/")
def read_root():
    return {"status": "ok", "message": "Data Analyst Agent API"}


@app.post("/api/")
async def handle_api(
    request: Request,
    questions_file: UploadFile | None = File(default=None, alias="questions.txt", description="Questions text file (required)")
):
    # Time budget for the entire request (Step 3)
    with with_time_budget(150.0) as budget:
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

        if q_file is None or not getattr(q_file, "filename", None):
            raise HTTPException(status_code=400, detail="questions.txt is required")

        # Optional: validate content type (allow octet-stream or missing)
        if getattr(q_file, "content_type", None) not in ("text/plain", "application/octet-stream", None):
            raise HTTPException(status_code=400, detail="questions.txt must be a text file")

        # Touch-read questions to ensure accessible
        try:
            await q_file.read()
        except Exception:
            raise HTTPException(status_code=400, detail="Failed to read questions.txt")

        # Process all other files
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

        # Return with timing info (Step 3)
        return JSONResponse(
            {
                "status": "ok",
                "note": "step-3",
                "counts": {
                    "dataframes": len(inputs["dataframes"]),
                    "images": len(inputs["images"]),
                    "raw": len(inputs["raw"]),
                },
                "timing": {
                    "elapsed_sec": round(budget.elapsed_seconds(), 3),
                    "remaining_sec": round(budget.remaining_seconds(), 3),
                },
            },
            status_code=200,
        )
