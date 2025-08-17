import os
from uvicorn import run

if __name__ == "__main__":
    # Local dev launcher; Render uses Procfile instead.
    port = int(os.environ.get("PORT", "8000"))
    run("app.main:app", host="127.0.0.1", port=port, reload=True, log_level="info")
