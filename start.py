import os
import sys

# Ensure we can import the FastAPI app from data-analyst-agent/app
REPO_ROOT = os.path.dirname(os.path.abspath(__file__))
APP_DIR = os.path.join(REPO_ROOT, "data-analyst-agent")
if APP_DIR not in sys.path:
    sys.path.insert(0, APP_DIR)

from uvicorn import run

if __name__ == "__main__":
    # Allow PORT env override (useful on platforms like Render later)
    port = int(os.environ.get("PORT", "8000"))
    # Start Uvicorn serving app.main:app
    run("app.main:app", host="127.0.0.1", port=port, reload=True, log_level="info")
