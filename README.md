

# Data-Analyst-Project2

## Run the Data Analyst Agent (Step 1)

Windows PowerShell friendly start script:

```powershell
python .\start.py
```

This launches Uvicorn and serves the FastAPI app from `app/main.py` at:


Example test (requires a local `questions.txt` file):

```powershell
curl -s -X POST "http://127.0.0.1:8000/api/" -F "questions.txt=@questions.txt"
```

## Deploying to Render

Follow these steps to deploy on Render as a Web Service:

1) Push this repo to GitHub (or connect your Git provider).

2) Ensure the following files exist at the repo root:
	- `requirements.txt`
	- `Procfile` with: `web: uvicorn app.main:app --host 0.0.0.0 --port ${PORT:-8000}`
	- `.gitignore` including `.venv/`, `__pycache__/`, `.env`, and other non-deploy files.

3) In Render:
	- Create New → Web Service → Connect your repo.
	- Environment: `Python 3.x` (Auto-detected via `requirements.txt`).
	- Build Command: `pip install -r requirements.txt`
	- Start Command: `uvicorn app.main:app --host 0.0.0.0 --port $PORT`
	- Instance Type: pick free or a small size.

4) (Optional) Environment variables:
	- If you plan to use OpenAI: `OPENAI_API_KEY`, `OPENAI_MODEL` (see `data-analyst-agent/.env.example`).

5) Deploy. Render will build the image and start the service. Health check path can be `/`.

6) Test your endpoint once live:
	- POST to `https://<your-service>.onrender.com/api/` with `multipart/form-data` and a `questions.txt` part.

Notes
- Do not commit your `.env`. Add any secrets via Render’s Environment tab.
- If you see memory errors while plotting, lower image size or concurrency.