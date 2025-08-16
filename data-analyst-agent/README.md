# Data Analyst Agent (Step 1)

Minimal FastAPI skeleton providing a POST `/api/` endpoint that accepts a required `questions.txt` file and returns a dummy JSON response.

## How to run (Windows PowerShell)

1. Create a virtual environment (optional but recommended)

```powershell
python -m venv .venv ; .\.venv\Scripts\Activate.ps1
```

2. Install dependencies

```powershell
pip install -r data-analyst-agent\requirements.txt
```

3. Start the server

```powershell
uvicorn app.main:app --reload --port 8000 --app-dir data-analyst-agent
```

4. Test the endpoint (requires a local `questions.txt` file)

```powershell
curl -s -X POST "http://127.0.0.1:8000/api/" -F "questions.txt=@questions.txt"
```

You should receive:

```json
{"status":"ok","note":"skeleton"}
```

Proceed to Step 2 only after this works.
