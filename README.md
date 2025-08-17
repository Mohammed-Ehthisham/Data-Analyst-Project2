# Data-Analyst-Project2

## Run the Data Analyst Agent (Step 1)

Windows PowerShell friendly start script:

```powershell
python .\start.py
```

This launches Uvicorn and serves the FastAPI app from `data-analyst-agent/app/main.py` at:

- http://127.0.0.1:8000/
- POST http://127.0.0.1:8000/api/

Example test (requires a local `questions.txt` file):

```powershell
curl -s -X POST "http://127.0.0.1:8000/api/" -F "questions.txt=@questions.txt"
```