from fastapi import FastAPI
from automation import main as run_script
import datetime

app = FastAPI()

@app.get("/")
def read_root():
    return {"status": "Ceentiel automation is deployed!"}

@app.post("/run")
def run_ceentiel(start: str = "2024-06-01T00:00:00.000Z", end: str = "2025-06-01T00:00:00.000Z"):
    # Call your main function from automation.py
    run_script(start, end)
    return {"message": "✅ Script executed", "start": start, "end": end}
