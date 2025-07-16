import threading
import os
from fastapi import FastAPI
from fastapi.responses import FileResponse
import automation  # import your automation.py file as a module

app = FastAPI()

@app.on_event("startup")
def startup_event():
    def run_data_fetch():
        automation.main("2024-06-01T00:00:00.000Z", "2025-06-01T00:00:00.000Z")
    threading.Thread(target=run_data_fetch).start()

@app.get("/")
def home():
    return {"message": "Ceentiel automation server running."}

@app.get("/latest_ceentiel_report.csv")
def get_csv():
    file_path = os.path.join("public", "latest_ceentiel_report.csv")
    if os.path.exists(file_path):
        return FileResponse(file_path, media_type="text/csv", filename="latest_ceentiel_report.csv")
    return {"error": "CSV file not found yet. Please wait a moment and try again."}
