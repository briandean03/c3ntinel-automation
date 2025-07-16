from fastapi import FastAPI
from fastapi.responses import FileResponse, JSONResponse
import os

app = FastAPI()

CSV_PATH = "public/latest_ceentiel_report.csv"

@app.get("/")
def root():
    return {"message": "Ceentiel Automation is running."}

@app.get("/latest_ceentiel_report.csv")
def get_csv():
    if os.path.exists(CSV_PATH):
        return FileResponse(CSV_PATH, media_type='text/csv', filename="latest_ceentiel_report.csv")
    else:
        return JSONResponse(content={"error": "CSV file not found yet. Please wait a moment and try again."}, status_code=404)
