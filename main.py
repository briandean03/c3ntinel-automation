from fastapi import FastAPI
from fastapi.responses import FileResponse, RedirectResponse
import os
from automation import run as run_report
from detect_faulty_metres import run as run_faulty_report

app = FastAPI()

@app.get("/")
async def root():
    return RedirectResponse(url="/status")
    # Or use: return {"message": "Welcome to C3ntinel Automation API. Try /status, /run-report, or /run-faulty-report"}

@app.get("/status")
async def status():
    return {"status": "API is running"}

@app.get("/run-report")
async def run_report_endpoint():
    run_report()
    return {"status": "Report generation triggered"}

@app.get("/run-faulty-report")
async def run_faulty_report_endpoint():
    run_faulty_report()
    return {"status": "Faulty report generation triggered"}

@app.get("/latest_ceentiel_report.csv")
async def get_report():
    file_path = "public/latest_ceentiel_report.csv"
    if os.path.exists(file_path):
        return FileResponse(file_path, media_type="text/csv", filename="latest_ceentiel_report.csv")
    return {"error": "File not found"}

@app.get("/faulty_meter_deltas.csv")
async def get_faulty_report():
    file_path = "public/faulty_meter_deltas.csv"
    if os.path.exists(file_path):
        return FileResponse(file_path, media_type="text/csv", filename="faulty_meter_deltas.csv")
    return {"error": "File not found"}