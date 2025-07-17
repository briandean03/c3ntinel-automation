from fastapi import FastAPI
from fastapi.responses import FileResponse, JSONResponse
import os
import asyncio
from automation import run
from detect_faulty_metres import run as run_faulty

app = FastAPI()
CSV_PATH = "public/latest_ceentiel_report.csv"
FAULTY_CSV_PATH = "public/faulty_meter_deltas.csv"

@app.on_event("startup")
async def start_background_task():
    asyncio.create_task(asyncio.to_thread(run))

@app.get("/")
async def root():
    return {"message": "Ceentiel Automation is running."}

@app.get("/run-report")
async def trigger_report():
    asyncio.create_task(asyncio.to_thread(run))
    return {"message": "Report generation started"}

@app.get("/run-faulty-report")
async def trigger_faulty_report():
    asyncio.create_task(asyncio.to_thread(run_faulty))
    return {"message": "Faulty meters report generation started"}

@app.get("/latest_ceentiel_report.csv")
async def get_csv():
    if os.path.exists(CSV_PATH):
        return FileResponse(CSV_PATH, media_type='text/csv', filename="latest_ceentiel_report.csv")
    else:
        return JSONResponse(content={"error": "CSV file not found yet. Please wait a moment and try again."}, status_code=404)

@app.get("/faulty_meter_deltas.csv")
async def get_faulty_csv():
    if os.path.exists(FAULTY_CSV_PATH):
        return FileResponse(FAULTY_CSV_PATH, media_type='text/csv', filename="faulty_meter_deltas.csv")
    else:
        return JSONResponse(content={"error": "Faulty meters report not found."}, status_code=404)

@app.get("/status")
async def get_status():
    status = {}
    if os.path.exists(CSV_PATH):
        status["latest_ceentiel_report"] = "Generated"
    else:
        status["latest_ceentiel_report"] = "In progress or failed"
    if os.path.exists(FAULTY_CSV_PATH):
        status["faulty_meter_deltas"] = "Generated"
    else:
        status["faulty_meter_deltas"] = "In progress or failed"
    return status

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=int(os.getenv("PORT", 8000)))