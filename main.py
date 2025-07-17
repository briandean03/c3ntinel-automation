from fastapi import FastAPI
from fastapi.responses import FileResponse, JSONResponse
import os
import asyncio
from automation import run

app = FastAPI()
CSV_PATH = "public/latest_ceentiel_report.csv"

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

@app.get("/latest_ceentiel_report.csv")
async def get_csv():
    if os.path.exists(CSV_PATH):
        return FileResponse(CSV_PATH, media_type='text/csv', filename="latest_ceentiel_report.csv")
    else:
        return JSONResponse(content={"error": "CSV file not found yet. Please wait a moment and try again."}, status_code=404)

@app.get("/status")
async def get_status():
    if os.path.exists(CSV_PATH):
        return {"status": "Report generated", "file": CSV_PATH}
    else:
        return {"status": "Report generation in progress or failed"}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=int(os.getenv("PORT", 8000)))