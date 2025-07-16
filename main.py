from fastapi import FastAPI
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
import subprocess
import os

app = FastAPI()

# Serve static files (like CSVs) from /public
app.mount("/public", StaticFiles(directory="public"), name="public")

@app.get("/")
def root():
    return {"message": "Ceentiel Automation Service is Live"}

@app.post("/run")
def run_script():
    try:
        result = subprocess.run(["python", "automation.py"], capture_output=True, text=True, check=True)
        return {"status": "success", "output": result.stdout}
    except subprocess.CalledProcessError as e:
        return {"status": "error", "error": e.stderr}

@app.get("/download")
def download_csv():
    filepath = "public/latest_ceentiel_report.csv"
    if os.path.exists(filepath):
        return FileResponse(path=filepath, filename="latest_ceentiel_report.csv", media_type='text/csv')
    else:
        return {"error": "File not found"}
