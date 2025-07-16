from fastapi import FastAPI
from fastapi.responses import FileResponse
import os

app = FastAPI()

@app.get("/")
def home():
    return {"message": "Ceentiel automation is running!"}

@app.get("/latest_ceentiel_report.csv")
def get_csv():
    csv_path = os.path.join("public", "latest_ceentiel_report.csv")
    if os.path.exists(csv_path):
        return FileResponse(csv_path, media_type='text/csv', filename="latest_ceentiel_report.csv")
    return {"error": "CSV file not found"}
