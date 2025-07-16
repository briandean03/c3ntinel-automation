from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles
import os
from automation import main

app = FastAPI()

# Run the automation on startup (will generate the CSV into 'public/')
@app.on_event("startup")
def run_automation():
    # These can be parameterized or hardcoded for now
    start_date = "2024-06-01T00:00:00.000Z"
    end_date = "2025-06-01T00:00:00.000Z"
    main(start_date, end_date)

# Serve the public/ directory
if not os.path.exists("public"):
    os.makedirs("public")

app.mount("/", StaticFiles(directory="public", html=True), name="static")
