#!/bin/bash
echo "🔁 Starting FastAPI server..."

# Read the Render-assigned port (default to 8000 if not set)
PORT=${PORT:-8000}

# Run FastAPI server using uvicorn
uvicorn main:app --host 0.0.0.0 --port $PORT
