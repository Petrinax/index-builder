#!/bin/bash

# Startup script for Index Builder FastAPI Application

cd /Users/piyushupreti/Documents/Projects/index-builder

echo "Starting Index Builder API..."
echo "API will be available at: http://localhost:8000"
echo "API Documentation at: http://localhost:8000/docs"
echo ""

# Run the FastAPI application
python -m uvicorn fast_app.main:app --reload --host 0.0.0.0 --port 8000

