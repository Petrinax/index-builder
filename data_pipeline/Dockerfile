FROM python:3.11-slim

WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y \
    gcc \
    g++ \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements and install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy the application code
COPY data_pipeline/ ./data_pipeline/
COPY .env* ./

# Create directories for logs and data
RUN mkdir -p logs data csv_exports

# Set Python path
ENV PYTHONPATH=/app

CMD ["python", "-m", "data_pipeline.ingestion_pipeline", "run_daily_snapshot"]

