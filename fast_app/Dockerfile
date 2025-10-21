FROM python:3.11-slim

WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements file
COPY requirements.txt .

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY fast_app/ /app/fast_app/
COPY data_pipeline/ /app/data_pipeline/

# Create necessary directories
RUN mkdir -p /app/data /app/logs /app/fast_app/exports

# Expose port
EXPOSE 8000

# Set environment variables
ENV PYTHONUNBUFFERED=1
ENV PYTHONPATH=/app

# Run the FastAPI application
CMD ["uvicorn", "fast_app.main:app", "--host", "0.0.0.0", "--port", "8000"]

