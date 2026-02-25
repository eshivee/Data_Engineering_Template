
# -----------------------------
# Medallion Architecture Dockerfile
# -----------------------------

# Use official Python image
FROM python:3.11-slim

# Set working directory
WORKDIR /app

# Install system dependencies (if needed for Spark)
RUN apt-get update && apt-get install -y     curl     build-essential     && rm -rf /var/lib/apt/lists/*

# Copy requirements first (better Docker layer caching)
COPY requirements.txt .

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy project files
COPY . .

# Set environment variable (default to dev)
ENV ENVIRONMENT=dev

# Default command (can be overridden by Airflow or Kubernetes)
CMD ["python", "-m", "medallion_architecture.run_pipeline"]
