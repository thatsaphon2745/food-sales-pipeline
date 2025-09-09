# Base image: Python 3.9.3
FROM python:3.9.3

# Set working directory
WORKDIR /app

# Env: real-time logs, no .pyc files, set timezone
ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    TZ=Asia/Bangkok

# Install dependencies
COPY requirements.txt .
RUN pip install --upgrade pip && \
    pip install --no-cache-dir --prefer-binary -r requirements.txt

# Copy source code
COPY ingest/ ./ingest/

# Default command: run ingestion script
CMD ["python", "ingest/ingest_foodsales.py"]
