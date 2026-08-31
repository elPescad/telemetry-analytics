FROM python:3.11-slim

# Prevent Python from writing .pyc files and enable live log streaming
ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1

WORKDIR /app

# Install dependencies first for Docker layer caching
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application source code
COPY app/ ./app/

# Create persistent storage directory
RUN mkdir -p /app/data/summaries

EXPOSE 5000

# Launch FastAPI server with integrated APScheduler
CMD ["uvicorn", "app.api:app", "--host", "0.0.0.0", "--port", "5000"]