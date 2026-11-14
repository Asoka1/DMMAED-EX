# ==============================================================================
# AUCA Big Data Analytics - Containerized Environment
# Student: Asoka Niyonsaba Jean Claude (Student ID: 101410)
# ==============================================================================
FROM python:3.11-slim

WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    curl \
    netcat-openbsd \
    default-jre-headless \
    && rm -rf /var/lib/apt/lists/*

# Install Python requirements
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt flask

# Copy source code and scripts
COPY . .

EXPOSE 8080

CMD ["python3", "app.py"]

