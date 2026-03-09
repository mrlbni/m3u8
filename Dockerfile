FROM python:3.11-slim

# Install system dependencies
RUN apt-get update && apt-get install -y \
    ffmpeg \
    gcc \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Copy requirements first for better caching
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy bot code
COPY m3u8_bot.py .

# Create directories
RUN mkdir -p downloads temp logs

# Expose port for health checks
EXPOSE 5000

# Run with gunicorn for Flask and Python for bot
CMD gunicorn m3u8_bot:app --bind 0.0.0.0:$PORT & python m3u8_bot.py