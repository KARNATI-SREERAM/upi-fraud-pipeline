# Image for the transaction producer. Slim base keeps the image small for an 8 GB laptop.
FROM python:3.12-slim

# Print logs immediately instead of buffering them, so `docker compose logs` is live.
ENV PYTHONUNBUFFERED=1
WORKDIR /app

# Copy requirements first: Docker caches this layer, so code edits don't re-run pip install.
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY generator.py main.py ./
# Exec form (JSON list) makes python PID 1, so it receives SIGTERM and can flush on shutdown.
CMD ["python", "main.py"]
