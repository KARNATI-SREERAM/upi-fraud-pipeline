# Image for the stream processor (Kafka consumer -> validation -> PostgreSQL).
FROM python:3.12-slim

ENV PYTHONUNBUFFERED=1
WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY validate.py store.py main.py ./
CMD ["python", "main.py"]
