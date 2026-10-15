# Image for the stream processor (Kafka consumer -> validate -> score -> PostgreSQL).
# Built from the project root (see docker-compose.yml) so it can copy the shared model code.
FROM python:3.12-slim

ENV PYTHONUNBUFFERED=1
WORKDIR /app

# Keep the folder layout so processor/requirements.txt can include ../model/requirements.txt.
COPY model/requirements.txt model/requirements.txt
COPY processor/requirements.txt processor/requirements.txt
RUN pip install --no-cache-dir -r processor/requirements.txt

COPY processor/validate.py processor/store.py processor/pipeline.py model/features.py model/scorer.py ./
CMD ["python", "pipeline.py"]
