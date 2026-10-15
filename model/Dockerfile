# One-shot trainer image: generates labelled data, trains the model, writes it to /models, exits.
# Built from the project root so it can copy the producer's generator (same data as the live stream).
FROM python:3.12-slim

ENV PYTHONUNBUFFERED=1
WORKDIR /app

COPY model/requirements.txt model/requirements.txt
RUN pip install --no-cache-dir -r model/requirements.txt

COPY producer/generator.py model/features.py model/train.py ./
CMD ["python", "train.py", "--out", "/models/model.joblib"]
