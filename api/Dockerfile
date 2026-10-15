# Image for the FastAPI service. Built from the project root so it can reuse the pipeline's
# validation, history query, feature and scoring code (one source of truth for each).
FROM python:3.12-slim

ENV PYTHONUNBUFFERED=1
WORKDIR /app

COPY model/requirements.txt model/requirements.txt
COPY api/requirements.txt api/requirements.txt
RUN pip install --no-cache-dir -r api/requirements.txt

COPY api/app.py processor/validate.py processor/store.py model/features.py model/scorer.py ./
EXPOSE 8000
CMD ["uvicorn", "app:app", "--host", "0.0.0.0", "--port", "8000"]
