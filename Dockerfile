# syntax=docker/dockerfile:1

FROM python:3.12-slim

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1

WORKDIR /app

COPY requirements.txt ./

RUN pip install --upgrade --no-cache-dir pip \
    && pip install --no-cache-dir -r requirements.txt \
    # Install production process manager (gunicorn) and uvicorn worker class
    && pip install --no-cache-dir gunicorn uvicorn

COPY . .

EXPOSE 8000

ENV APP_ENV=production

# Run with Gunicorn using Uvicorn workers in production
CMD ["gunicorn", "-k", "uvicorn.workers.UvicornWorker", "app.main:app", "--bind", "0.0.0.0:8000", "--workers", "4", "--log-level", "info"]
