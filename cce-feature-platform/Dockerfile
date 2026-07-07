FROM python:3.12-slim

WORKDIR /app

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1
ENV PYTHONPATH=/app/src

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY src ./src
COPY frontend ./frontend

EXPOSE 8000

CMD ["sh", "-c", "python -m cce_platform.pipeline run && uvicorn cce_platform.api:app --host 0.0.0.0 --port 8000"]
