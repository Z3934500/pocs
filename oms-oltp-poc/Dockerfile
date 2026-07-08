FROM python:3.13-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY pyproject.toml .
COPY src ./src
COPY frontend ./frontend

ENV PYTHONPATH=/app/src

EXPOSE 8000

CMD ["uvicorn", "oms_oltp.api:app", "--host", "0.0.0.0", "--port", "8000"]
