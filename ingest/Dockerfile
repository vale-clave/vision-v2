FROM python:3.10-slim

WORKDIR /app
ENV PYTHONPATH "${PYTHONPATH}:/app"

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY shared/ ./shared/
COPY ingest/ ./ingest/

CMD ["python", "-u", "ingest/ingest.py"]
