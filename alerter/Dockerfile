FROM python:3.9-slim

WORKDIR /app
ENV PYTHONPATH "${PYTHONPATH}:/app"

# Instalamos las dependencias de sistema
RUN apt-get update && apt-get install -y --no-install-recommends \
    && rm -rf /var/lib/apt/lists/* \
    && apt-get clean

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY shared/ ./shared/
COPY alerter/ ./alerter/

CMD ["python", "-u", "alerter/alerter.py"]
