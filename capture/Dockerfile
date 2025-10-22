FROM python:3.9-slim

WORKDIR /app
ENV PYTHONPATH "${PYTHONPATH}:/app"

# Instalamos las dependencias de sistema necesarias para OpenCV y video
RUN apt-get update && apt-get install -y --no-install-recommends \
    ffmpeg \
    libsm6 \
    libxext6 \
    libxrender1 \
    libgomp1 \
    libgl1 \
    libglib2.0-0 \
    && rm -rf /var/lib/apt/lists/* \
    && apt-get clean

COPY requirements.txt .
# Omitimos torch, torchvision y ultralytics para este servicio
RUN pip install --no-cache-dir fastapi uvicorn redis psycopg2-binary python-dotenv pydantic pydantic-settings sse-starlette opencv-python-headless shapely pyyaml

COPY config.yaml .
COPY shared/ ./shared/
COPY capture/ ./capture/

CMD ["python", "-u", "capture/capture.py"]
