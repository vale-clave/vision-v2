FROM pytorch/pytorch:2.1.0-cuda12.1-cudnn8-runtime

# Instalar dependencias mínimas de sistema, incluyendo las de CV2
RUN apt-get update && apt-get install -y --no-install-recommends \
    python3-pip \
    libgl1-mesa-glx \
    libglib2.0-0 \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app
ENV PYTHONPATH "${PYTHONPATH}:/app"

COPY requirements.txt .
# La imagen ya viene con torch y torchvision.
# Solo instalamos el resto de dependencias, forzando una versión de numpy compatible.
RUN pip3 install --no-cache-dir "numpy<2" fastapi uvicorn redis psycopg2-binary python-dotenv pydantic pydantic-settings sse-starlette ultralytics shapely pyyaml

COPY config.yaml .
COPY shared/ ./shared/
COPY worker/ ./worker/

CMD ["python3", "-u", "worker/worker.py"]
