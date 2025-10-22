FROM python:3.10-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY shared ./shared
COPY config.yaml .

# El comando a ejecutar se define en el archivo docker-compose.yml
# Este Dockerfile solo crea el entorno necesario.
