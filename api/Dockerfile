FROM python:3.10-slim

WORKDIR /app
ENV PYTHONPATH "${PYTHONPATH}:/app"

# Usamos el requirements.txt del directorio raíz
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copiamos los directorios necesarios
COPY shared/ ./shared/
COPY api/ ./api/

# El comando para iniciar el servidor de la API
CMD ["uvicorn", "api.main:app", "--host", "0.0.0.0", "--port", "8000"]
