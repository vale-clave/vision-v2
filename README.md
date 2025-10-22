# Vision V2

Este directorio contiene la re-escritura modular del sistema de visión por computadora usando:

* TimescaleDB (extensión sobre PostgreSQL) como base de datos de series temporales
* Redis como cola de ingesta
* FastAPI para exponer métricas en tiempo real

Servicios previstos (contenedores):

1. **worker** → procesa RTSP con YOLOv8, publica eventos en Redis
2. **ingest** → consume de Redis y escribe lotes en TimescaleDB
3. **api** → consultas y streaming SSE/WebSocket

## Configuración rápida

1. Copia `.env.example` a `.env` y rellena:
   ```
   DATABASE_URL=postgres://…  # tu instancia Timescale
   REDIS_URL=redis://localhost:6379/0
   ```
2. `docker compose up --build`

## Configuración modular
Cada `tenant` declara cámaras y zonas en `config.yaml`. El script `python shared/config_loader.py` crea/actualiza estos registros y los umbrales de alerta.

```yaml
# Ejemplo mínimo
tenants:
  - id: 1
    name: Demo
    cameras:
      - id: 1
        rtsp_url: rtsp://...
        zones:
          - id: 10
            name: Interior
            thresholds:
              occupancy: 30
```

## Estructura de carpetas

```
vision-v2/
  api/
  ingest/
  worker/
  shared/
  docker-compose.yml
```
