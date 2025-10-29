from fastapi import FastAPI
from sse_starlette.sse import EventSourceResponse
from datetime import datetime, timedelta
from shared.db import get_conn, init_pool
from shared.settings import settings
import asyncio
import json
from decimal import Decimal
import time
import psycopg2

# --- FIX: Añadir Middleware de CORS ---
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse
import redis.asyncio as redis # <- CAMBIO 1: Usamos la versión asíncrona


init_pool()
app = FastAPI(title="Vision V2 API")

# CAMBIO 2: Se define el cliente de Redis aquí, en el scope global y de forma asíncrona
redis_client = redis.from_url(settings.redis_url.unicode_string())

# --- Configurar CORS para producción y desarrollo ---
origins = [
    "https://www.clave.restaurant",
    "https://clave.restaurant",
    "http://localhost:3000",
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)
# ------------------------------------

# --- FIX: Codificador JSON robusto para manejar tipos de la BD como Decimal ---
def robust_json_encoder(obj):
    if isinstance(obj, Decimal):
        return float(obj)
    raise TypeError(f"Object of type {obj.__class__.__name__} is not JSON serializable")
# --------------------------------------------------------------------------

@app.get("/health")
def health():
    return {"status": "ok", "time": datetime.utcnow().isoformat()}

def _snapshot():
    """
    Calcula un snapshot de las métricas actuales (ocupación y dwell time)
    consultando la base de datos.
    Implementa reintentos para manejar conexiones de BD inestables.
    """
    now = datetime.utcnow()
    metrics = {}
    
    max_retries = 3
    for attempt in range(max_retries):
        try:
            with get_conn() as conn:
                with conn.cursor() as cur:
                    # 1. Obtener la ocupación actual por zona (consulta mejorada)
                    cur.execute(
                        """
                        WITH last_events AS (
                            SELECT DISTINCT ON (zone_id, track_id)
                                   zone_id,
                                   event,
                                   ts
                            FROM zone_events
                            ORDER BY zone_id, track_id, ts DESC
                        )
                        SELECT
                            le.zone_id,
                            COUNT(*) AS occupancy
                        FROM
                            last_events le
                        JOIN
                            zones z ON le.zone_id = z.id
                        WHERE
                            le.event = 'enter'
                            AND le.ts > NOW() - (z.ghost_timeout_minutes * INTERVAL '1 minute')
                        GROUP BY
                            le.zone_id;
                        """
                    )
                    occupancy_rows = cur.fetchall()
                    for row in occupancy_rows:
                        zone_id, occupancy = row
                        if zone_id not in metrics:
                            metrics[zone_id] = {}
                        metrics[zone_id]['occupancy'] = occupancy

                    # 2. Obtener el dwell time promedio de los últimos 5 minutos
                    cur.execute(
                        """
                        SELECT zone_id, AVG(dwell_seconds) AS avg_dwell_seconds_5m
                        FROM zone_events
                        WHERE ts > NOW() - INTERVAL '5 minutes' AND event = 'exit' AND dwell_seconds IS NOT NULL
                        GROUP BY zone_id;
                        """
                    )
                    dwell_rows = cur.fetchall()
                    for row in dwell_rows:
                        zone_id, avg_dwell = row
                        if zone_id not in metrics:
                            metrics[zone_id] = {}
                        if avg_dwell is not None:
                            metrics[zone_id]['avg_dwell_seconds_5m'] = avg_dwell
            
            # Si todo fue exitoso, salimos del bucle
            break

        except psycopg2.OperationalError as e:
            print(f"Error de conexión a la BD (intento {attempt + 1}/{max_retries}): {e}")
            if attempt < max_retries - 1:
                time.sleep(1)  # Esperar 1 segundo antes de reintentar
            else:
                print("No se pudo conectar a la base de datos después de varios intentos.")
                # Devolver métricas vacías si todos los reintentos fallan
                metrics = {} 
    
    # Formatear el resultado final
    data = {"timestamp": now.isoformat() + "Z", "zones": metrics}
    return data

@app.get("/realtime/stream")
async def stream():
    async def gen():
        while True:
            snapshot_data = _snapshot()
            # Convertimos manualmente el diccionario a un string JSON usando nuestro encoder robusto
            json_payload = json.dumps(snapshot_data, default=robust_json_encoder)
            yield {"event": "metrics", "data": json_payload}
            await asyncio.sleep(2)
    return EventSourceResponse(gen())

@app.get("/video/stream/{camera_id}")
async def video_stream(camera_id: int):
    async def frame_generator():
        while True:
            # CAMBIO 3: Usamos 'await' para la llamada asíncrona a Redis
            frame_bytes = await redis_client.get(f"annotated_frame_cam_{camera_id}")
            
            if frame_bytes:
                # El formato MJPEG requiere estos encabezados especiales entre cada frame
                yield (b'--frame\r\n'
                       b'Content-Type: image/jpeg\r\n\r\n' + frame_bytes + b'\r\n')
            
            # Controla la fluidez del stream. Un valor más bajo = más FPS (pero más carga en la red).
            # 0.05 equivale a ~20 FPS.
            await asyncio.sleep(0.05) 

    return StreamingResponse(frame_generator(), media_type="multipart/x-mixed-replace; boundary=frame")
