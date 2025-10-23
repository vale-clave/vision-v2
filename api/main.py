from fastapi import FastAPI
from sse_starlette.sse import EventSourceResponse
from datetime import datetime, timedelta
from shared.db import get_conn, init_pool
from shared.settings import settings
import asyncio

# --- FIX: Añadir Middleware de CORS ---
from fastapi.middleware.cors import CORSMiddleware

init_pool()
app = FastAPI(title="Vision V2 API")

# --- FIX: Configurar CORS para producción y desarrollo ---
# Lista de orígenes permitidos
origins = [
    "https://www.clave.restaurant",
    "https://clave.restaurant",
    "http://localhost:3000",  # Para desarrollo local del frontend
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,  # Usamos la lista de orígenes permitidos
    allow_credentials=True,
    allow_methods=["*"],  # Permite todos los métodos (GET, POST, etc.)
    allow_headers=["*"],  # Permite todos los headers
)
# ------------------------------------

@app.get("/health")
def health():
    return {"status": "ok", "time": datetime.utcnow().isoformat()}


def _snapshot():
    """
    Calcula un snapshot de las métricas actuales (ocupación y dwell time)
    consultando la base de datos.
    """
    now = datetime.utcnow()
    metrics = {}

    with get_conn() as conn:
        with conn.cursor() as cur:
            # 1. Obtener la ocupación actual por zona
            # Para cada zona, contamos cuántos tracks tienen 'enter' como su último evento.
            cur.execute(
                """
                WITH last_events AS (
                    SELECT DISTINCT ON (zone_id, track_id)
                           zone_id,
                           event
                    FROM zone_events
                    ORDER BY zone_id, track_id, ts DESC
                )
                SELECT zone_id, COUNT(*) AS occupancy
                FROM last_events
                WHERE event = 'enter'
                GROUP BY zone_id;
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
                # FIX: Cast avg_dwell (que puede ser un Decimal) a float antes de redondear.
                if avg_dwell is not None:
                    metrics[zone_id]['avg_dwell_seconds_5m'] = round(float(avg_dwell), 2)

    # Formatear el resultado final
    data = {"timestamp": now.isoformat() + "Z", "zones": metrics}
    return data


@app.get("/realtime/stream")
async def stream():
    async def gen():
        while True:
            yield {"event": "metrics", "data": _snapshot()}
            await asyncio.sleep(2)
    return EventSourceResponse(gen())
