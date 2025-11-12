import json
import os
import time
from typing import List, Tuple, Set
from datetime import datetime, timedelta

import redis
from psycopg2.extras import execute_values
from psycopg2 import OperationalError, InterfaceError

from shared.db import get_conn, init_pool
from shared.settings import settings

BATCH_SIZE = int(os.getenv("BATCH_SIZE", 200))
SLEEP_SEC = float(os.getenv("LOOP_SLEEP", 0.2))
QUEUE_KEY = os.getenv("REDIS_QUEUE", "detections_queue")
MAX_RETRIES = int(os.getenv("DB_MAX_RETRIES", 5))
RETRY_DELAY = float(os.getenv("DB_RETRY_DELAY", 2.0))
# Ventana de tiempo para deduplicación (segundos)
DEDUP_TIME_WINDOW = float(os.getenv("DEDUP_TIME_WINDOW", 3.0))

redis_client = redis.from_url(settings.redis_url.unicode_string(), decode_responses=True)
init_pool()

# Cache en memoria para deduplicación rápida (últimos eventos procesados)
# Estructura: {(camera_id, zone_id, track_id, event, ts_bucket): True}
# ts_bucket es el timestamp redondeado a la ventana de tiempo
_dedup_cache: Set[Tuple[int, int, int, str, float]] = set()
_cache_cleanup_time = time.time()


def _create_dedup_key(camera_id: int, zone_id: int, track_id: int, event: str, ts: str) -> Tuple[int, int, int, str, float]:
    """Crea una clave de deduplicación basada en los campos del evento"""
    # Parsear timestamp y redondear a la ventana de tiempo
    try:
        ts_dt = datetime.fromisoformat(ts.replace('Z', '+00:00'))
        ts_epoch = ts_dt.timestamp()
        # Redondear a la ventana de tiempo para agrupar eventos muy cercanos
        ts_bucket = round(ts_epoch / DEDUP_TIME_WINDOW) * DEDUP_TIME_WINDOW
    except Exception:
        # Si falla el parsing, usar el timestamp actual
        ts_bucket = round(time.time() / DEDUP_TIME_WINDOW) * DEDUP_TIME_WINDOW
    
    return (camera_id, zone_id, track_id, event, ts_bucket)


def _is_duplicate(camera_id: int, zone_id: int, track_id: int, event: str, ts: str) -> bool:
    """Verifica si un evento es duplicado usando el cache en memoria"""
    global _dedup_cache, _cache_cleanup_time
    
    # Limpiar cache cada 5 minutos
    current_time = time.time()
    if current_time - _cache_cleanup_time > 300:
        # Mantener solo eventos de los últimos 2 minutos
        cutoff_bucket = round((current_time - 120) / DEDUP_TIME_WINDOW) * DEDUP_TIME_WINDOW
        _dedup_cache = {k for k in _dedup_cache if k[4] >= cutoff_bucket}
        _cache_cleanup_time = current_time
    
    dedup_key = _create_dedup_key(camera_id, zone_id, track_id, event, ts)
    
    if dedup_key in _dedup_cache:
        return True
    
    # Añadir al cache
    _dedup_cache.add(dedup_key)
    return False


def _flush_batch(batch: List[Tuple]):
    """Intenta escribir el batch a la base de datos con reintentos y deduplicación"""
    if not batch:
        return
    
    # Filtrar duplicados antes de insertar
    filtered_batch = []
    for item in batch:
        camera_id, zone_id, track_id, event, ts = item
        
        # Verificar duplicados usando cache en memoria
        if not _is_duplicate(camera_id, zone_id, track_id, event, ts):
            filtered_batch.append(item)
    
    if not filtered_batch:
        return
    
    # Si filtramos algunos duplicados, loguear
    if len(filtered_batch) < len(batch):
        print(f"Deduplicación: {len(batch) - len(filtered_batch)} eventos duplicados filtrados de {len(batch)} totales")
    
    for attempt in range(MAX_RETRIES):
        try:
            with get_conn() as conn:
                with conn.cursor() as cur:
                    # Usar ON CONFLICT para evitar duplicados en la base de datos también
                    # Nota: Esto requiere un índice único o constraint, pero como no lo tenemos,
                    # usamos una verificación previa con una consulta
                    
                    # Verificar duplicados en la base de datos para los últimos 5 segundos
                    if filtered_batch:
                        # Extraer timestamps y crear una consulta de verificación
                        ts_values = [item[4] for item in filtered_batch]
                        min_ts = min(ts_values)
                        max_ts = max(ts_values)
                        
                        # Expandir ventana de búsqueda un poco
                        check_start = datetime.fromisoformat(min_ts.replace('Z', '+00:00')) - timedelta(seconds=DEDUP_TIME_WINDOW)
                        check_end = datetime.fromisoformat(max_ts.replace('Z', '+00:00')) + timedelta(seconds=DEDUP_TIME_WINDOW)
                        
                        # Verificar eventos existentes en la ventana de tiempo
                        cur.execute(
                            """
                            SELECT camera_id, zone_id, track_id, event, ts
                            FROM raw_vision_rogers.zone_events
                            WHERE ts >= %s AND ts <= %s
                            """,
                            (check_start, check_end)
                        )
                        existing_events = {row for row in cur.fetchall()}
                        
                        # Filtrar eventos que ya existen en la BD
                        final_batch = []
                        for item in filtered_batch:
                            camera_id, zone_id, track_id, event, ts = item
                            ts_dt = datetime.fromisoformat(ts.replace('Z', '+00:00'))
                            
                            # Verificar si existe un evento similar en la ventana de tiempo
                            is_duplicate = False
                            for existing in existing_events:
                                ex_cam, ex_zone, ex_track, ex_event, ex_ts = existing
                                if (ex_cam == camera_id and ex_zone == zone_id and 
                                    ex_track == track_id and ex_event == event):
                                    # Verificar si están dentro de la ventana de tiempo
                                    time_diff = abs((ex_ts - ts_dt).total_seconds())
                                    if time_diff < DEDUP_TIME_WINDOW:
                                        is_duplicate = True
                                        break
                            
                            if not is_duplicate:
                                final_batch.append(item)
                        
                        if not final_batch:
                            print("Todos los eventos del batch ya existen en la BD")
                            return
                        
                        filtered_batch = final_batch
                    
                    execute_values(cur,
                        """
                        INSERT INTO raw_vision_rogers.zone_events (camera_id, zone_id, track_id, event, ts)
                        VALUES %s
                        """,
                        filtered_batch
                    )
                conn.commit()
            # Si llegamos aquí, el commit fue exitoso
            return
            
        except (OperationalError, InterfaceError) as e:
            error_msg = str(e)
            print(f"Error al escribir batch a la BD (intento {attempt + 1}/{MAX_RETRIES}): {error_msg}")
            
            if attempt < MAX_RETRIES - 1:
                # Esperar antes de reintentar, con backoff exponencial
                delay = RETRY_DELAY * (2 ** attempt)
                print(f"Reintentando en {delay} segundos...")
                time.sleep(delay)
            else:
                # Si todos los reintentos fallan, registrar el error pero no perder el batch
                print(f"ERROR CRÍTICO: No se pudo escribir batch después de {MAX_RETRIES} intentos.")
                print(f"Batch perdido contiene {len(batch)} eventos")
                # Podrías implementar aquí un mecanismo de fallback (ej: escribir a un archivo)
                raise
                
        except Exception as e:
            # Manejar otros errores, incluyendo pool agotado
            error_msg = str(e).lower()
            if "pool" in error_msg and ("exhausted" in error_msg or "timeout" in error_msg):
                print(f"Pool de conexiones agotado (intento {attempt + 1}/{MAX_RETRIES}): {e}")
                
                if attempt < MAX_RETRIES - 1:
                    # Esperar más tiempo cuando el pool está agotado
                    delay = RETRY_DELAY * (2 ** attempt) + 2
                    print(f"Esperando {delay} segundos antes de reintentar...")
                    time.sleep(delay)
                    # Forzar recreación del pool
                    init_pool()
                else:
                    print(f"ERROR CRÍTICO: Pool agotado después de {MAX_RETRIES} intentos.")
                    raise
            else:
                # Otros errores inesperados
                print(f"Error inesperado al escribir batch: {e}")
                raise


def main():
    batch: List[Tuple] = []
    consecutive_errors = 0
    max_consecutive_errors = 10
    
    while True:
        item = redis_client.lpop(QUEUE_KEY)
        if item:
            try:
                d = json.loads(item)
                # Omitimos tenant_id y dwell, ya que no están en la nueva tabla de ingesta
                batch.append((
                    d["camera_id"],
                    d["zone_id"],
                    d["track_id"],
                    d["event"],
                    d["ts"],
                ))
            except Exception as e:
                print(f"Error al parsear item de Redis: {e}")
                continue
            
            if len(batch) >= BATCH_SIZE:
                try:
                    _flush_batch(batch)
                    batch = []
                    consecutive_errors = 0  # Resetear contador de errores
                except Exception as e:
                    consecutive_errors += 1
                    print(f"Error al escribir batch: {e}")
                    
                    if consecutive_errors >= max_consecutive_errors:
                        print(f"Demasiados errores consecutivos ({consecutive_errors}). Esperando más tiempo antes de reintentar...")
                        time.sleep(10)  # Esperar más tiempo si hay muchos errores
                        consecutive_errors = 0
                    else:
                        # Mantener el batch para reintentar más tarde
                        time.sleep(1)
        else:
            if batch:
                try:
                    _flush_batch(batch)
                    batch = []
                    consecutive_errors = 0
                except Exception as e:
                    consecutive_errors += 1
                    print(f"Error al escribir batch final: {e}")
                    
                    if consecutive_errors >= max_consecutive_errors:
                        print(f"Demasiados errores consecutivos ({consecutive_errors}). Esperando más tiempo...")
                        time.sleep(10)
                        consecutive_errors = 0
                    else:
                        time.sleep(1)
                        continue  # Mantener el batch para el siguiente ciclo
            time.sleep(SLEEP_SEC)


if __name__ == "__main__":
    main()
