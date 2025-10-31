import json
import os
import time
from typing import List, Tuple

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

redis_client = redis.from_url(settings.redis_url.unicode_string(), decode_responses=True)
init_pool()


def _flush_batch(batch: List[Tuple]):
    """Intenta escribir el batch a la base de datos con reintentos"""
    if not batch:
        return
    
    for attempt in range(MAX_RETRIES):
        try:
            with get_conn() as conn:
                with conn.cursor() as cur:
                    execute_values(cur,
                        """
                        INSERT INTO zone_events (tenant_id, camera_id, zone_id, track_id, event, ts, dwell_seconds)
                        VALUES %s
                        """,
                        batch
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
                dwell = d.get("dwell")
                batch.append((
                    d.get("tenant_id", 1),
                    d["camera_id"],
                    d["zone_id"],
                    d["track_id"],
                    d["event"],
                    d["ts"],
                    dwell
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
