import psycopg2
from psycopg2.pool import ThreadedConnectionPool
from psycopg2 import OperationalError, InterfaceError
from contextlib import contextmanager
from .settings import settings
import time

_POOL: ThreadedConnectionPool | None = None


def init_pool(minconn: int = 2, maxconn: int = 20):
    """Inicializa el pool de conexiones con más conexiones por defecto"""
    global _POOL
    if _POOL is not None:
        try:
            _POOL.closeall()
        except Exception:
            pass
    _POOL = ThreadedConnectionPool(minconn, maxconn, settings.database_url.unicode_string())


def _is_connection_closed(conn):
    """Verifica si una conexión está cerrada o inválida"""
    if conn.closed:
        return True
    try:
        # Intentar ejecutar una consulta simple para verificar la conexión
        cursor = conn.cursor()
        cursor.execute("SELECT 1")
        cursor.close()
        return False
    except (OperationalError, InterfaceError):
        return True


@contextmanager
def get_conn(max_retries: int = 3):
    """Obtiene una conexión del pool con manejo robusto de errores"""
    global _POOL
    if _POOL is None:
        init_pool()
    
    conn = None
    for attempt in range(max_retries):
        try:
            # Intentar obtener una conexión del pool
            try:
                conn = _POOL.getconn()
            except Exception as e:
                # Manejar errores del pool (incluyendo pool agotado)
                error_msg = str(e).lower()
                if "pool" in error_msg and ("exhausted" in error_msg or "timeout" in error_msg):
                    print(f"Pool agotado (intento {attempt + 1}/{max_retries}). Esperando...")
                    if attempt < max_retries - 1:
                        time.sleep(1 + attempt)  # Backoff progresivo
                        continue
                    else:
                        # Si el pool está agotado después de varios intentos, recrearlo
                        print("Pool agotado después de varios intentos. Recreando pool...")
                        init_pool()
                        conn = _POOL.getconn()
                else:
                    # Otro tipo de error, relanzar
                    raise
            
            # Verificar si la conexión está activa (solo si no es la primera vez)
            if attempt > 0 and _is_connection_closed(conn):
                # La conexión está cerrada, cerrarla y obtener una nueva
                try:
                    _POOL.putconn(conn, close=True)
                except Exception:
                    pass
                
                if attempt >= max_retries - 1:
                    print("Recreando pool de conexiones debido a conexiones cerradas")
                    init_pool()
                    conn = _POOL.getconn()
                else:
                    conn = None
                    time.sleep(0.5)
                    continue
            
            # Si llegamos aquí, tenemos una conexión válida
            try:
                yield conn
            finally:
                # SIEMPRE devolver la conexión al pool
                if conn:
                    try:
                        # Verificar si la conexión está cerrada antes de devolverla
                        if conn.closed:
                            _POOL.putconn(conn, close=True)
                        else:
                            _POOL.putconn(conn)
                    except Exception as e:
                        print(f"Error al devolver conexión al pool: {e}")
                        # Si hay error, intentar cerrarla forzadamente
                        try:
                            if not conn.closed:
                                conn.close()
                        except Exception:
                            pass
            
            # Si llegamos aquí, todo fue exitoso
            return
            
        except (OperationalError, InterfaceError) as e:
            print(f"Error de conexión (intento {attempt + 1}/{max_retries}): {e}")
            if conn:
                try:
                    _POOL.putconn(conn, close=True)
                except Exception:
                    pass
            
            if attempt < max_retries - 1:
                time.sleep(1 + attempt)
                # Recrear el pool si es necesario
                if attempt >= 1:
                    print("Recreando pool de conexiones")
                    init_pool()
                conn = None
            else:
                raise
        except Exception as e:
            # Manejar cualquier otro error
            print(f"Error inesperado obteniendo conexión (intento {attempt + 1}/{max_retries}): {e}")
            if conn:
                try:
                    _POOL.putconn(conn, close=True)
                except Exception:
                    pass
            
            if attempt < max_retries - 1:
                time.sleep(1)
                conn = None
            else:
                raise
