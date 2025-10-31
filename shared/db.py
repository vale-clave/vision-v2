import psycopg2
from psycopg2.pool import ThreadedConnectionPool
from psycopg2 import OperationalError, InterfaceError
from contextlib import contextmanager
from .settings import settings
import time

_POOL: ThreadedConnectionPool | None = None


def init_pool(minconn: int = 1, maxconn: int = 10):
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
    global _POOL
    if _POOL is None:
        init_pool()
    
    conn = None
    connection_returned = False
    
    for attempt in range(max_retries):
        try:
            conn = _POOL.getconn()
            
            # Verificar si la conexión está activa
            if _is_connection_closed(conn):
                # La conexión está cerrada, intentar cerrarla y obtener una nueva
                try:
                    _POOL.putconn(conn, close=True)
                    connection_returned = True
                except Exception:
                    pass
                
                # Si falla múltiples veces, recrear el pool
                if attempt >= max_retries - 1:
                    print("Recreando pool de conexiones debido a conexiones cerradas")
                    init_pool()
                    conn = _POOL.getconn()
                    connection_returned = False
                else:
                    conn = None
                    time.sleep(0.5)
                    continue
            
            # Si llegamos aquí, tenemos una conexión válida
            try:
                yield conn
                connection_returned = True
                return
            finally:
                # Devolver la conexión al pool solo si no se devolvió antes
                if conn and not connection_returned:
                    try:
                        _POOL.putconn(conn)
                        connection_returned = True
                    except Exception:
                        pass
            
        except (OperationalError, InterfaceError) as e:
            print(f"Error de conexión (intento {attempt + 1}/{max_retries}): {e}")
            if conn and not connection_returned:
                try:
                    _POOL.putconn(conn, close=True)
                    connection_returned = True
                except Exception:
                    pass
            
            if attempt < max_retries - 1:
                # Esperar antes de reintentar
                time.sleep(1)
                # Recrear el pool si es necesario
                if attempt >= 1:
                    print("Recreando pool de conexiones")
                    init_pool()
                conn = None
                connection_returned = False
            else:
                raise
