import psycopg2
from psycopg2.pool import ThreadedConnectionPool
from contextlib import contextmanager
from .settings import settings

_POOL: ThreadedConnectionPool | None = None


def init_pool(minconn: int = 1, maxconn: int = 10):
    global _POOL
    if _POOL is None:
        _POOL = ThreadedConnectionPool(minconn, maxconn, settings.database_url.unicode_string())


@contextmanager
def get_conn():
    if _POOL is None:
        init_pool()
    conn = _POOL.getconn()
    try:
        yield conn
    finally:
        _POOL.putconn(conn)
