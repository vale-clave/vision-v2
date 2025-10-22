from pydantic_settings import BaseSettings
from pydantic import PostgresDsn, RedisDsn

class Settings(BaseSettings):
    database_url: PostgresDsn
    # Cambiamos el default a 'redis' para que funcione dentro de Docker Compose
    redis_url: RedisDsn = "redis://redis:6379/0"

    class Config:
        env_file = ".env"
        # La siguiente línea permite que las variables de entorno anulen los valores del .env
        env_file_encoding = 'utf-8'

settings = Settings()
