from pydantic_settings import BaseSettings
from pydantic import PostgresDsn, RedisDsn

class Settings(BaseSettings):
    database_url: PostgresDsn
    # Cambiamos el default a 'localhost' para que funcione en el entorno de Runpod
    redis_url: RedisDsn = "redis://localhost:6379/0"
    
    # --- Nuevas variables para el servicio de Alertas ---
    resend_api_key: str = "re_123456789" # <- Reemplazar con valor real en .env
    alert_email_to: str = "alerts@example.com" # <- Reemplazar con valor real en .env

    class Config:
        env_file = ".env"
        # La siguiente línea permite que las variables de entorno anulen los valores del .env
        env_file_encoding = 'utf-8'

settings = Settings()
