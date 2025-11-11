from pydantic_settings import BaseSettings
from pydantic import PostgresDsn, RedisDsn, field_validator
from typing import List

class Settings(BaseSettings):
    database_url: PostgresDsn
    # Cambiamos el default a 'localhost' para que funcione en el entorno de Runpod
    redis_url: RedisDsn = "redis://localhost:6379/0"
    
    # --- Nuevas variables para el servicio de Alertas ---
    resend_api_key: str | None = None
    resend_from_email: str = "Clave Alerts <noreply@alerts.tryclave.ai>"
    alert_email_to: List[str] | None = None
    google_api_key: str | None = None

    @field_validator('alert_email_to', mode='before')
    @classmethod
    def parse_alert_emails(cls, v):
        """Convierte un string separado por comas en una lista de emails."""
        if v is None:
            return None
        if isinstance(v, list):
            return v
        if isinstance(v, str):
            # Dividir por comas, limpiar espacios y filtrar vacíos
            emails = [email.strip() for email in v.split(',') if email.strip()]
            return emails if emails else None
        return v

    class Config:
        env_file = ".env"
        # La siguiente línea permite que las variables de entorno anulen los valores del .env
        env_file_encoding = 'utf-8'

settings = Settings()
