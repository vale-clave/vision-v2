import os
import time
from datetime import datetime
from decimal import Decimal
import resend

from shared.db import get_conn, init_pool
from shared.settings import settings
from alerter.email_templates import get_alert_html

# --- Configuración ---
LOOP_SLEEP_SECONDS = 30 # Comprobar alertas cada 30 segundos
# Configuración de Resend (desde .env)
resend.api_key = settings.resend_api_key
ALERT_EMAIL_TO = settings.alert_email_to

# --- Estado en memoria para Cooldown ---
# Guardará el estado de las alertas para no enviar spam.
# Formato: {(zone_id, metric): "triggered"}
alert_states = {}

def _get_current_metrics() -> dict:
    """
    Calcula las métricas actuales de ocupación y dwell time.
    Es una copia de la lógica del API para mantener consistencia.
    """
    metrics = {}
    with get_conn() as conn:
        with conn.cursor() as cur:
            # 1. Ocupación
            cur.execute(
                """
                WITH last_events AS (
                    SELECT DISTINCT ON (zone_id, track_id) zone_id, event, ts
                    FROM zone_events ORDER BY zone_id, track_id, ts DESC
                )
                SELECT zone_id, COUNT(*) AS occupancy
                FROM last_events
                WHERE event = 'enter' AND ts > NOW() - INTERVAL '20 minutes'
                GROUP BY zone_id;
                """
            )
            for row in cur.fetchall():
                zone_id, occupancy = row
                if zone_id not in metrics: metrics[zone_id] = {}
                metrics[zone_id]['occupancy'] = occupancy

            # 2. Dwell Time
            cur.execute(
                """
                SELECT zone_id, AVG(dwell_seconds) AS avg_dwell
                FROM zone_events
                WHERE ts > NOW() - INTERVAL '5 minutes' AND event = 'exit' AND dwell_seconds IS NOT NULL
                GROUP BY zone_id;
                """
            )
            for row in cur.fetchall():
                zone_id, avg_dwell = row
                if zone_id not in metrics: metrics[zone_id] = {}
                if avg_dwell is not None:
                    metrics[zone_id]['dwell'] = float(avg_dwell)
    return metrics

def _check_alerts():
    """Bucle principal que comprueba y envía alertas."""
    
    print(f"[{datetime.now()}] Chequeando alertas...")
    
    current_metrics = _get_current_metrics()
    
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT zt.zone_id, z.name, c.name, zt.metric, zt.threshold, zt.level
                FROM zone_thresholds zt
                JOIN zones z ON zt.zone_id = z.id
                JOIN cameras c ON z.camera_id = c.id
                """
            )
            thresholds = cur.fetchall()

    for zone_id, zone_name, cam_name, metric, threshold, level in thresholds:
        key = (zone_id, metric)
        
        # Obtener el valor actual de la métrica para esta zona
        current_value = current_metrics.get(zone_id, {}).get(metric)

        if current_value is None:
            continue

        # Comprobar si se supera el umbral
        is_exceeded = current_value > threshold

        # Lógica de Cooldown
        if is_exceeded and not alert_states.get(key):
            # --- ¡ALERTA! ---
            print(f"ALERTA DISPARADA: Zona '{zone_name}', Métrica '{metric}', Valor '{current_value}' > Umbral '{threshold}'")
            
            # 1. Marcar estado como "triggered" para no volver a enviar
            alert_states[key] = "triggered"
            
            # 2. Enviar email
            try:
                from_email, subject, html = get_alert_html(
                    metric=metric,
                    level=level,
                    value=current_value,
                    threshold=threshold,
                    zone_name=zone_name,
                    camera_name=cam_name
                )
                params = {
                    "from": from_email,
                    "to": [ALERT_EMAIL_TO],
                    "subject": subject,
                    "html": html,
                }
                resend.Emails.send(params)
                print(" -> Email de alerta enviado con éxito.")
            except Exception as e:
                print(f" -> ERROR al enviar email: {e}")

        elif not is_exceeded and alert_states.get(key):
            # La situación volvió a la normalidad, reseteamos el estado
            print(f"NORMALIDAD: Zona '{zone_name}', Métrica '{metric}' ha vuelto a la normalidad.")
            alert_states.pop(key)


if __name__ == "__main__":
    init_pool()
    print("Iniciando servicio de Alertas...")
    while True:
        try:
            _check_alerts()
        except Exception as e:
            print(f"ERROR en el ciclo principal de alertas: {e}")
        time.sleep(LOOP_SLEEP_SECONDS)
