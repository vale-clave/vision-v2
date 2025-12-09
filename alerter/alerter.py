import os
import time
from datetime import datetime
from decimal import Decimal
import resend

from shared.db import get_conn, init_pool
from shared.settings import settings
from alerter.email_templates import get_alert_html
import json
import redis
from collections import deque

# --- Configuración ---
LOOP_SLEEP_SECONDS = 30 # Comprobar alertas cada 30 segundos
# Configuración de Resend (desde .env)
resend.api_key = settings.resend_api_key
ALERT_EMAIL_TO = settings.alert_email_to

# --- Estado en memoria para Cooldown ---
# Guardará el estado de las alertas para no enviar spam.
# Formato: {(zone_id, metric): "triggered"}
alert_states = {}
# Tiempo que se viene superando el umbral por zona/métrica
# Formato: {(zone_id, metric): datetime_inicio}
exceed_since = {}
# Historial de hits por ventana (modo voto)
# Formato: {(zone_id, metric): deque[timestamps]}
exceed_hits: dict[tuple[int, str], deque] = {}
# Tiempo que se viene manteniendo por debajo del umbral (para normalidad)
# Formato: {(zone_id, metric): datetime_inicio_bajo}
below_since: dict[tuple[int, str], datetime] = {}

def _get_current_metrics() -> dict:
    """
    Calcula las métricas actuales:
    1) Intenta Redis (tiempo real, consistente con dashboard).
    2) Fallback a BD si Redis no está disponible.
    """
    metrics: dict = {}

    # 1) Redis
    try:
        r = redis.from_url(settings.redis_url.unicode_string())
        keys = r.keys("occupancy_cam_*")
        for key in keys:
            raw = r.get(key)
            if not raw:
                continue
            try:
                data = json.loads(raw)
                zones = data.get("zones", {})
                for zone_id_str, occ in zones.items():
                    zone_id = int(zone_id_str)
                    metrics.setdefault(zone_id, {})["occupancy"] = int(occ)
            except Exception:
                continue
    except Exception:
        pass

    # 2) Fallback a BD
    if not metrics:
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    WITH time_window AS (
                        SELECT NOW() - INTERVAL '15 minutes' AS start_ts,
                               NOW() AS end_ts
                    ),
                    starting_occupancy AS (
                        SELECT
                            ze.zone_id,
                            GREATEST(0, COALESCE(SUM(CASE WHEN ze.event = 'enter' THEN 1 ELSE -1 END), 0)) AS occupancy
                        FROM raw_vision_socado.zone_events ze, time_window tw
                        WHERE ze.ts < tw.start_ts
                        GROUP BY ze.zone_id
                    ),
                    events_in_window AS (
                        SELECT ze.zone_id, ze.ts, ze.event
                        FROM raw_vision_socado.zone_events ze, time_window tw
                        WHERE ze.ts >= tw.start_ts AND ze.ts <= tw.end_ts
                    ),
                    occupancy_changes AS (
                        SELECT
                            zone_id,
                            ts,
                            SUM(CASE WHEN event = 'enter' THEN 1 ELSE -1 END)
                                OVER (PARTITION BY zone_id ORDER BY ts
                                      ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_change
                        FROM events_in_window
                    ),
                    latest_change AS (
                        SELECT DISTINCT ON (zone_id)
                               zone_id,
                               cumulative_change
                        FROM occupancy_changes
                        ORDER BY zone_id, ts DESC
                    )
                    SELECT
                        z.zone_id,
                        GREATEST(0,
                            COALESCE(so.occupancy, 0) +
                            COALESCE(lc.cumulative_change, 0)
                        ) AS occupancy
                    FROM (SELECT DISTINCT zone_id FROM raw_vision_socado.zone_events) z
                    LEFT JOIN starting_occupancy so ON z.zone_id = so.zone_id
                    LEFT JOIN latest_change lc ON z.zone_id = lc.zone_id;
                    """
                )
                for row in cur.fetchall():
                    zone_id, occupancy = row
                    metrics.setdefault(zone_id, {})["occupancy"] = int(occupancy)

    # Dwell informativo (no se usa para disparo)
    try:
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT zone_id, AVG(avg_dwell_seconds) AS avg_dwell
                    FROM analytics.fact_vision_metrics_hourly
                    WHERE hour > NOW() - INTERVAL '1 hour'
                    GROUP BY zone_id;
                    """
                )
                for row in cur.fetchall():
                    zone_id, avg_dwell = row
                    if avg_dwell is not None:
                        metrics.setdefault(zone_id, {})["dwell"] = float(avg_dwell)
    except Exception:
        pass
    return metrics

def _check_alerts():
    """Bucle principal que comprueba y envía alertas."""
    
    print(f"[{datetime.now()}] Chequeando alertas...")
    
    current_metrics = _get_current_metrics()
    min_exceed_seconds = int(getattr(settings, "alert_min_exceed_seconds", 0) or 0)
    vote_window_seconds = int(getattr(settings, "alert_vote_window_seconds", 0) or 0)
    vote_min_hits = int(getattr(settings, "alert_vote_min_hits", 0) or 0)
    min_below_seconds = int(getattr(settings, "alert_min_below_seconds", 0) or 0)
    
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT zt.zone_id, z.name, c.name, zt.metric, zt.threshold, zt.level
                FROM raw_vision_socado.zone_thresholds zt
                JOIN raw_vision_socado.zones z ON zt.zone_id = z.id
                JOIN raw_vision_socado.cameras c ON z.camera_id = c.id
                """
            )
            thresholds = cur.fetchall()

    for zone_id, zone_name, cam_name, metric, threshold, level in thresholds:
        key = (zone_id, metric)
        
        # Obtener el valor actual de la métrica para esta zona
        current_value = current_metrics.get(zone_id, {}).get(metric)

        if current_value is None:
            continue

        # Comprobar si se supera el umbral (inclusivo)
        is_exceeded = current_value >= threshold

        # Registrar hits para modo voto
        now = datetime.now()
        if vote_window_seconds > 0 and vote_min_hits > 0:
            dq = exceed_hits.setdefault(key, deque())
            # Mantener solo timestamps dentro de la ventana
            cutoff = now.timestamp() - vote_window_seconds
            while dq and dq[0] < cutoff:
                dq.popleft()
            if is_exceeded:
                dq.append(now.timestamp())
            else:
                # No contamos hit; solo purga arriba
                pass

        # Control de persistencia sobre el umbral + Cooldown (modo tiempo continuo)
        if is_exceeded and min_exceed_seconds > 0:
            if key not in exceed_since:
                exceed_since[key] = now
            elapsed = (now - exceed_since[key]).total_seconds()
            if elapsed >= min_exceed_seconds and not alert_states.get(key):
                # --- ¡ALERTA! ---
                print(f"ALERTA DISPARADA: Zona '{zone_name}', Métrica '{metric}', Valor '{current_value}' > Umbral '{threshold}' por {int(elapsed)}s (mín {min_exceed_seconds}s)")
                
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
                        "to": ALERT_EMAIL_TO,
                        "subject": subject,
                        "html": html,
                    }
                    resend.Emails.send(params)
                    print(" -> Email de alerta enviado con éxito.")
                except Exception as e:
                    print(f" -> ERROR al enviar email: {e}")
            # Al estar sobre umbral, reseteamos contador de bajo-umbral
            if key in below_since:
                below_since.pop(key)

        # Control de mayoría por ventana (modo voto)
        elif (vote_window_seconds > 0 and vote_min_hits > 0
              and not alert_states.get(key)
              and is_exceeded):
            dq = exceed_hits.setdefault(key, deque())
            # Purga por si no se purgó arriba
            cutoff = now.timestamp() - vote_window_seconds
            while dq and dq[0] < cutoff:
                dq.popleft()
            if len(dq) >= vote_min_hits:
                print(f"ALERTA DISPARADA (voto): Zona '{zone_name}', Métrica '{metric}', valor={current_value} >= {threshold}, hits={len(dq)}/{vote_min_hits} en {vote_window_seconds}s")
                alert_states[key] = "triggered"
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
                        "to": ALERT_EMAIL_TO,
                        "subject": subject,
                        "html": html,
                    }
                    resend.Emails.send(params)
                    print(" -> Email de alerta enviado con éxito.")
                except Exception as e:
                    print(f" -> ERROR al enviar email: {e}")
            # Si no superó por voto, y está bajo umbral, acumulamos tiempo bajo
            if not is_exceeded and alert_states.get(key):
                if key not in below_since:
                    below_since[key] = now

        elif not is_exceeded and alert_states.get(key):
            # Está por debajo del umbral mientras hay alerta activa: aplicar histeresis
            if key not in below_since:
                below_since[key] = now
            elapsed_below = (now - below_since[key]).total_seconds()
            if elapsed_below >= min_below_seconds:
                print(f"NORMALIDAD: Zona '{zone_name}', Métrica '{metric}' bajo umbral por {int(elapsed_below)}s (mín {min_below_seconds}s).")
                alert_states.pop(key)
                # Reiniciar estados
                if key in exceed_since:
                    exceed_since.pop(key)
                if key in below_since:
                    below_since.pop(key)
                # Limpiar hits si la ventana quedó vacía
                if key in exceed_hits:
                    dq = exceed_hits[key]
                    cutoff = now.timestamp() - vote_window_seconds if vote_window_seconds > 0 else now.timestamp()
                    while dq and dq[0] < cutoff:
                        dq.popleft()
                    if not dq:
                        exceed_hits.pop(key)
        elif not is_exceeded:
            # No superado: reiniciar contador de persistencia si existía
            if key in exceed_since:
                exceed_since.pop(key)
            # Si no hay alerta activa, reiniciar también el contador de bajo-umbral
            if key in below_since and not alert_states.get(key):
                elapsed_below = (now - below_since[key]).total_seconds()
                # Mantener below_since para medir histeresis si se activa alerta? Solo necesario con alerta activa.
                below_since.pop(key)


if __name__ == "__main__":
    init_pool()
    print("Iniciando servicio de Alertas...")
    while True:
        try:
            _check_alerts()
        except Exception as e:
            print(f"ERROR en el ciclo principal de alertas: {e}")
        time.sleep(LOOP_SLEEP_SECONDS)
