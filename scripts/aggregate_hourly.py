import argparse
from datetime import datetime, timedelta, timezone

import psycopg2
from psycopg2.extras import DictCursor
from shared.db import get_conn


# ESTA ES UNA CONSULTA SQL COMPLETAMENTE REVISADA Y MEJORADA.
# Aborda los problemas de la versión anterior:
# 1. CÁLCULO DE OCUPACIÓN:
#    - Primero obtiene la ocupación final de la hora ANTERIOR ('starting_occupancy') para usarla como base.
#    - Luego crea una línea de tiempo detallada de todos los eventos 'enter' y 'exit' DENTRO de la hora actual.
#    - Calcula la ocupación en cada punto de tiempo sumando o restando de la ocupación inicial.
#    - Calcula el promedio de ocupación ponderado por el tiempo que duró cada estado de ocupación.
# 2. CÁLCULO DEL TIEMPO DE PERMANENCIA:
#    - Identifica todos los eventos 'enter' en la hora actual o antes, para pistas que todavía estaban dentro.
#    - Busca el correspondiente evento 'exit' INCLUSO SI OCURRE DESPUÉS de la hora actual.
#    - Esto captura correctamente los tiempos de permanencia que cruzan los límites de la hora.
#    - Limita el tiempo de permanencia contado a la hora actual para no contaminar las métricas de otras horas.
# 3. CONTEO DE ENTRADAS:
#    - Sigue siendo un simple recuento de eventos 'enter' dentro de la hora.
AGGREGATION_QUERY = """
WITH time_range AS (
    SELECT
        date_trunc('hour', %s::TIMESTAMPTZ at time zone 'America/Guayaquil') AS start_ts_local,
        date_trunc('hour', %s::TIMESTAMPTZ at time zone 'America/Guayaquil') + interval '1 hour' AS end_ts_local
),
time_range_utc AS (
    SELECT
        start_ts_local AT TIME ZONE 'America/Guayaquil' AS start_ts_utc,
        end_ts_local AT TIME ZONE 'America/Guayaquil' AS end_ts_utc
    FROM time_range
),
starting_occupancy AS (
    SELECT
        zone_id,
        COALESCE(SUM(CASE WHEN event = 'enter' THEN 1 ELSE -1 END), 0) AS occupancy
    FROM zone_events, time_range_utc
    WHERE ts < start_ts_utc
    GROUP BY zone_id
),
events_in_hour AS (
    SELECT
        ts,
        zone_id,
        track_id,
        event
    FROM zone_events, time_range_utc
    WHERE ts >= start_ts_utc AND ts < end_ts_utc
),
occupancy_changes AS (
    SELECT
        zone_id,
        ts,
        SUM(CASE WHEN event = 'enter' THEN 1 ELSE -1 END) OVER (PARTITION BY zone_id ORDER BY ts) AS net_change
    FROM events_in_hour
),
occupancy_timeline AS (
    SELECT
        oc.zone_id,
        oc.ts,
        COALESCE(so.occupancy, 0) + oc.net_change AS current_occupancy,
        LEAD(oc.ts, 1, (SELECT end_ts_utc FROM time_range_utc)) OVER (PARTITION BY oc.zone_id ORDER BY oc.ts) - oc.ts AS duration
    FROM occupancy_changes oc
    LEFT JOIN starting_occupancy so ON oc.zone_id = so.zone_id
),
occupancy_metrics AS (
    SELECT
        zone_id,
        SUM(current_occupancy * EXTRACT(EPOCH FROM duration)) / EXTRACT(EPOCH FROM ((SELECT end_ts_utc FROM time_range_utc) - (SELECT start_ts_utc FROM time_range_utc))) AS avg_occupancy,
        MAX(current_occupancy) AS max_occupancy
    FROM occupancy_timeline
    GROUP BY zone_id
),
entries_in_hour AS (
    SELECT
        zone_id,
        count(*) as total_entries
    FROM events_in_hour
    WHERE event = 'enter'
    GROUP BY zone_id
),
dwell_times AS (
    SELECT
        e.zone_id,
        EXTRACT(EPOCH FROM (LEAST(x.ts, (SELECT end_ts_utc FROM time_range_utc)) - GREATEST(e.ts, (SELECT start_ts_utc FROM time_range_utc)))) as dwell_seconds
    FROM
        zone_events e
    JOIN
        zone_events x ON e.track_id = x.track_id AND e.zone_id = x.zone_id
    CROSS JOIN time_range_utc tr
    WHERE
        e.event = 'enter' AND x.event = 'exit'
        AND e.ts < tr.end_ts_utc
        AND x.ts >= tr.start_ts_utc
),
final_metrics AS (
    SELECT
        z.id as zone_id,
        COALESCE(om.avg_occupancy, so.occupancy, 0) as avg_occupancy,
        COALESCE(om.max_occupancy, so.occupancy, 0) as max_occupancy,
        COALESCE(AVG(dt.dwell_seconds), 0) as avg_dwell_seconds,
        COALESCE(e.total_entries, 0) as total_entries
    FROM zones z
    LEFT JOIN starting_occupancy so ON z.id = so.zone_id
    LEFT JOIN occupancy_metrics om ON z.id = om.zone_id
    LEFT JOIN dwell_times dt ON z.id = dt.zone_id
    LEFT JOIN entries_in_hour e ON z.id = e.zone_id
    GROUP BY z.id, so.occupancy, om.avg_occupancy, om.max_occupancy, e.total_entries
)
INSERT INTO hourly_metrics (ts, zone_id, avg_occupancy, max_occupancy, avg_dwell_seconds, total_entries)
SELECT
    (SELECT start_ts_local FROM time_range),
    fm.zone_id,
    fm.avg_occupancy,
    fm.max_occupancy,
    fm.avg_dwell_seconds,
    fm.total_entries
FROM final_metrics fm
WHERE fm.zone_id IS NOT NULL
ON CONFLICT (ts, zone_id) DO UPDATE SET
    avg_occupancy = EXCLUDED.avg_occupancy,
    max_occupancy = EXCLUDED.max_occupancy,
    avg_dwell_seconds = EXCLUDED.avg_dwell_seconds,
    total_entries = EXCLUDED.total_entries;
"""


def run_aggregation(target_hour: datetime):
    """
    Ejecuta la agregación por hora para la hora especificada.
    """
    target_hour_str = target_hour.isoformat()
    print(f"Ejecutando agregación para la hora que comienza en: {target_hour_str}")
    try:
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(AGGREGATION_QUERY, (target_hour_str, target_hour_str))
                conn.commit()
        print("Agregación completada.")
    except psycopg2.Error as e:
        print(f"Error de base de datos: {e}")
        # En una aplicación real, aquí se podría hacer conn.rollback()


def main():
    parser = argparse.ArgumentParser(description="Ejecutar agregación por hora para métricas de visión.")
    parser.add_argument(
        "--hour",
        type=str,
        help="La hora a procesar en formato ISO 8601 y zona horaria de Ecuador (p. ej., '2025-10-28T14:00:00-05:00'). Por defecto, la hora anterior."
    )

    args = parser.parse_args()

    # Usar la zona horaria de Ecuador
    ecuador_tz = timezone(timedelta(hours=-5))

    if args.hour:
        target_hour = datetime.fromisoformat(args.hour)
    else:
        # Por defecto, el inicio de la hora anterior en la zona horaria de Ecuador
        now_ecuador = datetime.now(ecuador_tz)
        target_hour = (now_ecuador - timedelta(hours=1)).replace(minute=0, second=0, microsecond=0)

    run_aggregation(target_hour)


if __name__ == "__main__":
    main()
