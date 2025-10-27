import asyncio
import argparse
from datetime import datetime, timedelta, timezone

from shared.db import get_pool


# Esta consulta calcula las métricas clave para una hora determinada a partir de los eventos sin procesar.
# 1. Agrupa los eventos de entrada/salida de la hora.
# 2. Calcula los tiempos de permanencia uniendo los eventos de 'enter' y 'exit' para cada 'track_id'.
# 3. Cuenta las entradas totales.
# 4. Calcula la ocupación creando una línea de tiempo de cambios (+1 para 'enter', -1 para 'exit') y
#    luego calcula el máximo y el promedio de esa ocupación a lo largo de la hora.
# 5. Inserta los resultados finales en la tabla hourly_metrics, actualizando si ya existe una entrada
#    para esa hora y zona (haciendo el script re-ejecutable de forma segura).
AGGREGATION_QUERY = """
WITH time_range AS (
    SELECT
        date_trunc('hour', $1::timestamptz) AS start_ts,
        date_trunc('hour', $1::timestamptz) + interval '1 hour' AS end_ts
),
enter_exit_events AS (
    SELECT
        ts,
        zone_id,
        track_id,
        event_type
    FROM events, time_range
    WHERE ts >= time_range.start_ts AND ts < time_range.end_ts
),
dwell_times AS (
    SELECT
        e.zone_id,
        EXTRACT(EPOCH FROM (MIN(x.ts) - e.ts)) as dwell_seconds
    FROM
        enter_exit_events e
    JOIN
        enter_exit_events x ON e.track_id = x.track_id AND e.zone_id = x.zone_id
    WHERE
        e.event_type = 'enter' AND x.event_type = 'exit' AND x.ts > e.ts
    GROUP BY
        e.zone_id, e.track_id, e.ts
),
entries AS (
    SELECT
        zone_id,
        COUNT(*) AS total_entries
    FROM enter_exit_events
    WHERE event_type = 'enter'
    GROUP BY zone_id
),
occupancy_changes AS (
    SELECT ts, zone_id, CASE WHEN event_type = 'enter' THEN 1 ELSE -1 END AS change
    FROM enter_exit_events
),
occupancy_at_time AS (
    SELECT
        ts,
        zone_id,
        SUM(change) OVER (PARTITION BY zone_id ORDER BY ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS occupancy
    FROM occupancy_changes
),
final_metrics AS (
    SELECT
        z.id as zone_id,
        COALESCE(e.total_entries, 0) as total_entries,
        COALESCE(AVG(dt.dwell_seconds), 0) as avg_dwell_seconds,
        COALESCE(MAX(o.occupancy), 0) as max_occupancy,
        COALESCE(AVG(o.occupancy), 0) as avg_occupancy
    FROM zones z
    LEFT JOIN entries e ON z.id = e.zone_id
    LEFT JOIN dwell_times dt ON z.id = dt.zone_id
    LEFT JOIN occupancy_at_time o ON z.id = o.zone_id
    GROUP BY z.id, e.total_entries
)
INSERT INTO hourly_metrics (ts, zone_id, total_entries, avg_dwell_seconds, max_occupancy, avg_occupancy)
SELECT
    (SELECT start_ts FROM time_range),
    fm.zone_id,
    fm.total_entries,
    fm.avg_dwell_seconds,
    fm.max_occupancy,
    fm.avg_occupancy
FROM final_metrics fm
WHERE fm.zone_id IS NOT NULL
ON CONFLICT (ts, zone_id) DO UPDATE SET
    total_entries = EXCLUDED.total_entries,
    avg_dwell_seconds = EXCLUDED.avg_dwell_seconds,
    max_occupancy = EXCLUDED.max_occupancy,
    avg_occupancy = EXCLUDED.avg_occupancy;
"""


async def run_aggregation(target_hour: datetime):
    """
    Ejecuta la agregación por hora para la hora especificada.
    """
    pool = await get_pool()
    target_hour_str = target_hour.isoformat()
    print(f"Ejecutando agregación para la hora que comienza en: {target_hour_str}")
    async with pool.acquire() as conn:
        await conn.execute(AGGREGATION_QUERY, target_hour_str)
    print("Agregación completada.")


def main():
    parser = argparse.ArgumentParser(description="Ejecutar agregación por hora para métricas de visión.")
    parser.add_argument(
        "--hour",
        type=str,
        help="La hora a procesar en formato ISO 8601 (p. ej., '2025-10-28T14:00:00Z'). Por defecto, la hora anterior."
    )

    args = parser.parse_args()

    if args.hour:
        # Asegurarse de que la hora esté en UTC
        target_hour = datetime.fromisoformat(args.hour.replace('Z', '+00:00'))
    else:
        # Por defecto, el inicio de la hora anterior en UTC
        now = datetime.now(timezone.utc)
        target_hour = (now - timedelta(hours=1)).replace(minute=0, second=0, microsecond=0)

    asyncio.run(run_aggregation(target_hour))


if __name__ == "__main__":
    main()
