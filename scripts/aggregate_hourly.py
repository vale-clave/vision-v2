import argparse
import os
from datetime import datetime, timedelta
import pytz  # Usaremos pytz para un manejo robusto de zonas horarias
import psycopg2
from psycopg2.extras import DictCursor
from shared.db import get_conn

# SQL para obtener las tiendas activas y su información de horario
GET_ACTIVE_STORES_QUERY = """
SELECT 
    s.id as store_id,
    s.tenant_id,
    s.timezone,
    s.operating_hours_start,
    s.operating_hours_end
FROM public.stores s
WHERE 
    s.operating_hours_start IS NOT NULL 
    AND s.operating_hours_end IS NOT NULL
    AND s.timezone IS NOT NULL;
"""

# Consulta de agregación rediseñada para el nuevo esquema
# Nota: La tabla 'zones' ahora se une desde el esquema raw_vision_socado
AGGREGATION_QUERY = """
WITH time_range AS (
    SELECT
        %s::TIMESTAMPTZ AS start_ts_utc,
        %s::TIMESTAMPTZ AS end_ts_utc
),
-- Obtener todas las zonas para la tienda actual
store_zones AS (
    SELECT z.id, z.name
    FROM raw_vision_socado.zones z
    JOIN raw_vision_socado.cameras c ON z.camera_id = c.id
    WHERE c.store_id = %s
),
-- Muestras de ocupación dentro de la hora (publicadas por el sampler)
samples_in_hour AS (
    SELECT s.zone_id, s.occupancy, s.ts
    FROM raw_vision_socado.zone_occupancy_samples s, time_range tr
    WHERE s.ts >= tr.start_ts_utc AND s.ts < tr.end_ts_utc
),
 latest_snapshot AS (
     -- último snapshot de ocupación antes del inicio de la hora
     SELECT DISTINCT ON (s.zone_id)
         s.zone_id,
         s.snapshot_ts,
         s.occupancy
     FROM raw_vision_socado.zone_occupancy_snapshots s, time_range
     WHERE s.snapshot_ts <= start_ts_utc
     ORDER BY s.zone_id, s.snapshot_ts DESC
 ),
 changes_after_snapshot AS (
     -- cambios desde el snapshot hasta el inicio de la hora
     SELECT
         ze.zone_id,
         SUM(CASE WHEN ze.event = 'enter' THEN 1 ELSE -1 END) AS change
     FROM raw_vision_socado.zone_events ze
     LEFT JOIN latest_snapshot ls ON ls.zone_id = ze.zone_id, time_range
     WHERE ze.ts >= COALESCE(ls.snapshot_ts, TIMESTAMP 'epoch')
       AND ze.ts <  start_ts_utc
     GROUP BY ze.zone_id
 ),
 starting_occupancy AS (
     SELECT
         COALESCE(ls.zone_id, cas.zone_id) AS zone_id,
         GREATEST(0, COALESCE(ls.occupancy, 0) + COALESCE(cas.change, 0)) AS occupancy
     FROM latest_snapshot ls
     FULL OUTER JOIN changes_after_snapshot cas ON cas.zone_id = ls.zone_id
 ),
 events_in_hour AS (
     SELECT
         ts,
         zone_id,
         track_id,
         event
     FROM raw_vision_socado.zone_events, time_range
     WHERE ts >= start_ts_utc AND ts < end_ts_utc
 ),
 events_with_seed AS (
     -- Semilla al inicio de la hora para que exista línea base aunque no haya eventos
     SELECT sz.id AS zone_id, tr.start_ts_utc AS ts, 0 AS delta
     FROM store_zones sz, time_range tr
     UNION ALL
     SELECT e.zone_id,
            e.ts,
            CASE WHEN e.event = 'enter' THEN 1 ELSE -1 END AS delta
     FROM events_in_hour e
 ),
 occupancy_changes AS (
     SELECT
         zone_id,
         ts,
         SUM(delta) OVER (PARTITION BY zone_id ORDER BY ts
                          ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS net_change
     FROM events_with_seed
 ),
occupancy_timeline AS (
    SELECT
        oc.zone_id,
        oc.ts,
        GREATEST(0, COALESCE(so.occupancy, 0) + oc.net_change) AS current_occupancy,
        LEAD(oc.ts, 1, (SELECT end_ts_utc FROM time_range)) OVER (PARTITION BY oc.zone_id ORDER BY oc.ts) - oc.ts AS duration
    FROM occupancy_changes oc
    LEFT JOIN starting_occupancy so ON oc.zone_id = so.zone_id
),
t_timeline AS (
    -- timeline con fin explícito
    SELECT
        zone_id,
        ts AS start_ts,
        (ts + duration) AS end_ts,
        current_occupancy
    FROM occupancy_timeline
),
minute_series AS (
    -- series de minutos dentro de la hora objetivo
    SELECT gs AS minute_start,
           gs + INTERVAL '1 minute' AS minute_end
    FROM generate_series(
        (SELECT start_ts_utc FROM time_range),
        (SELECT end_ts_utc FROM time_range) - INTERVAL '1 minute',
        INTERVAL '1 minute'
    ) gs
),
per_minute_max AS (
    -- para cada minuto y zona, tomar el máximo observado en las muestras
    SELECT
        DATE_TRUNC('minute', s.ts) AS minute_start,
        s.zone_id,
        MAX(s.occupancy) AS max_occupancy_minute
    FROM samples_in_hour s
    GROUP BY minute_start, s.zone_id
),
avg_minute_peak AS (
    -- promedio de los picos por minuto durante la hora
    SELECT zone_id, AVG(max_occupancy_minute) AS avg_minute_peak
    FROM per_minute_max
    GROUP BY zone_id
),
occupancy_metrics AS (
    SELECT
        zone_id,
        AVG(occupancy)::float AS avg_occupancy,
        MAX(occupancy) AS max_occupancy
    FROM samples_in_hour
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
    -- Emparejar cada enter con su exit correspondiente más cercano
    WITH enters AS (
        SELECT 
            zone_id,
            track_id,
            ts as enter_ts,
            ROW_NUMBER() OVER (PARTITION BY zone_id, track_id ORDER BY ts) as enter_seq
        FROM raw_vision_socado.zone_events, time_range tr
        WHERE event = 'enter'
        AND ts >= tr.start_ts_utc AND ts < tr.end_ts_utc
    ),
    exits AS (
        SELECT 
            zone_id,
            track_id,
            ts as exit_ts,
            ROW_NUMBER() OVER (PARTITION BY zone_id, track_id ORDER BY ts) as exit_seq
        FROM raw_vision_socado.zone_events, time_range tr
        WHERE event = 'exit'
        AND ts >= tr.start_ts_utc AND ts < tr.end_ts_utc
    ),
    matched_pairs AS (
        SELECT
            e.zone_id,
            e.track_id,
            e.enter_ts,
            x.exit_ts,
            EXTRACT(EPOCH FROM (x.exit_ts - e.enter_ts)) as dwell_seconds
        FROM enters e
        INNER JOIN exits x ON e.zone_id = x.zone_id 
            AND e.track_id = x.track_id 
            AND e.enter_seq = x.exit_seq
            AND x.exit_ts > e.enter_ts  -- El exit debe ser después del enter
    )
    SELECT 
        zone_id,
        dwell_seconds
    FROM matched_pairs
    WHERE dwell_seconds > 0  -- Filtrar tiempos negativos o cero
),
final_metrics AS (
    SELECT
        sz.id as zone_id,
        sz.name as zone_name,
        -- FIX: Solo usar samples reales, NO starting_occupancy (que puede acumularse incorrectamente)
        -- Si no hay samples, retornar NULL para no insertar datos incorrectos
        om.avg_occupancy,
        -- Limite máximo de 100 como safety check adicional
        LEAST(om.max_occupancy, 100) as max_occupancy,
        COALESCE(MAX(amp.avg_minute_peak), 0) as avg_minute_peak,
        COALESCE(AVG(dt.dwell_seconds), 0) as avg_dwell_seconds,
        COALESCE(e.total_entries, 0) as total_entries,
        -- Flag para saber si hay datos válidos de samples
        (om.avg_occupancy IS NOT NULL) as has_samples
    FROM store_zones sz
    LEFT JOIN starting_occupancy so ON sz.id = so.zone_id
    LEFT JOIN occupancy_metrics om ON sz.id = om.zone_id
    LEFT JOIN avg_minute_peak amp ON sz.id = amp.zone_id
    LEFT JOIN dwell_times dt ON sz.id = dt.zone_id
    LEFT JOIN entries_in_hour e ON sz.id = e.zone_id
    GROUP BY sz.id, sz.name, so.occupancy, om.avg_occupancy, om.max_occupancy, e.total_entries
)
INSERT INTO analytics.fact_vision_metrics_hourly
(hour, tenant_id, store_id, zone_id, zone_name, avg_occupancy, max_occupancy, avg_minute_peak, avg_dwell_seconds, total_entries)
SELECT
    %s, -- El inicio de la hora en UTC
    %s, -- tenant_id
    %s, -- store_id
    fm.zone_id,
    fm.zone_name,
    -- Si no hay samples, usar 0 en lugar de NULL (más seguro que starting_occupancy)
    COALESCE(fm.avg_occupancy, 0) as avg_occupancy,
    COALESCE(fm.max_occupancy, 0) as max_occupancy,
    fm.avg_minute_peak,
    fm.avg_dwell_seconds,
    fm.total_entries
FROM final_metrics fm
WHERE fm.zone_id IS NOT NULL
ON CONFLICT (hour, tenant_id, store_id, zone_id) DO UPDATE SET
    avg_occupancy = EXCLUDED.avg_occupancy,
    max_occupancy = EXCLUDED.max_occupancy,
    avg_minute_peak = EXCLUDED.avg_minute_peak,
    avg_dwell_seconds = EXCLUDED.avg_dwell_seconds,
    total_entries = EXCLUDED.total_entries;
"""

# CLEANUP_QUERY ya no se usa directamente, la lógica está en cleanup_raw_data()


def is_store_open(now_utc: datetime, store: DictCursor) -> bool:
    """Verifica si una tienda está abierta en la hora actual."""
    try:
        store_tz = pytz.timezone(store['timezone'])
        now_local = now_utc.astimezone(store_tz)
        
        # Comparamos solo la parte de la hora
        current_time = now_local.time()
        start_time = store['operating_hours_start']
        end_time = store['operating_hours_end']
        
        return start_time <= current_time < end_time
    except pytz.UnknownTimeZoneError:
        print(f"Error: Zona horaria desconocida '{store['timezone']}' para la tienda {store['store_id']}")
        return False
    except Exception as e:
        print(f"Error al verificar el horario de la tienda {store['store_id']}: {e}")
        return False


def run_aggregation_for_store(conn, store: DictCursor, target_hour_utc: datetime):
    """Ejecuta la agregación para una tienda y hora específicas."""
    
    start_of_hour_utc = target_hour_utc.replace(minute=0, second=0, microsecond=0)
    end_of_hour_utc = start_of_hour_utc + timedelta(hours=1)
    
    store_id = store['store_id']
    tenant_id = store['tenant_id']
    
    print(f"Procesando tienda {store_id} para la hora {start_of_hour_utc.isoformat()}...")
    
    try:
        with conn.cursor() as cur:
            cur.execute(AGGREGATION_QUERY, (
                start_of_hour_utc.isoformat(),
                end_of_hour_utc.isoformat(),
                str(store_id),
                start_of_hour_utc.isoformat(),
                str(tenant_id),
                str(store_id)
            ))
        conn.commit()
        print(f"Agregación completada para la tienda {store_id}.")
    except psycopg2.Error as e:
        print(f"Error de base de datos durante la agregación para la tienda {store_id}: {e}")
        conn.rollback()


RAW_RETENTION_HOURS = int(os.getenv("RAW_RETENTION_HOURS", "5"))

def cleanup_raw_data(conn, end_of_hour_utc: datetime):
    """Borra datos crudos manteniendo historial suficiente para starting_occupancy."""
    cutoff = end_of_hour_utc - timedelta(hours=RAW_RETENTION_HOURS)
    print(f"Limpiando datos crudos anteriores a {cutoff.isoformat()} (retención {RAW_RETENTION_HOURS}h)...")
    try:
        # 1) Antes de borrar, crear snapshots en cutoff para todas las zonas
        with conn.cursor() as cur:
            cur.execute("""
                WITH latest_snapshot AS (
                    SELECT DISTINCT ON (s.zone_id)
                        s.zone_id,
                        s.snapshot_ts,
                        s.occupancy
                    FROM raw_vision_socado.zone_occupancy_snapshots s
                    WHERE s.snapshot_ts <= %s
                    ORDER BY s.zone_id, s.snapshot_ts DESC
                ),
                changes_to_cutoff AS (
                    SELECT
                        ze.zone_id,
                        SUM(CASE WHEN ze.event = 'enter' THEN 1 ELSE -1 END) AS change
                    FROM raw_vision_socado.zone_events ze
                    LEFT JOIN latest_snapshot ls ON ls.zone_id = ze.zone_id
                    WHERE ze.ts >= COALESCE(ls.snapshot_ts, TIMESTAMP 'epoch')
                      AND ze.ts <  %s
                    GROUP BY ze.zone_id
                ),
                zones AS (
                    SELECT DISTINCT zone_id FROM raw_vision_socado.zone_events
                ),
                snapshot_data AS (
                    SELECT
                        z.zone_id,
                        %s::timestamptz AS snapshot_ts,
                        GREATEST(0, COALESCE(ls.occupancy, 0) + COALESCE(c.change, 0)) AS occupancy
                    FROM zones z
                    LEFT JOIN latest_snapshot ls ON ls.zone_id = z.zone_id
                    LEFT JOIN changes_to_cutoff c ON c.zone_id = z.zone_id
                )
                INSERT INTO raw_vision_socado.zone_occupancy_snapshots (zone_id, snapshot_ts, occupancy)
                SELECT zone_id, snapshot_ts, occupancy
                FROM snapshot_data
                ON CONFLICT (zone_id, snapshot_ts) DO UPDATE SET occupancy = EXCLUDED.occupancy;
            """, (cutoff.isoformat(), cutoff.isoformat(), cutoff.isoformat()))
            conn.commit()

        # 2) Luego, borrar en lotes lo anterior a cutoff
        with conn.cursor() as cur:
            total_deleted = 0
            batch_size = 100000
            max_iterations = 100  # Prevenir bucles infinitos
            
            for iteration in range(max_iterations):
                cur.execute("""
                    WITH deleted AS (
                        DELETE FROM raw_vision_socado.zone_events 
                        WHERE ctid IN (
                            SELECT ctid FROM raw_vision_socado.zone_events 
                            WHERE ts < %s 
                            LIMIT %s
                        )
                        RETURNING *
                    )
                    SELECT COUNT(*) FROM deleted;
                """, (cutoff.isoformat(), batch_size))
                
                deleted_count = cur.fetchone()[0]
                total_deleted += deleted_count
                conn.commit()
                
                if deleted_count == 0:
                    break
                
                print(f"  Eliminados {total_deleted:,} eventos (lote de {deleted_count:,})...")
            
            print(f"Limpieza completada. Total eliminado: {total_deleted:,} eventos.")
    except psycopg2.Error as e:
        print(f"Error de base de datos durante la limpieza: {e}")
        conn.rollback()


def main():
    parser = argparse.ArgumentParser(description="Ejecutar agregación por hora para métricas de visión.")
    parser.add_argument(
        "--hour",
        type=str,
        help="La hora a procesar en formato ISO 8601 UTC (p. ej., '2025-10-28T14:00:00Z'). Por defecto, la hora anterior."
    )
    args = parser.parse_args()

    now_utc = datetime.now(pytz.utc)

    if args.hour:
        target_hour_utc = datetime.fromisoformat(args.hour.replace('Z', '+00:00'))
    else:
        # Por defecto, el inicio de la hora anterior
        target_hour_utc = (now_utc - timedelta(hours=1)).replace(minute=0, second=0, microsecond=0)

    try:
        with get_conn() as conn:
            with conn.cursor(cursor_factory=DictCursor) as cur:
                cur.execute(GET_ACTIVE_STORES_QUERY)
                stores = cur.fetchall()
            
            active_stores = [s for s in stores if is_store_open(target_hour_utc, s)]
            
            if not active_stores:
                print("No hay tiendas abiertas en este momento. Saliendo.")
                return

            print(f"Se encontraron {len(active_stores)} tiendas abiertas para procesar.")
            
            for store in active_stores:
                run_aggregation_for_store(conn, store, target_hour_utc)
            
            # La limpieza se ejecuta una vez al final, para la hora procesada
            end_of_processed_hour = target_hour_utc + timedelta(hours=1)
            cleanup_raw_data(conn, end_of_processed_hour)

    except psycopg2.Error as e:
        print(f"No se pudo conectar a la base de datos: {e}")
    except Exception as e:
        print(f"Ocurrió un error inesperado: {e}")


if __name__ == "__main__":
    main()
