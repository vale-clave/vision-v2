import argparse
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
# Nota: La tabla 'zones' ahora se une desde el esquema raw_vision_rogers
AGGREGATION_QUERY = """
WITH time_range AS (
    SELECT
        %s::TIMESTAMPTZ AS start_ts_utc,
        %s::TIMESTAMPTZ AS end_ts_utc
),
-- Obtener todas las zonas para la tienda actual
store_zones AS (
    SELECT z.id, z.name
    FROM raw_vision_rogers.zones z
    JOIN raw_vision_rogers.cameras c ON z.camera_id = c.id
    WHERE c.store_id = %s
),
starting_occupancy AS (
    SELECT
        zone_id,
        COALESCE(SUM(CASE WHEN event = 'enter' THEN 1 ELSE -1 END), 0) AS occupancy
    FROM raw_vision_rogers.zone_events, time_range
    WHERE ts < start_ts_utc
    GROUP BY zone_id
),
events_in_hour AS (
    SELECT
        ts,
        zone_id,
        track_id,
        event
    FROM raw_vision_rogers.zone_events, time_range
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
        LEAD(oc.ts, 1, (SELECT end_ts_utc FROM time_range)) OVER (PARTITION BY oc.zone_id ORDER BY oc.ts) - oc.ts AS duration
    FROM occupancy_changes oc
    LEFT JOIN starting_occupancy so ON oc.zone_id = so.zone_id
),
occupancy_metrics AS (
    SELECT
        zone_id,
        SUM(current_occupancy * EXTRACT(EPOCH FROM duration)) / 3600.0 AS avg_occupancy,
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
        EXTRACT(EPOCH FROM (LEAST(x.ts, (SELECT end_ts_utc FROM time_range)) - GREATEST(e.ts, (SELECT start_ts_utc FROM time_range)))) as dwell_seconds
    FROM
        raw_vision_rogers.zone_events e
    JOIN
        raw_vision_rogers.zone_events x ON e.track_id = x.track_id AND e.zone_id = x.zone_id
    CROSS JOIN time_range tr
    WHERE
        e.event = 'enter' AND x.event = 'exit'
        AND e.ts < tr.end_ts_utc
        AND x.ts >= tr.start_ts_utc
),
final_metrics AS (
    SELECT
        sz.id as zone_id,
        sz.name as zone_name,
        COALESCE(om.avg_occupancy, so.occupancy, 0) as avg_occupancy,
        COALESCE(om.max_occupancy, so.occupancy, 0) as max_occupancy,
        COALESCE(AVG(dt.dwell_seconds), 0) as avg_dwell_seconds,
        COALESCE(e.total_entries, 0) as total_entries
    FROM store_zones sz
    LEFT JOIN starting_occupancy so ON sz.id = so.zone_id
    LEFT JOIN occupancy_metrics om ON sz.id = om.zone_id
    LEFT JOIN dwell_times dt ON sz.id = dt.zone_id
    LEFT JOIN entries_in_hour e ON sz.id = e.zone_id
    GROUP BY sz.id, sz.name, so.occupancy, om.avg_occupancy, om.max_occupancy, e.total_entries
)
INSERT INTO analytics.fact_vision_metrics_hourly
(hour, tenant_id, store_id, zone_id, zone_name, avg_occupancy, max_occupancy, avg_dwell_seconds, total_entries)
SELECT
    %s, -- El inicio de la hora en UTC
    %s, -- tenant_id
    %s, -- store_id
    fm.zone_id,
    fm.zone_name,
    fm.avg_occupancy,
    fm.max_occupancy,
    fm.avg_dwell_seconds,
    fm.total_entries
FROM final_metrics fm
WHERE fm.zone_id IS NOT NULL
ON CONFLICT (hour, tenant_id, store_id, zone_id) DO UPDATE SET
    avg_occupancy = EXCLUDED.avg_occupancy,
    max_occupancy = EXCLUDED.max_occupancy,
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


def cleanup_raw_data(conn, end_of_hour_utc: datetime):
    """Borra los datos crudos ya procesados en lotes para evitar bloqueos largos."""
    print(f"Limpiando datos crudos anteriores a {end_of_hour_utc.isoformat()}...")
    try:
        with conn.cursor() as cur:
            total_deleted = 0
            batch_size = 100000
            max_iterations = 100  # Prevenir bucles infinitos
            
            for iteration in range(max_iterations):
                cur.execute("""
                    WITH deleted AS (
                        DELETE FROM raw_vision_rogers.zone_events 
                        WHERE ctid IN (
                            SELECT ctid FROM raw_vision_rogers.zone_events 
                            WHERE ts < %s 
                            LIMIT %s
                        )
                        RETURNING *
                    )
                    SELECT COUNT(*) FROM deleted;
                """, (end_of_hour_utc.isoformat(), batch_size))
                
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
