import yaml
import psycopg2
from psycopg2.extras import DictCursor
from shared.db import get_conn

CONFIG_FILE = 'config.yaml'
TENANT_SCHEMA = 'raw_vision_socado'

def sync_config(conn, config):
    """
    Sincroniza la configuración del archivo YAML con la base de datos.
    """
    print("Iniciando la sincronización de la configuración...")

    try:
        with conn.cursor(cursor_factory=DictCursor) as cur:
            # Iterar sobre cada tenant en el archivo de configuración
            for tenant_config in config.get('tenants', []):
                tenant_name = tenant_config['name']
                tenant_id_yaml = tenant_config.get('id')
                store_id_yaml = tenant_config.get('store_id')
                
                print(f"Procesando tenant: {tenant_name} (ID YAML: {tenant_id_yaml})")
                
                # 1. Validar que tenemos un store_id en el YAML
                if not store_id_yaml:
                    print(f"  ERROR: No se especificó 'store_id' para el tenant '{tenant_name}'. Saltando este tenant.")
                    continue
                
                # 2. Verificar que la tienda existe y pertenece al tenant correcto
                cur.execute("""
                    SELECT s.id, s.tenant_id, s.name, t.name as tenant_name
                    FROM public.stores s
                    JOIN public.tenants t ON s.tenant_id = t.id
                    WHERE s.id = %s
                """, (store_id_yaml,))
                store_row = cur.fetchone()
                
                if not store_row:
                    print(f"  ERROR: No se encontró una tienda con el ID '{store_id_yaml}' en 'public.stores'. Saltando este tenant.")
                    continue
                
                store_id = store_row['id']
                tenant_id_uuid = store_row['tenant_id']
                
                # 3. Verificar que el tenant del YAML coincide con el tenant de la tienda
                if tenant_name.lower() != store_row['tenant_name'].lower():
                    print(f"  ADVERTENCIA: El tenant '{tenant_name}' del YAML no coincide con el tenant '{store_row['tenant_name']}' de la tienda. Continuando de todas formas...")
                
                print(f"  Tienda encontrada: UUID={store_id}, Name={store_row['name']}")
                print(f"  Tenant asociado: UUID={tenant_id_uuid}, Name={store_row['tenant_name']}")
                
                # 2. Sincronizar Cámaras
                for camera_config in tenant_config.get('cameras', []):
                    print(f"  Sincronizando cámara ID: {camera_config['id']} - {camera_config['name']}")
                    sql_camera = f"""
                    INSERT INTO {TENANT_SCHEMA}.cameras (id, store_id, name, location, rtsp_url, fps)
                    VALUES (%s, %s, %s, %s, %s, %s)
                    ON CONFLICT (id) DO UPDATE SET
                        store_id = EXCLUDED.store_id,
                        name = EXCLUDED.name,
                        location = EXCLUDED.location,
                        rtsp_url = EXCLUDED.rtsp_url,
                        fps = EXCLUDED.fps;
                    """
                    cur.execute(sql_camera, (
                        camera_config['id'],
                        store_id,
                        camera_config['name'],
                        camera_config['location'],
                        camera_config['rtsp_url'],
                        camera_config['fps']
                    ))

                    # 3. Sincronizar Zonas para esta cámara
                    for zone_config in camera_config.get('zones', []):
                        print(f"    Sincronizando zona ID: {zone_config['id']} - {zone_config['name']}")
                        sql_zone = f"""
                        INSERT INTO {TENANT_SCHEMA}.zones (id, camera_id, name, metrics, polygon)
                        VALUES (%s, %s, %s, %s, %s::jsonb)
                        ON CONFLICT (id) DO UPDATE SET
                            camera_id = EXCLUDED.camera_id,
                            name = EXCLUDED.name,
                            metrics = EXCLUDED.metrics,
                            polygon = EXCLUDED.polygon;
                        """
                        cur.execute(sql_zone, (
                            zone_config['id'],
                            camera_config['id'],
                            zone_config['name'],
                            zone_config['metrics'],
                            str(zone_config['polygon']).replace("'", '"') # Simple JSON conversion
                        ))

                        # 4. Sincronizar Umbrales para esta zona
                        for threshold_config in zone_config.get('thresholds', []):
                            metric = threshold_config['metric']
                            print(f"      Sincronizando umbral para métrica: {metric}")
                            sql_threshold = f"""
                            INSERT INTO {TENANT_SCHEMA}.zone_thresholds (zone_id, metric, threshold, level)
                            VALUES (%s, %s, %s, %s)
                            ON CONFLICT (zone_id, metric) DO UPDATE SET
                                threshold = EXCLUDED.threshold,
                                level = EXCLUDED.level;
                            """
                            cur.execute(sql_threshold, (
                                zone_config['id'],
                                metric,
                                threshold_config['threshold'],
                                threshold_config['level']
                            ))
            
            conn.commit()
            print("\nSincronización completada con éxito.")

    except psycopg2.Error as e:
        print(f"Error de base de datos durante la sincronización: {e}")
        conn.rollback()
    except Exception as e:
        print(f"Ocurrió un error inesperado: {e}")

def main():
    try:
        with open(CONFIG_FILE, 'r') as f:
            config = yaml.safe_load(f)
    except FileNotFoundError:
        print(f"Error: El archivo de configuración '{CONFIG_FILE}' no fue encontrado.")
        return
    except yaml.YAMLError as e:
        print(f"Error al parsear el archivo YAML: {e}")
        return

    try:
        with get_conn() as conn:
            sync_config(conn, config)
    except psycopg2.Error as e:
        print(f"No se pudo conectar a la base de datos: {e}")

if __name__ == "__main__":
    main()
