import yaml
import psycopg2
import json

from shared.db import get_conn

CONFIG_PATH = "config.yaml"

def sync_config_to_db():
    """
    Lee el config.yaml (con la estructura anidada correcta) y sincroniza los tenants,
    cámaras, zonas y umbrales con la base de datos.
    """
    print("Iniciando la sincronización de config.yaml con la base de datos (Esquema Correcto Anidado)...")

    try:
        with open(CONFIG_PATH, 'r') as f:
            config = yaml.safe_load(f)
    except FileNotFoundError:
        print(f"Error: El archivo de configuración '{CONFIG_PATH}' no fue encontrado.")
        return
    except yaml.YAMLError as e:
        print(f"Error al leer el archivo YAML: {e}")
        return

    try:
        with get_conn() as conn:
            with conn.cursor() as cur:
                tenants = config.get('tenants', [])
                if not tenants:
                    print("Advertencia: No se encontraron tenants en config.yaml.")
                    return

                for tenant in tenants:
                    tenant_id = tenant['id']
                    # Sincronizar tenant
                    print(f"Sincronizando Tenant ID: {tenant_id} - {tenant.get('name')}")
                    cur.execute(
                        """
                        INSERT INTO tenants (id, name)
                        VALUES (%s, %s)
                        ON CONFLICT (id) DO UPDATE SET name = EXCLUDED.name;
                        """,
                        (tenant_id, tenant.get('name'))
                    )

                    # Sincronizar cámaras
                    for camera in tenant.get('cameras', []):
                        cam_id = camera['id']
                        print(f"Sincronizando Cámara ID: {cam_id}")
                        cur.execute(
                            """
                            INSERT INTO cameras (id, tenant_id, name, rtsp_url, location, fps)
                            VALUES (%s, %s, %s, %s, %s, %s)
                            ON CONFLICT (id) DO UPDATE SET
                                tenant_id = EXCLUDED.tenant_id,
                                name = EXCLUDED.name,
                                rtsp_url = EXCLUDED.rtsp_url,
                                location = EXCLUDED.location,
                                fps = EXCLUDED.fps;
                            """,
                            (cam_id, tenant_id, camera.get('name'), camera.get('rtsp_url'), camera.get('location'), camera.get('fps', 30))
                        )

                        # Sincronizar zonas
                        for zone in camera.get('zones', []):
                            zone_id = zone['id']
                            print(f"Sincronizando Zona ID: {zone_id}")
                            cur.execute(
                                """
                                INSERT INTO zones (id, tenant_id, camera_id, name, polygon, metrics, ghost_timeout_minutes)
                                VALUES (%s, %s, %s, %s, %s::jsonb, %s, %s)
                                ON CONFLICT (id) DO UPDATE SET
                                    tenant_id = EXCLUDED.tenant_id,
                                    camera_id = EXCLUDED.camera_id,
                                    name = EXCLUDED.name,
                                    polygon = EXCLUDED.polygon,
                                    metrics = EXCLUDED.metrics,
                                    ghost_timeout_minutes = EXCLUDED.ghost_timeout_minutes;
                                """,
                                (zone_id, tenant_id, cam_id, zone.get('name'), json.dumps(zone.get('polygon')), zone.get('metrics', []), zone.get('ghost_timeout_minutes', 60))
                            )

                            # Limpiar umbrales viejos para esta zona
                            cur.execute("DELETE FROM zone_thresholds WHERE zone_id = %s;", (zone_id,))

                            # Sincronizar nuevos umbrales
                            if 'thresholds' in zone:
                                for threshold_item in zone['thresholds']:
                                    cur.execute(
                                        """
                                        INSERT INTO zone_thresholds (zone_id, metric, level, threshold)
                                        VALUES (%s, %s, %s, %s);
                                        """,
                                        (zone_id, threshold_item.get('metric'), threshold_item.get('level'), threshold_item.get('threshold'))
                                    )

                conn.commit()
                print("Sincronización completada exitosamente.")

    except (psycopg2.Error, KeyError, TypeError) as e:
        print(f"Ocurrió un error durante la sincronización: {e}")
        if 'conn' in locals() and conn:
            conn.rollback()

if __name__ == "__main__":
    sync_config_to_db()
