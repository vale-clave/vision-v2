import os
import time
import json
from datetime import datetime, timezone
from typing import Dict

import redis
from psycopg2.extras import execute_values
from psycopg2 import OperationalError, InterfaceError

from shared.db import get_conn, init_pool
from shared.settings import settings

SAMPLE_PERIOD_SECONDS = float(os.getenv("SAMPLE_PERIOD_SECONDS", "5"))
REDIS_PATTERN = "occupancy_cam_*"

def load_camera_mapping() -> Dict[int, dict]:
    """
    Devuelve {camera_id: {tenant_id, store_id}} desde la BD.
    """
    mapping: Dict[int, dict] = {}
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT c.id, c.tenant_id, c.store_id
                FROM raw_vision_rogers.cameras c
            """)
            for cam_id, tenant_id, store_id in cur.fetchall():
                mapping[int(cam_id)] = {"tenant_id": tenant_id, "store_id": store_id}
    return mapping


def main():
    init_pool()
    r = redis.from_url(settings.redis_url.unicode_string())
    camera_map = load_camera_mapping()
    last_map_reload = time.time()

    while True:
        now = datetime.now(timezone.utc)
        ts_iso = now.isoformat()
        try:
            keys = r.keys(REDIS_PATTERN)
            rows = []
            for key in keys:
                raw = r.get(key)
                if not raw:
                    continue
                try:
                    data = json.loads(raw)
                except Exception:
                    continue
                cam_id = int(data.get("camera_id", 0))
                zones = data.get("zones", {})
                m = camera_map.get(cam_id)
                if not m:
                    continue
                for zone_id_str, occ in zones.items():
                    try:
                        zone_id = int(zone_id_str)
                        occupancy = int(occ)
                    except Exception:
                        continue
                    rows.append((
                        ts_iso,
                        m["tenant_id"],
                        m["store_id"],
                        cam_id,
                        zone_id,
                        occupancy
                    ))

            if rows:
                # Inserción batch con upsert por (camera_id, zone_id, ts)
                with get_conn() as conn:
                    with conn.cursor() as cur:
                        execute_values(cur, """
                            INSERT INTO raw_vision_rogers.zone_occupancy_samples
                                (ts, tenant_id, store_id, camera_id, zone_id, occupancy)
                            VALUES %s
                            ON CONFLICT (camera_id, zone_id, ts) DO UPDATE
                                SET occupancy = EXCLUDED.occupancy
                        """, rows)
                    conn.commit()
        except (OperationalError, InterfaceError):
            time.sleep(1.0)
        except Exception:
            # Evitar que el sampler muera por datos malformados
            pass

        # Recargar mapeo de cámaras cada 5 minutos por si hay cambios
        if time.time() - last_map_reload > 300:
            try:
                camera_map = load_camera_mapping()
            except Exception:
                pass
            last_map_reload = time.time()

        time.sleep(SAMPLE_PERIOD_SECONDS)


if __name__ == "__main__":
    main()


