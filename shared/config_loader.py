import yaml
from pathlib import Path
from shared.db import get_conn, init_pool
import json

CONFIG_PATH = Path(__file__).resolve().parent.parent / "config.yaml"


def ensure_schema():
    with open(CONFIG_PATH, "r") as f:
        cfg = yaml.safe_load(f)
    init_pool()
    with get_conn() as conn:
        cur = conn.cursor()
        for tenant in cfg.get("tenants", []):
            tenant_id = tenant["id"]
            for cam in tenant.get("cameras", []):
                cur.execute(
                    """
                    INSERT INTO cameras (id, tenant_id, name, location, rtsp_url, fps)
                    VALUES (%s,%s,%s,%s,%s,%s)
                    ON CONFLICT (id) DO UPDATE SET name = EXCLUDED.name
                    """,
                    (cam["id"], tenant_id, cam.get("name"), cam.get("location"), cam.get("rtsp_url"), cam.get("fps", 30))
                )
                for zone in cam.get("zones", []):
                    cur.execute(
                        """
                        INSERT INTO zones (id, tenant_id, camera_id, name, metrics, polygon)
                        VALUES (%s,%s,%s,%s,%s,%s)
                        ON CONFLICT (id) DO UPDATE SET name = EXCLUDED.name, metrics = EXCLUDED.metrics
                        """,
                        (zone["id"], tenant_id, cam["id"], zone["name"], zone["metrics"], json.dumps(zone["polygon"]))
                    )
                    if "thresholds" in zone:
                        for metric, thr in zone["thresholds"].items():
                            cur.execute(
                                """
                                INSERT INTO zone_thresholds (zone_id, metric, threshold)
                                VALUES (%s,%s,%s)
                                ON CONFLICT (zone_id, metric) DO UPDATE SET threshold = EXCLUDED.threshold
                                """,
                                (zone["id"], metric, thr)
                            )
        conn.commit()

if __name__ == "__main__":
    ensure_schema()
