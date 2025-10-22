import json
import os
import time
from typing import List, Tuple

import redis
from psycopg2.extras import execute_values

from shared.db import get_conn, init_pool
from shared.settings import settings

BATCH_SIZE = int(os.getenv("BATCH_SIZE", 200))
SLEEP_SEC = float(os.getenv("LOOP_SLEEP", 0.2))
QUEUE_KEY = os.getenv("REDIS_QUEUE", "detections_queue")

redis_client = redis.from_url(settings.redis_url.unicode_string(), decode_responses=True)
init_pool()


def _flush_batch(batch: List[Tuple]):
    if not batch:
        return
    with get_conn() as conn:
        with conn.cursor() as cur:
            execute_values(cur,
                """
                INSERT INTO zone_events (tenant_id, camera_id, zone_id, track_id, event, ts, dwell_seconds)
                VALUES %s
                """,
                batch
            )
        conn.commit()


def main():
    batch: List[Tuple] = []
    while True:
        item = redis_client.lpop(QUEUE_KEY)
        if item:
            try:
                d = json.loads(item)
                dwell = d.get("dwell")
                batch.append((
                    d.get("tenant_id", 1),
                    d["camera_id"],
                    d["zone_id"],
                    d["track_id"],
                    d["event"],
                    d["ts"],
                    dwell
                ))
            except Exception:
                continue
            if len(batch) >= BATCH_SIZE:
                _flush_batch(batch)
                batch = []
        else:
            if batch:
                _flush_batch(batch)
                batch = []
            time.sleep(SLEEP_SEC)


if __name__ == "__main__":
    main()
