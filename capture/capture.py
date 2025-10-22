import os
import cv2
import redis
import time
import json
import base64
import yaml
from pathlib import Path
from shared.settings import settings

CONFIG_PATH = Path(__file__).resolve().parent.parent / "config.yaml"
FRAMES_QUEUE_KEY = os.getenv("REDIS_FRAMES_QUEUE", "frames_queue")
CAMERA_ID = int(os.getenv("CAMERA_ID", 1))

# --- Cargar configuración específica de la cámara ---
with open(CONFIG_PATH, "r") as f:
    cfg = yaml.safe_load(f)

cam_cfg = None
for tenant in cfg.get("tenants", []):
    for c in tenant.get("cameras", []):
        if c["id"] == CAMERA_ID:
            cam_cfg = c
            break
    if cam_cfg:
        break

if cam_cfg is None:
    raise RuntimeError(f"Capture service: Camera id {CAMERA_ID} not found in config")

RTSP_URL = cam_cfg.get("rtsp_url")
FPS = cam_cfg.get("fps", 10) # Usamos un FPS configurable o default a 10
FRAME_INTERVAL = 1.0 / FPS

# --- Conexión a Redis ---
redis_client = redis.from_url(settings.redis_url.unicode_string())

# --- Bucle principal de captura ---
print(f"Capture service started for Camera ID: {CAMERA_ID} at {FPS} FPS")

cap = cv2.VideoCapture(RTSP_URL)

while True:
    if not cap.isOpened():
        print(f"Capture service: Stream for camera {CAMERA_ID} disconnected. Reconnecting...")
        cap.release()
        cap = cv2.VideoCapture(RTSP_URL)
        time.sleep(5.0)
        continue

    ok, frame = cap.read()
    if not ok:
        print(f"Capture service: Cannot read frame from camera {CAMERA_ID}. Reconnecting...")
        cap.release()
        time.sleep(5.0)
        continue

    # Codificar el frame a JPEG y luego a base64
    _, buffer = cv2.imencode('.jpg', frame)
    frame_b64 = base64.b64encode(buffer).decode('utf-8')

    # Crear el payload
    payload = {
        "camera_id": CAMERA_ID,
        "ts": time.time(),
        "frame_b64": frame_b64
    }

    # Empujar a la cola de Redis
    redis_client.rpush(FRAMES_QUEUE_KEY, json.dumps(payload))
    
    # Esperar el intervalo de tiempo correcto para mantener el FPS deseado
    time.sleep(FRAME_INTERVAL)
