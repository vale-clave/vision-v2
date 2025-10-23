import os, time, json, redis, base64, numpy as np
from datetime import datetime
from shapely.geometry import Point, Polygon
import yaml
from pathlib import Path
from shared.settings import settings
from PIL import Image
import io
import torch

# FIX: PyTorch >= 2.6 rompe la carga de modelos de ultralytics.
# "Parcheamos" torch.load para forzar weights_only=False, ya que confiamos
# en la fuente del modelo. Esto soluciona el problema de raíz.
original_torch_load = torch.load
torch.load = lambda *args, **kwargs: original_torch_load(*args, weights_only=False, **kwargs)

from ultralytics import YOLO

CONFIG_PATH = Path(__file__).resolve().parent.parent / "config.yaml"
FRAMES_QUEUE_KEY = os.getenv("REDIS_FRAMES_QUEUE", "frames_queue")
DETECTIONS_QUEUE_KEY = os.getenv("REDIS_DETECTIONS_QUEUE", "detections_queue")
CAMERA_ID = int(os.getenv("CAMERA_ID", 1))

# --- Conexión a Redis ---
redis_client = redis.from_url(settings.redis_url.unicode_string(), decode_responses=True)

# --- Cargar configuración ---
with open(CONFIG_PATH, "r") as f:
    cfg = yaml.safe_load(f)

cam_cfg = None
for tenant in cfg.get("tenants", []):
    for c in tenant.get("cameras", []):
        if c["id"] == CAMERA_ID:
            cam_cfg = (tenant["id"], c)
            break
    if cam_cfg:
        break
if cam_cfg is None:
    raise RuntimeError(f"Worker: Camera id {CAMERA_ID} not found in config")

TENANT_ID, CAM = cam_cfg

ZONES = {}
for z in CAM.get("zones", []):
    ZONES[z["id"]] = {
        "poly": Polygon(z["polygon"]),
        "name": z["name"],
        "metrics": z.get("metrics", [])
    }

# --- Cargar el modelo YOLO ---
# Esta es la parte que antes causaba el conflicto. Ahora corre en un proceso separado.
MODEL_WEIGHTS = os.getenv("YOLO_WEIGHTS", "weights/yolov8s-world.pt")
model = YOLO(MODEL_WEIGHTS)
print("Worker: Modelo YOLO cargado con éxito.")

print(f"Worker started for Camera ID: {CAMERA_ID}")

# Diccionario para guardar el estado de los tracks
prev_tracks = {}

while True:
    # 1. Esperar bloqueantemente por un nuevo frame desde la cola de Redis
    # Usamos blpop para esperar eficientemente sin un bucle de polling constante
    item = redis_client.blpop(FRAMES_QUEUE_KEY, timeout=30)
    if item is None:
        continue
        
    _, data = item
    payload = json.loads(data)

    # Solo procesamos frames de nuestra propia cámara asignada
    if payload["camera_id"] != CAMERA_ID:
        continue

    # 2. Decodificar el frame de base64 a una imagen, SIN USAR OPENCV
    img_bytes = base64.b64decode(payload["frame_b64"])
    img = Image.open(io.BytesIO(img_bytes))
    frame = np.array(img) # YOLO espera un array de numpy

    # 3. Inferencia y Tracking (lógica original)
    results = model.track(frame, classes=[0], verbose=False, persist=True, tracker="bytetrack.yaml")[0]

    current_tracks = {}
    if results.boxes.id is not None:
        for box in results.boxes:
            track_id = int(box.id[0])
            x1, y1, x2, y2 = box.xyxy[0].cpu().numpy()
            cx = (x1 + x2) / 2
            cy = (y1 + y2) / 2
            current_tracks[track_id] = Point(cx, cy)

    # --- Lógica de Eventos de Entrada/Salida de Zona (sin cambios) ---
    for track_id, point in current_tracks.items():
        for zone_id, zinfo in ZONES.items():
            key = (track_id, zone_id)
            if zinfo["poly"].contains(point):
                if key not in prev_tracks:
                    print(f"EVENT: Track {track_id} ENTERED zone {zone_id} ('{zinfo['name']}')")
                    evt = {
                        "tenant_id": TENANT_ID,
                        "camera_id": CAMERA_ID,
                        "zone_id": zone_id,
                        "track_id": track_id,
                        "event": "enter",
                        "ts": datetime.utcnow().isoformat() + "Z",
                    }
                    redis_client.rpush(DETECTIONS_QUEUE_KEY, json.dumps(evt))
                    prev_tracks[key] = time.time()
    
    exited_keys = []
    for key in prev_tracks:
        track_id, zone_id = key
        
        is_outside = track_id not in current_tracks or not ZONES[zone_id]["poly"].contains(current_tracks[track_id])

        if is_outside:
            start_time = prev_tracks[key]
            print(f"EVENT: Track {track_id} EXITED zone {zone_id} ('{ZONES[zone_id]['name']}')")
            evt = {
                "tenant_id": TENANT_ID,
                "camera_id": CAMERA_ID,
                "zone_id": zone_id,
                "track_id": track_id,
                "event": "exit",
                "ts": datetime.utcnow().isoformat() + "Z",
            }
            if 'dwell' in ZONES[zone_id].get('metrics', []):
                evt['dwell'] = time.time() - start_time
            
            redis_client.rpush(DETECTIONS_QUEUE_KEY, json.dumps(evt))
            exited_keys.append(key)

    for key in exited_keys:
        del prev_tracks[key]
