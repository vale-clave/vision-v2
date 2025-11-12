import os
import time
import json
import redis
import base64
import numpy as np
from datetime import datetime
import yaml
from pathlib import Path
from shared.settings import settings
from PIL import Image
import io
import torch
import cv2

# FIX: PyTorch >= 2.6 rompe la carga de modelos de ultralytics.
# "Parcheamos" torch.load para forzar weights_only=False, ya que confiamos
# en la fuente del modelo. Esto soluciona el problema de raíz.
original_torch_load = torch.load
torch.load = lambda *args, **kwargs: original_torch_load(*args, weights_only=False, **kwargs)

from ultralytics import YOLO
import supervision as sv
from utils.timers import ClockBasedTimer

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

# --- Configurar zonas usando supervision ---
ZONES_CONFIG = {}
zones = []
zone_annotators = []
timers = []
box_annotators = []

for z in CAM.get("zones", []):
    zone_id = z["id"]
    polygon = np.array(z["polygon"], dtype=np.int32)
    
    # Crear PolygonZone de supervision
    zone = sv.PolygonZone(
        polygon=polygon,
        triggering_anchors=(sv.Position.CENTER,),
    )
    
    # Crear annotators para visualización
    zone_annotator = sv.PolygonZoneAnnotator(
        zone=zone,
        color=sv.ColorPalette.DEFAULT.by_idx(len(zones)),
        thickness=2,
        text_thickness=2,
        text_scale=0.5,
    )
    
    box_annotator = sv.BoxAnnotator(
        color=sv.ColorPalette.DEFAULT.by_idx(len(zones)),
        thickness=2,
    )
    
    zones.append(zone)
    zone_annotators.append(zone_annotator)
    box_annotators.append(box_annotator)
    timers.append(ClockBasedTimer())
    
    ZONES_CONFIG[zone_id] = {
        "zone": zone,
        "zone_annotator": zone_annotator,
        "box_annotator": box_annotator,
        "timer": timers[-1],
        "name": z["name"],
        "metrics": z.get("metrics", []),
        "ghost_timeout_seconds": z.get("ghost_timeout_minutes", 60) * 60,
        "index": len(zones) - 1,  # Índice en las listas
    }

# --- Cargar el modelo YOLO ---
MODEL_WEIGHTS = os.getenv("YOLO_WEIGHTS", "weights/yolov8s-world.pt")
model = YOLO(MODEL_WEIGHTS)
print("Worker: Modelo YOLO cargado con éxito.")

# --- Configurar tracker de supervision ---
tracker = sv.ByteTrack(minimum_matching_threshold=0.8)

print(f"Worker started for Camera ID: {CAMERA_ID}")

# Configuración de filtros
TRACK_CONFIDENCE_THRESHOLD = 0.5
TRACK_IOU_THRESHOLD = 0.7
EVENT_COOLDOWN_SECONDS = 5.0
MIN_TRACK_AGE_FOR_ENTER = 1.0
MIN_DWELL_TIME_SECONDS = 3.0

# Estado de tracking por zona: {zone_id: {track_id: enter_timestamp}}
zone_track_states = {zone_id: {} for zone_id in ZONES_CONFIG.keys()}

# Diccionario para guardar el último tiempo que se generó un evento para cada (track_id, zone_id)
last_event_time = {}

# Diccionario para rastrear cuándo se detectó por primera vez cada track
track_first_seen = {}

# Label annotator para mostrar track IDs y tiempos
label_annotator = sv.LabelAnnotator(
    text_color=sv.Color.BLACK,
    text_scale=0.5,
    text_thickness=1,
)

while True:
    # 1. Esperar bloqueantemente por un nuevo frame desde la cola de Redis
    item = redis_client.blpop(FRAMES_QUEUE_KEY, timeout=30)
    if item is None:
        continue
        
    _, data = item
    payload = json.loads(data)

    # Solo procesamos frames de nuestra propia cámara asignada
    if payload["camera_id"] != CAMERA_ID:
        continue

    # 2. Decodificar el frame de base64 a una imagen
    img_bytes = base64.b64decode(payload["frame_b64"])
    img = Image.open(io.BytesIO(img_bytes))
    frame = np.array(img)

    # 3. Inferencia con YOLO (sin tracking integrado)
    results = model(
        frame,
        classes=[0],  # Solo personas
        verbose=False,
        conf=TRACK_CONFIDENCE_THRESHOLD,
        iou=TRACK_IOU_THRESHOLD,
        imgsz=640,
        device='cuda' if torch.cuda.is_available() else 'cpu'
    )[0]

    # 4. Convertir a detecciones de supervision
    detections = sv.Detections.from_ultralytics(results)
    
    # Filtrar por confianza mínima
    detections = detections[detections.confidence > TRACK_CONFIDENCE_THRESHOLD]
    
    # 5. Aplicar tracking con supervision ByteTrack
    detections = tracker.update_with_detections(detections)
    
    current_time = time.time()
    
    # Registrar cuándo se vio por primera vez cada track
    if detections.tracker_id is not None:
        for tracker_id in detections.tracker_id:
            if tracker_id is not None:
                tracker_id_int = int(tracker_id)
                if tracker_id_int not in track_first_seen:
                    track_first_seen[tracker_id_int] = current_time

    # 6. Procesar cada zona
    for zone_id, zone_info in ZONES_CONFIG.items():
        zone = zone_info["zone"]
        timer = zone_info["timer"]
        zone_index = zone_info["index"]
        
        # Detectar qué objetos están en la zona
        is_in_zone = zone.trigger(detections)
        detections_in_zone = detections[is_in_zone]
        
        # Calcular tiempo en zona usando ClockBasedTimer
        if len(detections_in_zone) > 0:
            time_in_zone = timer.tick(detections_in_zone)
        else:
            time_in_zone = np.array([])
        
        # Obtener estado anterior de esta zona
        prev_tracks_in_zone = zone_track_states[zone_id]
        
        # Track IDs actuales en la zona
        current_track_ids_in_zone = set()
        if detections_in_zone.tracker_id is not None:
            current_track_ids_in_zone = set(detections_in_zone.tracker_id.tolist())
        
        # Detectar entradas (tracks nuevos en la zona)
        for i, tracker_id in enumerate(detections_in_zone.tracker_id):
            if tracker_id is None:
                continue
                
            tracker_id = int(tracker_id)
            current_track_ids_in_zone.add(tracker_id)
            
            # Verificar si es un track nuevo en la zona
            if tracker_id not in prev_tracks_in_zone:
                # Verificar edad mínima del track y cooldown
                track_age = current_time - track_first_seen.get(tracker_id, current_time)
                event_key = (tracker_id, zone_id, "enter")
                last_time = last_event_time.get(event_key, 0)
                time_since_last_event = current_time - last_time
                
                if time_since_last_event >= EVENT_COOLDOWN_SECONDS and track_age >= MIN_TRACK_AGE_FOR_ENTER:
                    print(f"EVENT: Track {tracker_id} ENTERED zone {zone_id} ('{zone_info['name']}') [age: {track_age:.1f}s]")
                    evt = {
                        "tenant_id": TENANT_ID,
                        "camera_id": CAMERA_ID,
                        "zone_id": zone_id,
                        "track_id": tracker_id,
                        "event": "enter",
                        "ts": datetime.utcnow().isoformat() + "Z",
                    }
                    redis_client.rpush(DETECTIONS_QUEUE_KEY, json.dumps(evt))
                    prev_tracks_in_zone[tracker_id] = current_time
                    last_event_time[event_key] = current_time
        
        # Detectar salidas (tracks que estaban en la zona pero ya no están)
        tracks_to_remove = []
        for tracker_id, enter_time in prev_tracks_in_zone.items():
            if tracker_id not in current_track_ids_in_zone:
                # El track salió de la zona o desapareció
                event_key = (tracker_id, zone_id, "exit")
                last_time = last_event_time.get(event_key, 0)
                
                # Verificar ghost timeout primero
                time_in_zone_total = current_time - enter_time
                ghost_timeout = zone_info["ghost_timeout_seconds"]
                
                # Verificar si el track desapareció completamente
                track_still_exists = False
                if detections.tracker_id is not None:
                    track_still_exists = tracker_id in [int(tid) for tid in detections.tracker_id if tid is not None]
                
                if not track_still_exists:
                    # Track desapareció completamente (ghost timeout)
                    if time_in_zone_total >= ghost_timeout:
                        print(f"GHOST TIMEOUT: Track {tracker_id} desapareció de zona {zone_id} ('{zone_info['name']}') después de {time_in_zone_total:.1f}s")
                        evt = {
                            "tenant_id": TENANT_ID,
                            "camera_id": CAMERA_ID,
                            "zone_id": zone_id,
                            "track_id": tracker_id,
                            "event": "exit",
                            "ts": datetime.utcnow().isoformat() + "Z",
                        }
                        redis_client.rpush(DETECTIONS_QUEUE_KEY, json.dumps(evt))
                        last_event_time[event_key] = current_time
                        tracks_to_remove.append(tracker_id)
                    continue
                
                # Track existe pero salió de la zona (salida normal)
                if current_time - last_time >= EVENT_COOLDOWN_SECONDS:
                    dwell_time = time_in_zone_total
                    
                    if dwell_time >= MIN_DWELL_TIME_SECONDS:
                        print(f"EVENT: Track {tracker_id} EXITED zone {zone_id} ('{zone_info['name']}') [dwell: {dwell_time:.1f}s]")
                        evt = {
                            "tenant_id": TENANT_ID,
                            "camera_id": CAMERA_ID,
                            "zone_id": zone_id,
                            "track_id": tracker_id,
                            "event": "exit",
                            "ts": datetime.utcnow().isoformat() + "Z",
                        }
                        if 'dwell' in zone_info.get('metrics', []):
                            evt['dwell_seconds'] = dwell_time
                        
                        redis_client.rpush(DETECTIONS_QUEUE_KEY, json.dumps(evt))
                        last_event_time[event_key] = current_time
                        tracks_to_remove.append(tracker_id)
                    else:
                        # Dwell time muy corto, probablemente falso positivo
                        print(f"SKIP: Track {tracker_id} salió de zona {zone_id} con dwell muy corto ({dwell_time:.1f}s < {MIN_DWELL_TIME_SECONDS}s)")
                        tracks_to_remove.append(tracker_id)
        
        # Remover tracks que salieron
        for tracker_id in tracks_to_remove:
            if tracker_id in prev_tracks_in_zone:
                del prev_tracks_in_zone[tracker_id]
        
        # Limpiar timers de tracks que ya no están en la zona
        if hasattr(timer, 'tracker_id2start_time'):
            active_tracker_ids = current_track_ids_in_zone
            timer.tracker_id2start_time = {
                tid: ts for tid, ts in timer.tracker_id2start_time.items()
                if tid in active_tracker_ids
            }

    # 7. Anotar frame para video stream
    annotated_frame = frame.copy()
    
    # Dibujar todas las zonas y detecciones
    for zone_id, zone_info in ZONES_CONFIG.items():
        zone = zone_info["zone"]
        zone_annotator = zone_info["zone_annotator"]
        box_annotator = zone_info["box_annotator"]
        timer = zone_info["timer"]
        zone_index = zone_info["index"]
        
        # Detectar objetos en esta zona
        is_in_zone = zone.trigger(detections)
        detections_in_zone = detections[is_in_zone]
        
        # Dibujar la zona con conteo
        annotated_frame = zone_annotator.annotate(scene=annotated_frame)
        
        # Dibujar bounding boxes de objetos en la zona
        if len(detections_in_zone) > 0:
            annotated_frame = box_annotator.annotate(
                scene=annotated_frame,
                detections=detections_in_zone
            )
            
            # Calcular tiempos y crear labels
            time_in_zone = timer.tick(detections_in_zone)
            labels = []
            if detections_in_zone.tracker_id is not None:
                for i, tracker_id in enumerate(detections_in_zone.tracker_id):
                    if tracker_id is None:
                        continue
                    time_seconds = time_in_zone[i] if i < len(time_in_zone) else 0
                    minutes = int(time_seconds // 60)
                    seconds = int(time_seconds % 60)
                    labels.append(f"#{int(tracker_id)} {minutes:02d}:{seconds:02d}")
            
            if labels:
                annotated_frame = label_annotator.annotate(
                    scene=annotated_frame,
                    detections=detections_in_zone,
                    labels=labels
                )
    
    # Codificar el frame dibujado a JPEG para la transmisión
    ok, buffer = cv2.imencode('.jpg', annotated_frame)
    if ok:
        frame_bytes = buffer.tobytes()
        redis_client.set(f"annotated_frame_cam_{CAMERA_ID}", frame_bytes)

    # 8. Limpieza periódica de estado
    # Limpiar tracks que ya no existen del diccionario track_first_seen
    active_track_ids = set()
    if detections.tracker_id is not None:
        active_track_ids = set(int(tid) for tid in detections.tracker_id if tid is not None)
    
    tracks_to_remove_first_seen = [
        tid for tid in track_first_seen.keys()
        if tid not in active_track_ids and current_time - track_first_seen[tid] > 30
    ]
    for tid in tracks_to_remove_first_seen:
        del track_first_seen[tid]
    
    # Limpiar eventos antiguos del diccionario de cooldown (más de 5 minutos)
    cutoff_time = current_time - 300
    keys_to_remove = [k for k, v in last_event_time.items() if v < cutoff_time]
    for k in keys_to_remove:
        del last_event_time[k]
