import os, time, json, redis, base64, numpy as np
from datetime import datetime
from shapely.geometry import Point, Polygon
import yaml
from pathlib import Path
from shared.settings import settings
from PIL import Image
import io
import torch
import cv2 # <- NUEVA IMPORTACIÓN

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
        "metrics": z.get("metrics", []),
        "ghost_timeout_seconds": z.get("ghost_timeout_minutes", 60) * 60  # Convertir minutos a segundos
    }

# --- Cargar el modelo YOLO ---
# Esta es la parte que antes causaba el conflicto. Ahora corre en un proceso separado.
MODEL_WEIGHTS = os.getenv("YOLO_WEIGHTS", "weights/yolov8s-world.pt")
model = YOLO(MODEL_WEIGHTS)
print("Worker: Modelo YOLO cargado con éxito.")

print(f"Worker started for Camera ID: {CAMERA_ID}")

# Diccionario para guardar el estado de los tracks
prev_tracks = {}
# Diccionario para guardar el último tiempo que se generó un evento para cada (track_id, zone_id)
# Esto previene eventos duplicados muy seguidos (debounce)
last_event_time = {}
# Cooldown mínimo entre eventos del mismo track en la misma zona (en segundos)
EVENT_COOLDOWN_SECONDS = 5.0  # Aumentado de 2.0 a 5.0 para reducir falsos positivos
# Tiempo mínimo que un track debe estar en una zona antes de generar evento de entrada (en segundos)
MIN_TRACK_AGE_FOR_ENTER = 1.0
# Tiempo mínimo de dwell time para considerar válido (en segundos)
MIN_DWELL_TIME_SECONDS = 3.0
# Configuración de tracking mejorada
TRACK_CONFIDENCE_THRESHOLD = 0.5  # Aumentado de default para reducir falsos positivos
TRACK_IOU_THRESHOLD = 0.7  # Aumentado para reducir ID switches
# Diccionario para rastrear cuándo se detectó por primera vez cada track
track_first_seen = {}

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

    # 3. Inferencia y Tracking con configuración mejorada
    # Configuración optimizada para reducir falsos positivos y mejorar tracking
    results = model.track(
        frame, 
        classes=[0],  # Solo personas
        verbose=False, 
        persist=True, 
        tracker="bytetrack.yaml",
        conf=TRACK_CONFIDENCE_THRESHOLD,  # Filtrar detecciones con baja confianza
        iou=TRACK_IOU_THRESHOLD,  # IOU más alto para reducir ID switches
        imgsz=640,  # Tamaño de imagen consistente
        device='cuda' if torch.cuda.is_available() else 'cpu'
    )[0]

    # 4. DIBUJAR ANOTACIONES Y ENVIAR A REDIS PARA EL STREAM DE VIDEO
    # El método plot() de ultralytics convenientemente devuelve el frame con las cajas dibujadas.
    annotated_frame = results.plot()

    # DIBUJAR POLÍGONOS DE LAS ZONAS
    for zone_id, zinfo in ZONES.items():
        # Obtener los puntos del polígono
        poly_coords = list(zinfo["poly"].exterior.coords)
        poly_points = np.array(poly_coords, dtype=np.int32).reshape((-1, 1, 2))
        
        # Dibujar el polígono con color semi-transparente
        # Usamos diferentes colores para cada zona
        colors = {
            1: (0, 255, 0),    # Verde - Interior Area
            2: (255, 0, 0),    # Azul - Register
            3: (0, 165, 255),  # Naranja - Drivers Queue
            4: (255, 255, 0),  # Cyan - Dining Area Outside
            5: (255, 0, 255),  # Magenta - Break Area
            6: (0, 255, 255),  # Amarillo - Inside Dining Area
        }
        color = colors.get(zone_id, (255, 255, 255))
        
        # Dibujar polígono relleno semi-transparente
        overlay = annotated_frame.copy()
        cv2.fillPoly(overlay, [poly_points], color)
        cv2.addWeighted(overlay, 0.2, annotated_frame, 0.8, 0, annotated_frame)
        
        # Dibujar el borde del polígono
        cv2.polylines(annotated_frame, [poly_points], True, color, 2)
        
        # Agregar etiqueta con el nombre de la zona
        centroid = zinfo["poly"].centroid
        label = f"Zone {zone_id}: {zinfo['name']}"
        cv2.putText(annotated_frame, label, (int(centroid.x), int(centroid.y)), 
                   cv2.FONT_HERSHEY_SIMPLEX, 0.5, color, 2)

    # Codificar el frame dibujado a JPEG para la transmisión
    ok, buffer = cv2.imencode('.jpg', annotated_frame)
    if ok:
        frame_bytes = buffer.tobytes()
        # Guardamos el frame en una clave simple, sobrescribiendo la anterior.
        # Es más eficiente para un stream de video que una lista.
        redis_client.set(f"annotated_frame_cam_{CAMERA_ID}", frame_bytes)


    # 5. Lógica de Eventos de Entrada/Salida de Zona mejorada
    current_tracks = {}
    current_time = time.time()
    
    # Rastrear cuándo se vio por primera vez cada track
    if results.boxes.id is not None:
        for box in results.boxes:
            track_id = int(box.id[0])
            confidence = float(box.conf[0].cpu().numpy())
            
            # Filtrar detecciones con confianza muy baja (doble filtro)
            if confidence < TRACK_CONFIDENCE_THRESHOLD:
                continue
            
            x1, y1, x2, y2 = box.xyxy[0].cpu().numpy()
            cx = (x1 + x2) / 2
            cy = (y1 + y2) / 2
            current_tracks[track_id] = Point(cx, cy)
            
            # Registrar cuándo se vio por primera vez este track
            if track_id not in track_first_seen:
                track_first_seen[track_id] = current_time

    # --- Lógica de Eventos de Entrada/Salida de Zona con Debounce mejorado ---
    for track_id, point in current_tracks.items():
        # Verificar que el track tenga suficiente edad antes de generar eventos
        track_age = current_time - track_first_seen.get(track_id, current_time)
        
        for zone_id, zinfo in ZONES.items():
            key = (track_id, zone_id)
            event_key = (track_id, zone_id, "enter")
            
            if zinfo["poly"].contains(point):
                if key not in prev_tracks:
                    # Verificar cooldown y edad mínima del track antes de generar evento de entrada
                    last_time = last_event_time.get(event_key, 0)
                    time_since_last_event = current_time - last_time
                    
                    # Requerir que el track tenga al menos MIN_TRACK_AGE_FOR_ENTER segundos de edad
                    # y que haya pasado el cooldown
                    if time_since_last_event >= EVENT_COOLDOWN_SECONDS and track_age >= MIN_TRACK_AGE_FOR_ENTER:
                        print(f"EVENT: Track {track_id} ENTERED zone {zone_id} ('{zinfo['name']}') [age: {track_age:.1f}s]")
                        evt = {
                            "tenant_id": TENANT_ID,
                            "camera_id": CAMERA_ID,
                            "zone_id": zone_id,
                            "track_id": track_id,
                            "event": "enter",
                            "ts": datetime.utcnow().isoformat() + "Z",
                        }
                        redis_client.rpush(DETECTIONS_QUEUE_KEY, json.dumps(evt))
                        prev_tracks[key] = current_time
                        last_event_time[event_key] = current_time
                    else:
                        # Aún en cooldown o track muy nuevo, pero marcar como dentro para evitar eventos de salida falsos
                        prev_tracks[key] = current_time
                else:
                    # Ya estaba dentro, actualizar el tiempo
                    prev_tracks[key] = current_time
    
    exited_keys = []
    ghost_exit_keys = []
    
    for key in prev_tracks:
        track_id, zone_id = key
        zone_info = ZONES[zone_id]
        ghost_timeout = zone_info["ghost_timeout_seconds"]
        event_key = (track_id, zone_id, "exit")
        
        # Verificar primero si el track desapareció completamente (ghost timeout)
        if track_id not in current_tracks:
            time_in_zone = current_time - prev_tracks[key]
            if time_in_zone >= ghost_timeout:
                # Ghost timeout: el track desapareció sin salida detectada
                print(f"GHOST TIMEOUT: Track {track_id} desapareció de zona {zone_id} ('{zone_info['name']}') después de {time_in_zone:.1f}s (timeout: {ghost_timeout}s)")
                evt = {
                    "tenant_id": TENANT_ID,
                    "camera_id": CAMERA_ID,
                    "zone_id": zone_id,
                    "track_id": track_id,
                    "event": "exit",
                    "ts": datetime.utcnow().isoformat() + "Z",
                }
                # No agregamos dwell porque es un ghost (no sabemos cuánto tiempo realmente estuvo)
                redis_client.rpush(DETECTIONS_QUEUE_KEY, json.dumps(evt))
                last_event_time[event_key] = current_time
                ghost_exit_keys.append(key)
                continue  # Saltar el procesamiento normal de salida
        
        # Procesamiento normal de salida (track existe pero salió de la zona)
        if track_id in current_tracks:
            is_outside = not ZONES[zone_id]["poly"].contains(current_tracks[track_id])
            if is_outside:
                # Verificar cooldown antes de generar evento de salida
                last_time = last_event_time.get(event_key, 0)
                if current_time - last_time >= EVENT_COOLDOWN_SECONDS:
                    start_time = prev_tracks[key]
                    dwell_time = current_time - start_time
                    
                    # Solo generar evento de salida si el dwell time es válido (mayor al mínimo)
                    # Esto filtra tracks muy cortos que probablemente son falsos positivos
                    if dwell_time >= MIN_DWELL_TIME_SECONDS:
                        print(f"EVENT: Track {track_id} EXITED zone {zone_id} ('{ZONES[zone_id]['name']}') [dwell: {dwell_time:.1f}s]")
                        evt = {
                            "tenant_id": TENANT_ID,
                            "camera_id": CAMERA_ID,
                            "zone_id": zone_id,
                            "track_id": track_id,
                            "event": "exit",
                            "ts": datetime.utcnow().isoformat() + "Z",
                        }
                        if 'dwell' in ZONES[zone_id].get('metrics', []):
                            evt['dwell'] = dwell_time
                        
                        redis_client.rpush(DETECTIONS_QUEUE_KEY, json.dumps(evt))
                        last_event_time[event_key] = current_time
                        exited_keys.append(key)
                    else:
                        # Dwell time muy corto, probablemente falso positivo - simplemente remover sin evento
                        print(f"SKIP: Track {track_id} salió de zona {zone_id} con dwell muy corto ({dwell_time:.1f}s < {MIN_DWELL_TIME_SECONDS}s) - ignorando")
                        exited_keys.append(key)
                # Si está en cooldown, no hacer nada (mantener el estado actual)

    # Remover los tracks que salieron normalmente
    for key in exited_keys:
        del prev_tracks[key]
    
    # Remover los tracks que fueron marcados como ghosts
    for key in ghost_exit_keys:
        del prev_tracks[key]
    
    # Limpiar tracks que ya no existen del diccionario track_first_seen
    active_track_ids = set(current_tracks.keys())
    tracks_to_remove = [tid for tid in track_first_seen.keys() if tid not in active_track_ids]
    for tid in tracks_to_remove:
        # Solo remover si el track lleva desaparecido más de 30 segundos
        if current_time - track_first_seen[tid] > 30:
            del track_first_seen[tid]
    
    # Limpiar eventos antiguos del diccionario de cooldown (más de 5 minutos)
    cutoff_time = current_time - 300
    keys_to_remove = [k for k, v in last_event_time.items() if v < cutoff_time]
    for k in keys_to_remove:
        del last_event_time[k]
