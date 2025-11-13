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
LATEST_FRAME_MODE = os.getenv("LATEST_FRAME_MODE", "1") in ("1", "true", "TRUE", "yes", "y")
LATEST_FRAME_KEY = f"frames_latest_cam_{CAMERA_ID}"

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

# Configuración mejorada para reconexiones
MAX_RECONNECT_ATTEMPTS = 10
RECONNECT_DELAY = 2.0  # Segundos entre intentos de reconexión
BACKOFF_MULTIPLIER = 1.5  # Multiplicador para backoff exponencial

# --- Conexión a Redis ---
redis_client = redis.from_url(settings.redis_url.unicode_string())

# --- Bucle principal de captura mejorado ---
print(f"Capture service started for Camera ID: {CAMERA_ID} at {FPS} FPS")

def create_capture():
    """Crea una nueva captura de video con configuración optimizada"""
    cap = cv2.VideoCapture(RTSP_URL)
    if cap.isOpened():
        # Configurar buffer size para reducir latencia
        cap.set(cv2.CAP_PROP_BUFFERSIZE, 1)
        # Configurar timeout para evitar bloqueos indefinidos
        cap.set(cv2.CAP_PROP_OPEN_TIMEOUT_MSEC, 5000)
        cap.set(cv2.CAP_PROP_READ_TIMEOUT_MSEC, 5000)
    return cap

cap = create_capture()
consecutive_failures = 0
last_successful_frame_time = time.time()

while True:
    current_time = time.time()
    
    # Verificar si la conexión está abierta
    if not cap.isOpened():
        consecutive_failures += 1
        print(f"Capture service: Stream for camera {CAMERA_ID} disconnected (intento {consecutive_failures}/{MAX_RECONNECT_ATTEMPTS}). Reconnecting...")
        cap.release()
        
        if consecutive_failures >= MAX_RECONNECT_ATTEMPTS:
            print(f"Capture service: Máximo de intentos alcanzado. Esperando más tiempo antes de reintentar...")
            time.sleep(30)
            consecutive_failures = 0
        
        delay = RECONNECT_DELAY * (BACKOFF_MULTIPLIER ** min(consecutive_failures, 5))
        time.sleep(delay)
        cap = create_capture()
        continue

    # Intentar leer frame
    ok, frame = cap.read()
    
    if not ok or frame is None:
        consecutive_failures += 1
        print(f"Capture service: Cannot read frame from camera {CAMERA_ID} (intento {consecutive_failures}/{MAX_RECONNECT_ATTEMPTS})")
        
        # Verificar si el frame está completamente negro/gris (posible problema de stream)
        if frame is not None:
            gray = cv2.cvtColor(frame, cv2.COLOR_BGR2GRAY)
            mean_brightness = gray.mean()
            if mean_brightness < 10:  # Frame muy oscuro, probablemente stream corrupto
                print(f"Capture service: Frame demasiado oscuro (brightness: {mean_brightness:.1f}), reconectando...")
                cap.release()
                time.sleep(RECONNECT_DELAY)
                cap = create_capture()
                continue
        
        cap.release()
        delay = RECONNECT_DELAY * (BACKOFF_MULTIPLIER ** min(consecutive_failures, 5))
        time.sleep(delay)
        cap = create_capture()
        continue
    
    # Frame leído exitosamente
    consecutive_failures = 0
    last_successful_frame_time = current_time
    
    # Codificar el frame a JPEG y luego a base64
    encode_params = [cv2.IMWRITE_JPEG_QUALITY, 85]  # Calidad reducida para menor tamaño
    _, buffer = cv2.imencode('.jpg', frame, encode_params)
    
    if buffer is None:
        print(f"Capture service: Error al codificar frame para camera {CAMERA_ID}")
        continue
    
    frame_b64 = base64.b64encode(buffer).decode('utf-8')

    # Crear el payload
    payload = {
        "camera_id": CAMERA_ID,
        "ts": current_time,
        "frame_b64": frame_b64
    }

    # Publicación en Redis
    try:
        if LATEST_FRAME_MODE:
            # Modo baja latencia: solo conservar el último frame
            # Guardamos como JSON (bytes) y opcionalmente ponemos TTL corto
            redis_client.set(LATEST_FRAME_KEY, json.dumps(payload))
            # TTL opcional para evitar claves viejas si el productor muere
            redis_client.expire(LATEST_FRAME_KEY, 5)
        else:
            # Modo cola: push con backpressure
            queue_length = redis_client.llen(FRAMES_QUEUE_KEY)
            if queue_length > 100:  # Si hay backlog, saltar este frame
                print(f"Capture service: Cola llena ({queue_length} frames), saltando frame")
            else:
                redis_client.rpush(FRAMES_QUEUE_KEY, json.dumps(payload))
    except Exception as e:
        print(f"Capture service: Error al publicar frame a Redis: {e}")
    
    # Esperar el intervalo de tiempo correcto para mantener el FPS deseado
    elapsed = time.time() - current_time
    sleep_time = max(0, FRAME_INTERVAL - elapsed)
    if sleep_time > 0:
        time.sleep(sleep_time)
