#!/bin/bash

# Este script orquesta el lanzamiento de todos los servicios de Vision V2
# utilizando tmux para gestionar las sesiones en segundo plano.

SESSION_NAME="vision"

# Usuario y host del VPS que tiene la IP fija (ajusta si usas otro usuario)
VPS_USER="root"
VPS_HOST="64.225.10.55"
SSH_KEY_PATH="$HOME/.ssh/id_vision_vps"

# --- Paso 1: Limpiar sesiones anteriores ---
echo "Limpiando sesiones de tmux anteriores..."
tmux kill-session -t $SESSION_NAME 2>/dev/null || true

# Forzar el inicio del servidor de tmux
tmux start-server

# --- Paso 2: Crear nueva sesión de tmux ---
echo "Creando nueva sesión de tmux: $SESSION_NAME"
tmux new-session -d -s $SESSION_NAME

# --- Paso 3: Sincronizar la configuración con la base de datos ---
echo "Sincronizando config.yaml con la base de datos..."
tmux new-window -t $SESSION_NAME:1 -n "ConfigLoader"
tmux send-keys -t $SESSION_NAME:1 "PYTHONPATH=. python3 scripts/sync_config.py" C-m
# Dar un pequeño margen para que la sincronización termine antes de lanzar los otros servicios
sleep 5

# --- Paso 4: Lanzar servicios principales ---
echo "Lanzando servicios principales (API, Ingest, Alerter)..."

# API
tmux new-window -t $SESSION_NAME:2 -n "API"
tmux send-keys -t $SESSION_NAME:2 "uvicorn api.main:app --host 0.0.0.0 --port 8888" C-m

# Ingest
tmux new-window -t $SESSION_NAME:3 -n "Ingest"
tmux send-keys -t $SESSION_NAME:3 "PYTHONPATH=. python3 ingest/ingest.py" C-m

# Alerter
tmux new-window -t $SESSION_NAME:4 -n "Alerter"
tmux send-keys -t $SESSION_NAME:4 "PYTHONPATH=. python3 -m alerter.alerter" C-m

# Aggregate Scheduler (ejecuta aggregate_hourly.py cada hora)
tmux new-window -t $SESSION_NAME:5 -n "AggregateScheduler"
tmux send-keys -t $SESSION_NAME:5 "PYTHONPATH=. python3 scripts/aggregate_scheduler.py" C-m

# --- Paso 6: Lanzar Sampler de Ocupación (muestras cada 5s) ---
echo "Lanzando Occupancy Sampler..."
tmux new-window -t $SESSION_NAME:6 -n "Sampler"
tmux send-keys -t $SESSION_NAME:6 "PYTHONPATH=. python3 scripts/occupancy_sampler.py" C-m

# --- Paso 5: Lanzar dinámicamente los workers y captures ---
echo "Lanzando workers y captures dinámicamente desde config.yaml..."

# --- Preparar túnel SOCKS5 hacia el VPS (IP estática) ---
echo "Verificando/levantando túnel SSH SOCKS5 hacia $VPS_HOST..."
if ! nc -z 127.0.0.1 1080 2>/dev/null; then
  if [ ! -f "$SSH_KEY_PATH" ]; then
    echo "ERROR: No se encontró la llave SSH en $SSH_KEY_PATH"
    echo "Copia tu llave privada para el VPS a $SSH_KEY_PATH y dale permisos 600."
    exit 1
  fi

  ssh -o StrictHostKeyChecking=no \
      -i "$SSH_KEY_PATH" \
      -D 1080 -f -N "$VPS_USER@$VPS_HOST" || {
    echo "Error creando túnel SSH al VPS $VPS_HOST"
    exit 1
  }
  echo "Túnel SOCKS5 levantado en 127.0.0.1:1080 hacia $VPS_HOST"
else
  echo "Túnel SOCKS5 ya parece estar escuchando en 127.0.0.1:1080, se reutiliza."
fi

# Extraer los IDs de las cámaras del config.yaml usando yq
# yq es un procesador de YAML para la línea de comandos, similar a jq para JSON.
# Lo instalaremos si no existe.
if ! command -v yq &> /dev/null
then
    echo "yq no encontrado. Instalando yq..."
    sudo wget https://github.com/mikefarah/yq/releases/latest/download/yq_linux_amd64 -O /usr/bin/yq && sudo chmod +x /usr/bin/yq
fi

CAMERA_IDS=$(yq e '.tenants[].cameras[].id' config.yaml)

WINDOW_INDEX=7
for CAM_ID in $CAMERA_IDS
do
  echo "Lanzando servicios para Cámara ID: $CAM_ID"
  
  # Capture
  tmux new-window -t $SESSION_NAME:$WINDOW_INDEX -n "Capture-$CAM_ID"
  tmux send-keys -t $SESSION_NAME:$WINDOW_INDEX \
    "CAMERA_ID=$CAM_ID PYTHONPATH=. PROXYCHAINS_CONF=\$PWD/proxychains.conf proxychains4 python3 capture/capture.py" C-m
  let WINDOW_INDEX++

  # Worker
  tmux new-window -t $SESSION_NAME:$WINDOW_INDEX -n "Worker-$CAM_ID"
  tmux send-keys -t $SESSION_NAME:$WINDOW_INDEX "CAMERA_ID=$CAM_ID PYTHONPATH=. python3 worker/worker.py" C-m
  let WINDOW_INDEX++
done

# --- Finalización ---
# Eliminar la ventana inicial por defecto (bash)
tmux kill-window -t $SESSION_NAME:0

echo "----------------------------------------------------"
echo "¡Todos los servicios de Vision V2 han sido lanzados!"
echo "Puedes adjuntarte a la sesión con: tmux attach -t $SESSION_NAME"
echo "Puedes despegarte con: Ctrl+b, luego d"
echo "----------------------------------------------------"
