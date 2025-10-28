#!/bin/bash

# Este script orquesta el lanzamiento de todos los servicios de Vision V2
# utilizando tmux para gestionar las sesiones en segundo plano.

SESSION_NAME="vision"

# --- Paso 1: Limpiar sesiones anteriores ---
echo "Limpiando sesiones de tmux anteriores..."
tmux kill-session -t $SESSION_NAME 2>/dev/null || true

# --- Paso 2: Crear nueva sesión de tmux ---
echo "Creando nueva sesión de tmux: $SESSION_NAME"
tmux new-session -d -s $SESSION_NAME

# --- Paso 3: Sincronizar la configuración con la base de datos ---
echo "Sincronizando config.yaml con la base de datos..."
tmux new-window -t $SESSION_NAME:1 -n "ConfigLoader"
tmux send-keys -t $SESSION_NAME:1 "PYTHONPATH=. python3 shared/config_loader.py" C-m
# Dar un pequeño margen para que la sincronización termine antes de lanzar los otros servicios
sleep 5

# --- Paso 4: Lanzar servicios principales ---
echo "Lanzando servicios principales (API, Ingest, Alerter)..."

# API
tmux new-window -t $SESSION_NAME:2 -n "API"
tmux send-keys -t $SESSION_NAME:2 "uvicorn api.main:app --host 0.0.0.0 --port 8000" C-m

# Ingest
tmux new-window -t $SESSION_NAME:3 -n "Ingest"
tmux send-keys -t $SESSION_NAME:3 "PYTHONPATH=. python3 ingest/ingest.py" C-m

# Alerter
tmux new-window -t $SESSION_NAME:4 -n "Alerter"
tmux send-keys -t $SESSION_NAME:4 "PYTHONPATH=. python3 -m alerter.alerter" C-m

# --- Paso 5: Lanzar dinámicamente los workers y captures ---
echo "Lanzando workers y captures dinámicamente desde config.yaml..."

# Extraer los IDs de las cámaras del config.yaml usando yq
# yq es un procesador de YAML para la línea de comandos, similar a jq para JSON.
# Lo instalaremos si no existe.
if ! command -v yq &> /dev/null
then
    echo "yq no encontrado. Instalando yq..."
    sudo wget https://github.com/mikefarah/yq/releases/latest/download/yq_linux_amd64 -O /usr/bin/yq && sudo chmod +x /usr/bin/yq
fi

CAMERA_IDS=$(yq e '.tenants[].cameras[].id' config.yaml)

WINDOW_INDEX=5
for CAM_ID in $CAMERA_IDS
do
  echo "Lanzando servicios para Cámara ID: $CAM_ID"
  
  # Capture
  tmux new-window -t $SESSION_NAME:$WINDOW_INDEX -n "Capture-$CAM_ID"
  tmux send-keys -t $SESSION_NAME:$WINDOW_INDEX "CAMERA_ID=$CAM_ID PYTHONPATH=. python3 capture/capture.py" C-m
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
