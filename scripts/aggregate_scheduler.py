#!/usr/bin/env python3
"""
Servicio scheduler que ejecuta aggregate_hourly.py cada hora.
Similar a como funciona alerter.py con su loop continuo.
"""
import time
import subprocess
import sys
from datetime import datetime, timedelta
import pytz

# Esperar 1 hora (3600 segundos) entre ejecuciones
HOUR_INTERVAL = 3600

def run_aggregation():
    """Ejecuta el script de agregación horaria."""
    try:
        print(f"[{datetime.now(pytz.utc).isoformat()}] Ejecutando agregación horaria...")
        result = subprocess.run(
            [sys.executable, "scripts/aggregate_hourly.py"],
            cwd=".",
            capture_output=True,
            text=True,
            timeout=600  # Timeout de 10 minutos por si acaso
        )
        
        if result.returncode == 0:
            print(f"[{datetime.now(pytz.utc).isoformat()}] Agregación completada exitosamente.")
            if result.stdout:
                print("Salida:", result.stdout)
        else:
            print(f"[{datetime.now(pytz.utc).isoformat()}] ERROR en agregación:")
            print("STDOUT:", result.stdout)
            print("STDERR:", result.stderr)
            
    except subprocess.TimeoutExpired:
        print(f"[{datetime.now(pytz.utc).isoformat()}] ERROR: Agregación excedió el timeout de 10 minutos")
    except Exception as e:
        print(f"[{datetime.now(pytz.utc).isoformat()}] ERROR inesperado ejecutando agregación: {e}")


def main():
    """Loop principal que ejecuta la agregación cada hora."""
    print("=" * 60)
    print("Iniciando Scheduler de Agregación Horaria")
    print(f"Ejecutará aggregate_hourly.py cada {HOUR_INTERVAL} segundos ({HOUR_INTERVAL/60} minutos)")
    print("=" * 60)
    
    # Calcular cuánto tiempo falta para el próximo minuto 0 de la hora
    # Esto asegura que se ejecute aproximadamente a las :00 de cada hora
    now = datetime.now(pytz.utc)
    next_hour = (now.replace(minute=0, second=0, microsecond=0) + 
                 timedelta(hours=1))
    seconds_until_next_hour = (next_hour - now).total_seconds()
    
    print(f"Esperando {seconds_until_next_hour:.0f} segundos hasta la próxima hora ({next_hour.isoformat()})...")
    time.sleep(seconds_until_next_hour)
    
    # Ejecutar la primera vez
    run_aggregation()
    
    # Luego ejecutar cada hora
    while True:
        try:
            print(f"[{datetime.now(pytz.utc).isoformat()}] Esperando {HOUR_INTERVAL} segundos hasta la próxima ejecución...")
            time.sleep(HOUR_INTERVAL)
            run_aggregation()
        except KeyboardInterrupt:
            print("\nDeteniendo scheduler...")
            break
        except Exception as e:
            print(f"ERROR en el ciclo principal: {e}")
            # Continuar ejecutando aunque haya un error
            time.sleep(60)  # Esperar 1 minuto antes de reintentar


if __name__ == "__main__":
    main()

