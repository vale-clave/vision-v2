import os
import argparse
from datetime import datetime, timedelta, timezone
import psycopg2
from psycopg2.extras import DictCursor
import google.generativeai as genai
from dotenv import load_dotenv

from shared.db import get_conn

# Cargar variables de entorno desde el archivo .env
load_dotenv()

# Definir la zona horaria de Ecuador
ECUADOR_TZ = timezone(timedelta(hours=-5))

def fetch_weekly_data(conn, start_date, end_date):
    """Obtiene los datos horarios agregados de la última semana."""
    query = """
        SELECT
            hm.ts,
            z.name as zone_name,
            c.name as camera_name,
            hm.avg_occupancy,
            hm.max_occupancy,
            hm.avg_dwell_seconds,
            hm.total_entries
        FROM hourly_metrics hm
        JOIN zones z ON hm.zone_id = z.id
        JOIN cameras c ON z.camera_id = c.id
        WHERE hm.ts >= %s AND hm.ts < %s
        ORDER BY z.name, hm.ts;
    """
    # Usamos un cursor con nombre para obtener los nombres de las columnas
    with conn.cursor(cursor_factory=DictCursor) as cur:
        cur.execute(query, (start_date, end_date))
        return [dict(row) for row in cur.fetchall()]

def format_data_for_llm(data):
    """Formatea los datos en una cadena legible para el prompt del LLM."""
    report_lines = []
    current_zone = None
    for row in data:
        zone_name = f"{row['zone_name']} ({row['camera_name']})"
        if zone_name != current_zone:
            current_zone = zone_name
            report_lines.append(f"\n**Zona: {current_zone}**")

        # Convertir timestamp a la zona horaria de Ecuador para mostrarlo
        ts_ecuador = row['ts'].astimezone(ECUADOR_TZ)
        ts_str = ts_ecuador.strftime('%A, %H:%M')
        avg_occ = round(row['avg_occupancy'], 1)
        max_occ = row['max_occupancy']
        # Convertir a minutos para que sea más legible para el LLM
        avg_dwell_min = round(row['avg_dwell_seconds'] / 60, 1)
        entries = row['total_entries']

        # Solo informar sobre horas con actividad para mantener el prompt conciso
        if entries > 0 or avg_occ > 0:
             report_lines.append(
                f"- {ts_str}: Ocupación Promedio: {avg_occ}, Ocupación Máxima: {max_occ}, "
                f"Estancia Promedio: {avg_dwell_min} min, Entradas: {entries}"
            )
    return "\n".join(report_lines)

def generate_insights_with_gemini(data_string):
    """Envía los datos a Gemini y obtiene los insights."""
    api_key = os.getenv("GOOGLE_API_KEY")
    if not api_key:
        raise ValueError("La variable de entorno GOOGLE_API_KEY no está configurada.")
    
    genai.configure(api_key=api_key)
    model = genai.GenerativeModel('gemini-2.5-flash')

    prompt = f"""
        Eres un analista de operaciones para un restaurante llamado "Clave". Tu tarea es analizar los datos de afluencia de la última semana y generar un resumen ejecutivo con insights accionables para el gerente. Eres conciso, profesional y te enfocas en lo que es más importante.

        Aquí están los datos de métricas por hora de la última semana, separados por zona:
        {data_string}

        Por favor, genera un reporte en formato Markdown con la siguiente estructura:
        1.  **Resumen General:** Un párrafo que describa la tendencia general de la semana. ¿Fue una semana ocupada? ¿Hubo algún día que destacara?
        2.  **Puntos Críticos y Horas Pico:** Identifica los 3-5 momentos o patrones más importantes de la semana (ej. "El Martes al mediodía hubo una congestión significativa en la zona de caja que duró 2 horas"). Sé específico.
        3.  **Observaciones Clave:** Menciona cualquier patrón interesante o inesperado (ej. "Los Miércoles por la noche el tiempo de estancia en el área principal es inusualmente alto, sugiriendo grupos grandes que se quedan más tiempo").
        4.  **Recomendaciones:** Ofrece 1 o 2 sugerencias concretas y accionables basadas en los datos (ej. "Considerar asignar un empleado adicional a la caja los Martes entre las 12 PM y 2 PM para reducir la espera").
    """
    
    response = model.generate_content(prompt)
    return response.text

def save_report_to_db(conn, start_date, end_date, summary):
    """Guarda el reporte generado en la tabla weekly_reports."""
    query = """
        INSERT INTO weekly_reports (start_date, end_date, llm_summary_markdown, status)
        VALUES (%s, %s, %s, 'completed')
        ON CONFLICT (start_date, end_date) DO UPDATE SET
            llm_summary_markdown = EXCLUDED.llm_summary_markdown,
            status = EXCLUDED.status,
            generated_at = NOW();
    """
    with conn.cursor() as cur:
        cur.execute(query, (start_date, end_date, summary))
        conn.commit()

def main():
    # Lógica para determinar start_date y end_date para el reporte.
    # Corregido: siempre calcula la semana pasada completa (de Lunes a Domingo).
    today_ecuador = datetime.now(ECUADOR_TZ).date()
    # Retrocede al domingo de la semana pasada
    last_week_sunday = today_ecuador - timedelta(days=(today_ecuador.weekday() + 1) % 7)
    # El final del rango del reporte es el Lunes siguiente a ese Domingo (para consultas < end_date)
    end_date = last_week_sunday + timedelta(days=1)
    # El inicio del rango es 7 días antes de ese Lunes
    start_date = end_date - timedelta(days=7)

    print(f"Generando reporte para la semana: {start_date} a {end_date - timedelta(days=1)} (Zona Horaria Ecuador)")

    try:
        with get_conn() as conn:
            print("Obteniendo datos de la base de datos...")
            weekly_data = fetch_weekly_data(conn, start_date, end_date)
            
            if not weekly_data:
                print("No se encontraron datos para la semana especificada. Saliendo.")
                return

            print("Formateando datos para el LLM...")
            formatted_data = format_data_for_llm(weekly_data)
            
            print("Generando insights con Gemini...")
            summary = generate_insights_with_gemini(formatted_data)
            print("--- Resumen de la IA ---")
            print(summary)
            print("----------------------")

            print("Guardando reporte en la base de datos...")
            save_report_to_db(conn, start_date, end_date, summary)
            print("¡Reporte guardado exitosamente!")

    except (psycopg2.Error, ValueError) as e:
        print(f"Ocurrió un error: {e}")
    except ImportError:
        print("Error: Faltan dependencias. Por favor, ejecuta 'pip install -r requirements.txt'")


if __name__ == "__main__":
    main()
