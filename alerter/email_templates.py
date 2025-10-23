"""Templates HTML para emails de alertas, adaptado para V2"""
from datetime import datetime
import os

RESEND_FROM_EMAIL = os.getenv("RESEND_FROM_EMAIL", "Clave Alerts <onboarding@resend.dev>")

def get_alert_html(metric: str, level: str, value: float, threshold: float, zone_name: str, camera_name: str) -> tuple[str, str, str]:
    """
    Genera HTML bonito para alertas
    
    Returns:
        (from_email, subject, html_body)
    """
    
    # Colores según nivel
    colors = {
        'warning': {'bg': '#FEF3C7', 'border': '#F59E0B', 'text': '#92400E', 'emoji': '⚠️'},
        'critical': {'bg': '#FEE2E2', 'border': '#DC2626', 'text': '#991B1B', 'emoji': '🚨'}
    }
    
    color = colors.get(level, colors['warning'])
    timestamp = datetime.now().strftime('%H:%M:%S - %d/%m/%Y')
    
    # Templates by metric type
    if metric == 'occupancy':
        subject = f"{color['emoji']} Alerta de Ocupación: {int(value)} personas en {zone_name}"
        title = "Alta Ocupación Detectada"
        message = f"El sistema ha detectado <strong>{int(value)} personas</strong> en la zona <strong>'{zone_name}'</strong> (Cámara: {camera_name}), superando el umbral de {int(threshold)}."
        recommendation = "💡 <strong>Recomendación:</strong> Monitorear la situación y considerar gestionar el flujo de personas si persiste."
            
    elif metric == 'dwell':
        minutes = int(value // 60)
        seconds = int(value % 60)
        thr_minutes = int(threshold // 60)
        thr_seconds = int(threshold % 60)
        
        subject = f"{color['emoji']} Alerta de Permanencia: {minutes}m {seconds}s en {zone_name}"
        title = "Tiempo de Permanencia Extendido"
        message = f"Se ha detectado un tiempo de permanencia promedio de <strong>{minutes}m y {seconds}s</strong> en la zona <strong>'{zone_name}'</strong> (Cámara: {camera_name}), superando el umbral de {thr_minutes}m y {thr_seconds}s."
        recommendation = "💡 <strong>Recomendación:</strong> Verificar si algún cliente necesita asistencia o si hay un problema en el área."

    else:
        subject = f"{color['emoji']} Alerta del Sistema"
        title = "Alerta Detectada"
        message = f"El sistema ha detectado una situación que requiere atención en la zona '{zone_name}'."
        recommendation = "Revisar el dashboard para más detalles."
    
    html = f"""
<!DOCTYPE html>
<html>
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
</head>
<body style="margin: 0; padding: 0; font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, 'Helvetica Neue', Arial, sans-serif; background-color: #f3f4f6;">
    <table width="100%" cellpadding="0" cellspacing="0" style="background-color: #f3f4f6; padding: 20px 0;">
        <tr>
            <td align="center">
                <table width="600" cellpadding="0" cellspacing="0" style="background-color: #ffffff; border-radius: 12px; overflow: hidden; box-shadow: 0 4px 6px rgba(0, 0, 0, 0.1);">
                    <tr>
                        <td style="background: linear-gradient(135deg, #3B5655 0%, #2f4645 100%); padding: 30px 40px; text-align: center;">
                            <img src="https://www.clave.restaurant/clave-logo-white.png" alt="Clave" style="width: 160px; height: auto; margin-bottom: 10px;">
                            <div style="color: #ffffff; font-size: 14px; opacity: 0.9;">Vision Monitoring System</div>
                        </td>
                    </tr>
                    <tr>
                        <td style="padding: 0 40px;">
                            <div style="background-color: {color['bg']}; border-left: 4px solid {color['border']}; padding: 16px 20px; margin: 30px 0 20px 0; border-radius: 6px;">
                                <div style="color: {color['text']}; font-size: 18px; font-weight: 600; margin-bottom: 4px;">
                                    {color['emoji']} {title}
                                </div>
                                <div style="color: {color['text']}; font-size: 13px; opacity: 0.8;">
                                    {timestamp}
                                </div>
                            </div>
                        </td>
                    </tr>
                    <tr>
                        <td style="padding: 0 40px 20px 40px;">
                            <p style="color: #374151; font-size: 16px; line-height: 1.6; margin: 0 0 20px 0;">
                                {message}
                            </p>
                            <div style="background-color: #F9FAFB; border-radius: 8px; padding: 16px; margin: 20px 0;">
                                <p style="color: #6B7280; font-size: 14px; line-height: 1.5; margin: 0;">
                                    {recommendation}
                                </p>
                            </div>
                        </td>
                    </tr>
                    <tr>
                        <td style="padding: 0 40px 30px 40px; text-align: center;">
                            <a href="https://www.clave.restaurant/dashboard/vision" style="display: inline-block; background: linear-gradient(135deg, #3B5655 0%, #2f4645 100%); color: #ffffff; text-decoration: none; padding: 14px 32px; border-radius: 8px; font-weight: 600; font-size: 15px; box-shadow: 0 2px 4px rgba(0, 0, 0, 0.1);">
                                View Live Dashboard
                            </a>
                        </td>
                    </tr>
                    <tr>
                        <td style="background-color: #F9FAFB; padding: 20px 40px; border-top: 1px solid #E5E7EB;">
                            <p style="color: #9CA3AF; font-size: 12px; line-height: 1.5; margin: 0; text-align: center;">
                                This is an automated message from Clave Vision system.
                            </p>
                        </td>
                    </tr>
                </table>
            </td>
        </tr>
    </table>
</body>
</html>
"""
    
    return RESEND_FROM_EMAIL, subject, html
