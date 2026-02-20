import os
import time
import requests
import xmlrpc.client
from datetime import datetime, date
from collections import defaultdict
import pandas as pd

# ===========================================================
# CONFIG (RELLENA SOLO LO QUE FALTA)
# ===========================================================
ODOO_URL = os.getenv('ODOO_URL')
ODOO_USER = os.getenv('ODOO_USER')
ODOO_PASSWORD = os.getenv('ODOO_PASSWORD')
ODOO_DB = os.getenv('ODOO_DB')
SLACK_BOT_TOKEN = os.getenv('SLACK_BOT_TOKEN')
SLACK_CHANNEL_ID = os.getenv('SLACK_CHANNEL_ID')

MAX_ROWS = 5000
SEND_IF_ZERO = False  # True => si no hay pendientes, no envía nada

OUT_DIR = "."  # carpeta donde guardar el Excel
MODEL_PICKING = "lo.stock.picking"   # vuestro modelo logístico

# IDs de tipo operación
PICKING_TYPES = [6, 35, 87]  # Devoluciones + Devoluciones Reveni + Cambios

# Estados pendientes
PENDING_STATES = ["assigned", "waiting", "confirmed"]

# ============================================================
# HELPERS
# ============================================================
def log(msg: str) -> None:
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}")

def safe_execute_kw(models_proxy, db, uid, pwd, model, method, args=None, kwargs=None, label=None):
    if args is None:
        args = []
    if kwargs is None:
        kwargs = {}
    tag = f" ({label})" if label else ""
    log(f"→ RPC{tag}: {model}.{method}")
    t0 = time.time()
    out = models_proxy.execute_kw(db, uid, pwd, model, method, args, kwargs)
    log(f"← RPC{tag}: OK en {time.time() - t0:.2f}s")
    return out

def fmt(val):
    return val or "—"

def first_day_of_month(d: date) -> date:
    return d.replace(day=1)

def first_day_next_month(d: date) -> date:
    if d.month == 12:
        return date(d.year + 1, 1, 1)
    return date(d.year, d.month + 1, 1)

def get_week_iso(dt):
    try:
        return dt.isocalendar()[1]
    except Exception:
        return -1

def get_month_name(month_num):
    """Devuelve nombre del mes en español"""
    months = {
        1: "Enero", 2: "Febrero", 3: "Marzo", 4: "Abril",
        5: "Mayo", 6: "Junio", 7: "Julio", 8: "Agosto",
        9: "Septiembre", 10: "Octubre", 11: "Noviembre", 12: "Diciembre"
    }
    return months.get(month_num, f"Mes {month_num}")

# ============================================================
# SLACK HELPERS
# ============================================================
def slack_api_post(method: str, data=None, files=None):
    token = SLACK_BOT_TOKEN or os.getenv("SLACK_BOT_TOKEN")
    if not token:
        raise Exception("Falta SLACK_BOT_TOKEN (rellénalo arriba o en os.environ['SLACK_BOT_TOKEN']).")

    url = f"https://slack.com/api/{method}"
    headers = {"Authorization": f"Bearer {token}"}

    if files:
        r = requests.post(url, headers=headers, data=data, files=files, timeout=60)
    else:
        headers["Content-Type"] = "application/json; charset=utf-8"
        r = requests.post(url, headers=headers, json=data, timeout=60)

    r.raise_for_status()
    j = r.json()
    if not j.get("ok"):
        raise Exception(f"Slack API error ({method}): {j}")
    return j


def slack_api_post_form(method: str, form: dict):
    token = SLACK_BOT_TOKEN or os.getenv("SLACK_BOT_TOKEN")
    if not token:
        raise Exception("Falta SLACK_BOT_TOKEN.")

    url = f"https://slack.com/api/{method}"
    headers = {"Authorization": f"Bearer {token}"}

    r = requests.post(url, headers=headers, data=form, timeout=60)  # <-- form-encoded
    r.raise_for_status()
    j = r.json()
    if not j.get("ok"):
        raise Exception(f"Slack API error ({method}): {j}")
    return j


def send_to_slack_with_excel(channel_id: str, text: str, excel_path: str, title: str, in_thread: bool = True):
    # 1) mensaje (tu slack_api_post actual sirve)
    msg = slack_api_post("chat.postMessage", data={
        "channel": channel_id,
        "text": text,
        "mrkdwn": True,
    })
    ts = msg["ts"]

    filename = os.path.basename(excel_path)
    file_size = os.path.getsize(excel_path)

    # 2) pedir upload_url + file_id (FORM, no JSON)
    upload_ctx = slack_api_post_form("files.getUploadURLExternal", {
        "filename": filename,
        "length": str(file_size),  # a veces Slack lo quiere como string
    })

    upload_url = upload_ctx.get("upload_url")
    file_id = upload_ctx.get("file_id")
    if not upload_url or not file_id:
        raise Exception(f"Respuesta inesperada de files.getUploadURLExternal: {upload_ctx}")

    # 3) subir binario a upload_url
    with open(excel_path, "rb") as f:
        r = requests.post(
            upload_url,
            files={"file": (filename, f, "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")},
            timeout=120
        )
    if r.status_code != 200:
        raise Exception(f"Error subiendo a upload_url (HTTP {r.status_code}): {r.text[:300]}")

    # 4) completar y compartir en canal (FORM)
    complete_form = {
        "channel_id": channel_id,
        "initial_comment": f"Adjunto: *{title}*",
        "files": f'[{{"id":"{file_id}","title":"{title}"}}]',
    }
    if in_thread:
        complete_form["thread_ts"] = ts

    slack_api_post_form("files.completeUploadExternal", complete_form)


# ============================================================
# VALIDACIÓN BÁSICA
# ============================================================
if not (ODOO_PASSWORD or os.getenv("ODOO_PASSWORD")):
    raise Exception("Falta ODOO_PASSWORD (rellénalo arriba o en os.environ['ODOO_PASSWORD']).")

# ============================================================
# CONEXIÓN ODOO
# ============================================================
log("🔐 Conectando a Odoo (XML-RPC)...")
common = xmlrpc.client.ServerProxy(f"{ODOO_URL}/xmlrpc/2/common", allow_none=True)
uid = common.authenticate(ODOO_DB, ODOO_USER, ODOO_PASSWORD or os.getenv("ODOO_PASSWORD"), {})
if not uid:
    raise Exception("❌ No se pudo autenticar en Odoo (revisa DB/USER/PASS)")
log(f"✅ Autenticado. uid={uid}")

models = xmlrpc.client.ServerProxy(f"{ODOO_URL}/xmlrpc/2/object", allow_none=True)

# ============================================================
# 1) OBTENER NOMBRES DE TIPOS DE OPERACIÓN
# ============================================================
log("🔎 Obteniendo nombres de tipos de operación...")

picking_type_data = safe_execute_kw(
    models, ODOO_DB, uid, ODOO_PASSWORD or os.getenv("ODOO_PASSWORD"),
    "stock.picking.type", "read",
    args=[PICKING_TYPES],
    kwargs={"fields": ["id", "name"]},
    label="read_picking_types"
)

# Crear diccionario: id -> nombre
picking_type_names = {pt["id"]: pt["name"] for pt in picking_type_data}
log(f"✅ Tipos de operación: {picking_type_names}")

# ============================================================
# 2) PENDIENTES
# ============================================================
log("🔎 Buscando devoluciones PENDIENTES (assigned/waiting/confirmed) en lo.stock.picking...")

pending_ids = safe_execute_kw(
    models, ODOO_DB, uid, ODOO_PASSWORD or os.getenv("ODOO_PASSWORD"),
    MODEL_PICKING, "search",
    args=[[
        ["state", "in", PENDING_STATES],
        ["picking_type_id", "in", PICKING_TYPES],
    ]],
    kwargs={"limit": MAX_ROWS},
    label="search_pending"
)

log(f"📦 Pendientes encontrados: {len(pending_ids)}")

pending_pickings = []
if pending_ids:
    pending_pickings = safe_execute_kw(
        models, ODOO_DB, uid, ODOO_PASSWORD or os.getenv("ODOO_PASSWORD"),
        MODEL_PICKING, "read",
        args=[pending_ids],
        kwargs={"fields": ["id", "name", "scheduled_date", "picking_type_id", "partner_id", "origin", "external_id", "state", "date_done"]},
        label="read_pending"
    )

pending_count = len(pending_pickings)
log(f"✅ Pendientes leídos: {pending_count}")

if pending_count == 0 and SEND_IF_ZERO:
    log("ℹ️ No hay pendientes y SEND_IF_ZERO=True → saliendo.")
    raise SystemExit

# ============================================================
# 3) DONE AÑO ACTUAL (todos los meses)
# ============================================================
today = date.today()
current_year = today.year
current_month = today.month

# Desde el 1 de enero del año actual
year_start = date(current_year, 1, 1).strftime("%Y-%m-%d 00:00:00")
# Hasta hoy
today_end = datetime.now().strftime("%Y-%m-%d 23:59:59")

log(f"🔎 Buscando devoluciones DONE del año {current_year} (desde {year_start})...")

done_year_ids = safe_execute_kw(
    models, ODOO_DB, uid, ODOO_PASSWORD or os.getenv("ODOO_PASSWORD"),
    MODEL_PICKING, "search",
    args=[[
        ["state", "=", "done"],
        ["picking_type_id", "in", PICKING_TYPES],
        ["date_done", ">=", year_start],
        ["date_done", "<=", today_end],
    ]],
    kwargs={"limit": MAX_ROWS},
    label="search_done_year"
)

log(f"✅ DONE año {current_year}: {len(done_year_ids)}")

done_year_pickings = safe_execute_kw(
    models, ODOO_DB, uid, ODOO_PASSWORD or os.getenv("ODOO_PASSWORD"),
    MODEL_PICKING, "read",
    args=[done_year_ids],
    kwargs={"fields": ["id", "name", "date_done"]},
    label="read_done_year"
)

# ============================================================
# 4) AGRUPAR DONE POR MES Y SEMANA
# ============================================================
monthly_returns = defaultdict(int)  # {mes_num: count}
weekly_returns = defaultdict(int)   # {semana_iso: count} solo del mes actual

for p in done_year_pickings:
    date_done = p.get("date_done")
    if date_done:
        date_obj = datetime.strptime(date_done, "%Y-%m-%d %H:%M:%S").date()
        month_num = date_obj.month
        monthly_returns[month_num] += 1
        
        # Si es del mes actual, también contar por semana
        if month_num == current_month:
            week_iso = get_week_iso(date_obj)
            if week_iso != -1:
                weekly_returns[week_iso] += 1

# ============================================================
# 5) AGRUPAR PENDIENTES POR TIPO Y MES-AÑO (scheduled_date)
# ============================================================
pending_by_type_month = defaultdict(lambda: defaultdict(int))  # {tipo: {(año, mes): count}}
pending_by_type_total = defaultdict(int)  # Para contar totales por tipo

for p in pending_pickings:
    scheduled = p.get("scheduled_date")
    pt = p.get("picking_type_id")
    
    # Obtener ID del tipo de operación
    tipo_id = pt[0] if isinstance(pt, list) and len(pt) > 0 else None
    
    # Obtener nombre correcto del diccionario
    tipo_nombre = picking_type_names.get(tipo_id, "Sin tipo")
    
    # Contar total por tipo (sin importar si tiene scheduled_date)
    pending_by_type_total[tipo_nombre] += 1
    
    if scheduled:
        try:
            # scheduled_date puede venir como "YYYY-MM-DD" o "YYYY-MM-DD HH:MM:SS"
            if len(scheduled) > 10:
                date_obj = datetime.strptime(scheduled, "%Y-%m-%d %H:%M:%S").date()
            else:
                date_obj = datetime.strptime(scheduled, "%Y-%m-%d").date()
            
            year_num = date_obj.year
            month_num = date_obj.month
            
            # Agrupar por tipo y (año, mes)
            pending_by_type_month[tipo_nombre][(year_num, month_num)] += 1
        except Exception as e:
            log(f"⚠️ Error parseando scheduled_date: {scheduled} - {e}")
    else:
        log(f"⚠️ Pendiente sin scheduled_date: {p.get('name')} - Tipo: {tipo_nombre}")

# Log de totales por tipo
log(f"📊 Pendientes por tipo:")
for tipo, count in sorted(pending_by_type_total.items()):
    log(f"   • {tipo}: {count} pendientes")
    if tipo in pending_by_type_month:
        log(f"     └─ Con fecha válida: {sum(pending_by_type_month[tipo].values())}")
    else:
        log(f"     └─ Con fecha válida: 0 (NO APARECERÁ EN LA TABLA)")

# ============================================================
# 6) OBTENER IDS CORRECTOS DE STOCK.PICKING
# ============================================================
log("🔎 Buscando IDs correspondientes en stock.picking...")

# Extraer todos los nombres de albaranes
picking_names = [p.get("name") for p in pending_pickings if p.get("name")]

# Buscar en stock.picking usando los nombres
stock_picking_ids = safe_execute_kw(
    models, ODOO_DB, uid, ODOO_PASSWORD or os.getenv("ODOO_PASSWORD"),
    "stock.picking", "search",
    args=[[["name", "in", picking_names]]],
    kwargs={"limit": MAX_ROWS},
    label="search_stock_picking"
)

# Leer los registros de stock.picking
stock_pickings_data = []
if stock_picking_ids:
    stock_pickings_data = safe_execute_kw(
        models, ODOO_DB, uid, ODOO_PASSWORD or os.getenv("ODOO_PASSWORD"),
        "stock.picking", "read",
        args=[stock_picking_ids],
        kwargs={"fields": ["id", "name"]},
        label="read_stock_picking"
    )

# Crear diccionario: nombre -> id de stock.picking
name_to_stock_id = {sp["name"]: sp["id"] for sp in stock_pickings_data}

log(f"✅ Mapeados {len(name_to_stock_id)} albaranes a stock.picking IDs")

# ============================================================
# 7) EXCEL CON HIPERVÍNCULO CORREGIDO
# ============================================================
excel_rows = []

for p in pending_pickings:
    partner = p.get("partner_id")
    cliente = partner[1] if isinstance(partner, list) and len(partner) > 1 else "—"

    pt = p.get("picking_type_id")
    tipo = pt[1] if isinstance(pt, list) and len(pt) > 1 else "—"

    origen = p.get("origin") or "—"
    external_id = p.get("external_id") or "—"
    scheduled = p.get("scheduled_date") or "—"
    
    albaran_name = p.get("name", "")

    # Obtener el ID correcto de stock.picking usando el nombre
    stock_id = name_to_stock_id.get(albaran_name, p['id'])  # fallback al id original si no se encuentra
    
    # URL completa con todos los parámetros (los valores de menu_id, action, active_id son fijos para devoluciones)
    link = f"{ODOO_URL}/web#id={stock_id}&cids=1&menu_id=238&action=393&active_id=6&model=stock.picking&view_type=form"

    excel_rows.append({
        "Albaran": albaran_name,
        "Fecha prevista": scheduled,
        "Tipo": tipo,
        "Cliente": cliente,
        "Pedido origen": origen,
        "ID externo": external_id,
        "Estado": p.get("state", ""),
        "URL": link,
    })

ts = datetime.now().strftime("%Y%m%d_%H%M")
excel_name = f"Informe_Devoluciones_Pendientes_{ts}.xlsx"
excel_path = os.path.join(OUT_DIR, excel_name)

df = pd.DataFrame(excel_rows)
df.to_excel(excel_path, index=False)
log(f"📊 Excel generado: {excel_path}")

# ============================================================
# 8) MENSAJE SLACK MEJORADO
# ============================================================
now_local = datetime.now()

# A) DONE - Resumen de meses anteriores + semanas del mes actual
done_summary = []

# Meses anteriores del año (1 hasta mes actual - 1)
for m in range(1, current_month):
    if m in monthly_returns:
        month_name = get_month_name(m)
        done_summary.append(f"• {month_name}: {monthly_returns[m]} devoluciones")

# Agregar separación antes del mes actual
if done_summary:
    done_summary.append("")  # línea en blanco

# Mes actual con desglose semanal
current_month_total = monthly_returns.get(current_month, 0)
current_month_name = get_month_name(current_month)
done_summary.append(f"*{current_month_name} (mes actual):* {current_month_total}")

weekly_lines = "\n".join([
    f"  • Semana {week}: {count} devoluciones" 
    for week, count in sorted(weekly_returns.items())
]) or "  —"

done_summary.append(weekly_lines)

done_text = "\n".join(done_summary)

# B) PENDIENTES - Tabla multi-fila por tipo de operación
if pending_by_type_month:
    # Obtener todos los meses únicos (ordenados)
    all_months = set()
    for tipo_data in pending_by_type_month.values():
        all_months.update(tipo_data.keys())
    all_months = sorted(all_months)
    
    # Crear headers (meses)
    headers = " │ ".join([
        f"{get_month_name(month)[:3]}-{year}".center(9) 
        for year, month in all_months
    ])
    
    # Crear filas por tipo
    tipo_rows = []
    for tipo_nombre in sorted(pending_by_type_month.keys()):
        tipo_data = pending_by_type_month[tipo_nombre]
        
        # Limitar nombre del tipo a 20 caracteres para que no desborde
        tipo_label = tipo_nombre[:20].ljust(20)
        
        # Valores para cada mes
        values = " │ ".join([
            f"{tipo_data.get((year, month), 0)}".center(9)
            for year, month in all_months
        ])
        
        tipo_rows.append(f"{tipo_label} │ {values}")
    
    separator = "─" * (22 + 11 * len(all_months) - 1)
    header_line = " " * 22 + headers
    
    pending_table = f"```\n{header_line}\n{separator}\n" + "\n".join(tipo_rows) + "\n```"
else:
    pending_table = "_No hay pendientes distribuidos por mes_"

# Mensaje completo
slack_text = (
    f"*📦 Informe de devoluciones*\n"
    f"Generado: `{now_local.strftime('%Y-%m-%d %H:%M')}`\n\n"
    f"*A) DONE (año {current_year}):*\n"
    f"{done_text}\n\n"
    f"*B) Pendientes totales:* {pending_count}\n"
    f"*Distribución por mes:*\n"
    f"{pending_table}\n"
)

# ============================================================
# 9) ENVIAR A SLACK
# ============================================================
log(f"📨 Enviando informe a Slack channel={SLACK_CHANNEL_ID} ...")
send_to_slack_with_excel(
    channel_id=SLACK_CHANNEL_ID,
    text=slack_text,
    excel_path=excel_path,
    title=excel_name,
    in_thread=True  # pon False si lo quieres como mensaje suelto (sin hilo)
)
log("✅ Excel enviado a Slack")

# Borrar el archivo Excel después de enviarlo
os.remove(excel_path)
log(f"🗑️ Excel eliminado: {excel_path}")

log("🏁 Fin del script.")
