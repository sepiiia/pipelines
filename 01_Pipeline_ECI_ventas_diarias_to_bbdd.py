#!/usr/bin/env python
# coding: utf-8

# In[24]:


import requests
import base64
import zipfile
import io
import os
import re
import pandas as pd
from datetime import datetime, timezone, timedelta

# ---- CONFIGURACIÓN ----
USER     = os.environ.get("EDIWIN_USER")
PASSWORD = os.environ.get("EDIWIN_PASSWORD")
DOMAIN   = os.environ.get("EDIWIN_DOMAIN")
GROUP    = os.environ.get("EDIWIN_GROUP")
SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY")

TIPO_DOCUMENTO = "SLSRPT"




# ---- FIN CONFIGURACIÓN ----

BASE_URL = "https://ediwin.edicomgroup.com"

# ── FECHAS ─────────────────────────────────────────────────
hoy   = datetime.now(timezone.utc)
ayer  = hoy - timedelta(days=1)

# Descarga el fichero de HOY (contiene venta de AYER)
FECHA_DESDE = hoy.strftime("%Y-%m-%dT00:00:00.000Z")
FECHA_HASTA = hoy.strftime("%Y-%m-%dT23:59:59.999Z")
fecha_ayer  = ayer.strftime("%Y-%m-%d")

# ── PASO 1: LOGIN ──────────────────────────────────────────
s = requests.Session()
r_login = s.post(
    f"{BASE_URL}/connect/registerSession",
    json={
        "user": USER,
        "password": PASSWORD,
        "domain": DOMAIN,
        "group": GROUP,
        "audit": "{\"remoteUserAgent\":\"Python/requests\",\"ediwinUser\":\"231219\"}"
    },
    headers={"Content-Type": "application/json"}
)
tokena = r_login.json().get("tokena")
if not tokena:
    raise Exception(f"Login fallido: {r_login.text}")
print(f"✅ Token obtenido: {tokena[:30]}...")

# ── PASO 2: DESCARGA EDIWIN ────────────────────────────────
ediwin_headers = {
    "tokena": tokena,
    "Content-Type": "application/json",
    "Accept": "application/json, text/plain, */*",
    "Origin": BASE_URL,
    "Referer": f"{BASE_URL}/"
}
body = {
    "filter": {
        "from": FECHA_DESDE,
        "to": FECHA_HASTA,
        "type": "LAST_YEAR",
        "filterCriteria": {"children": [], "criteria": None, "union": None}
    }
}
url = f"{BASE_URL}/api/documents/exportDocument?filename=&dateinname=false&control=false&isolatedfiles=false&volumeId=0&asynchronous=false"

r = s.post(url, headers=ediwin_headers, json=body)
data = r.json()

if data.get("result") != 1:
    raise Exception(f"Error descarga: {data}")

zip_bytes = base64.b64decode(data["outputData"]["file.zip"])
edifact_text = ""
with zipfile.ZipFile(io.BytesIO(zip_bytes)) as z:
    for nombre in z.namelist():
        if TIPO_DOCUMENTO in nombre:
            edifact_text = z.read(nombre).decode("utf-8", errors="replace")
            print(f"✅ Fichero EDI leído: {nombre}")

if not edifact_text:
    raise Exception("No se encontró fichero SLSRPT en el zip")

# ── PASO 3: PARSEO EDIFACT ─────────────────────────────────
segments = edifact_text.split("'")

rows = []
sucursal      = None
periodo_venta = None

i = 0
while i < len(segments):
    seg = segments[i]

    if seg.startswith("LOC+162"):
        match = re.search(r'(?<=\+162\+)\d+', seg)
        if match:
            sucursal = match.group()

    elif seg.startswith("DTM"):
        match = re.search(r'(?<=:)\d{8}', seg)
        if match:
            periodo_venta = datetime.strptime(match.group(), "%Y%m%d").strftime("%Y-%m-%d")

    elif seg.startswith("LIN"):
        ean_match = re.search(r'(?<=\+\+)\d+', seg)
        if not ean_match:
            i += 1
            continue
        ean = ean_match.group()
        cantidad_vendida  = 0
        cantidad_devuelta = 0

        j = i + 1
        while j < len(segments):
            qty_seg = segments[j]
            if qty_seg.startswith("QTY"):
                qty_type  = re.search(r'(?<=\+)[0-9A-Z]+', qty_seg)
                qty_value = re.search(r'(?<=:)\d+', qty_seg)
                if qty_type and qty_value:
                    if qty_type.group() == "153":
                        cantidad_vendida = int(qty_value.group())
                    elif qty_type.group() == "77E":
                        cantidad_devuelta = int(qty_value.group())
            elif qty_seg.startswith("LIN") or qty_seg.startswith("LOC+162"):
                break
            j += 1

        if sucursal and periodo_venta and ean:
            rows.append({
                "SUCURSAL":          int(sucursal),
                "PERIODO_VENTA":     periodo_venta,
                "EAN":               int(ean),
                "Cantidad_Vendida":  cantidad_vendida,
                "Cantidad_Devuelta": cantidad_devuelta,
                "Total":             cantidad_vendida - cantidad_devuelta
            })
        i = j - 1

    i += 1

df = pd.DataFrame(rows)
print(f"✅ Registros parseados: {len(df)}")
print(df.head())

# ── PASO 4: CONTROL DUPLICADOS + INSERTAR EN SUPABASE ──────
sb_headers = {
    "apikey": SUPABASE_KEY,
    "Authorization": f"Bearer {SUPABASE_KEY}",
    "Content-Type": "application/json",
    "Prefer": "return=minimal"
}

# Comprueba si ya existe la venta de AYER en Supabase
r_check = requests.get(
    f"{SUPABASE_URL}/rest/v1/FACT_SALES_ECI",
    headers={**sb_headers, "Range": "0-0"},
    params={"PERIODO_VENTA": f"eq.{fecha_ayer}", "select": "id"}
)

if r_check.json():
    print(f"⚠️ Ya existen registros para {fecha_ayer}, abortando para evitar duplicados.")
else:
    registros = df.to_dict(orient="records")
    for i in range(0, len(registros), 1000):
        lote = registros[i:i+1000]
        r = requests.post(
            f"{SUPABASE_URL}/rest/v1/FACT_SALES_ECI",
            headers=sb_headers,
            json=lote
        )
        if r.status_code in [200, 201]:
            print(f"✅ Lote {i}-{i+len(lote)} insertado")
        else:
            print(f"❌ Error en lote {i}: {r.text}")
    print(f"✅ Carga completada: {len(registros)} registros para {fecha_ayer}")


# In[ ]:




