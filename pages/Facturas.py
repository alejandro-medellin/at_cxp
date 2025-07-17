# Facturas.py
import streamlit as st
import pandas as pd
import requests
import time
import re
import json
import csv
from io import BytesIO
from datetime import datetime

# ————— Configuración —————
BASE_ID  = "appxk6WzoRfH7WKhG"
TABLE_ID = "tblGlvz8czJbwGHAn"
AIRTABLE_URL = f"https://api.airtable.com/v0/{BASE_ID}/{TABLE_ID}"

# ————— 1️⃣ Metadata: obtener esquema completo —————
def fetch_table_schema(token):
    url = f"https://api.airtable.com/v0/meta/bases/{BASE_ID}/tables"
    headers = {
        "Authorization": f"Bearer {token}",
        "X-API-Version": "0.1.0",
        "Content-Type": "application/json"
    }
    resp = requests.get(url, headers=headers)
    resp.raise_for_status()
    tables = resp.json().get("tables", [])
    for tbl in tables:
        if tbl.get("id") == TABLE_ID:
            return [field["name"] for field in tbl.get("fields", [])]
    raise RuntimeError(f"Tabla {TABLE_ID} no encontrada en metadata.")

# ————— 2️⃣ Descarga paginada con TODOS los campos + filtro de fecha —————
def fetch_all_records(url, token, fecha_ini, fecha_fin):
    campos = fetch_table_schema(token)

    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }
    all_records = []
    offset = None

    formula = (
        f"AND("
        f"IS_AFTER({{fecha_creacion_registro}}, '{fecha_ini}'),"
        f"IS_BEFORE({{fecha_creacion_registro}}, '{fecha_fin}')"
        f")"
    )

    status = st.empty()
    status.text(f"🔄 Descargando registros de {fecha_ini} → {fecha_fin}…")

    params = [("fields[]", fld) for fld in campos]
    params.append(("filterByFormula", formula))

    page = 1
    while True:
        if offset:
            params = [p for p in params if p[0] != "offset"] + [("offset", offset)]
        resp = requests.get(url, headers=headers, params=params)
        resp.raise_for_status()
        data = resp.json()
        recs = data.get("records", [])
        all_records.extend(recs)

        status.text(f"📦 Página {page} — acumulado: {len(all_records)} registros")
        page += 1

        offset = data.get("offset")
        if not offset:
            break
        time.sleep(0.2)

    status.text(f"✅ {len(all_records)} registros descargados.")
    return all_records

# ————— 3️⃣ Preprocesamiento: relleno y limpieza —————
def procesar_dataframe_facturas(records, token):
    campos = fetch_table_schema(token)
    rows = []
    for rec in records:
        flds = rec.get("fields", {}).copy()
        for c in campos:
            flds.setdefault(c, None)
        flds["Record ID"] = rec["id"]
        rows.append(flds)
    df = pd.DataFrame(rows)

    def norm(c):
        c = c.strip().replace(" ", "_").replace("(", "").replace(")", "")
        return re.sub(r"[^\w_]", "", c).lower()
    df.columns = [norm(c) for c in df.columns]

    drop_cols = [
        'empresa_oc','solicitante_requisicion','quien_radica','proveedor',
        'evento_oc','evento_requisicion','check_evento','ultima_asignacion_causador',
        'asignado_a','sub_pep','oc_texto','empresa','activacion_oc','id_factura_xml',
        'hora_factura_xml','tipo_factura_xml','moneda_factura_xml','numero_items_xml',
        'precio_xml','cantidad_xml', 'codigo_autorizacion_factura_xml',
        'direccion_cliente_xml', 'direccion_proveedor_xml', 'razon_en_tramite'
    ]
    df.drop(columns=[c for c in drop_cols if c in df.columns], inplace=True)

    df.rename(columns={
        'oc':'id_oc','requisicion':'id_requisicion','empresa_final':'empresa',
        'ceco':'centro_de_costo','asignado_a_bigquery':'causador_asignado',
        'requisicion_bigquery':'numero_requisicion','oc_bigquery':'numero_oc',
        'evento_requisicion_bigquery':'evento_de_la_solicitud',
        'evento_oc_bigquery':'evento_de_la_oc','quien_radica_bigquery':'quien_radica'
    }, inplace=True)

    def clean_linked(s):
        return (s.astype(str)
                 .str.replace("[","",regex=False)
                 .str.replace("]","",regex=False)
                 .str.replace("'","",regex=False))
    def clean_json(s):
        return s.astype(str).str.replace("'",'"',regex=False)
    def clean_xml(s):
        return s.astype(str).str.replace(">","",regex=False)

    linked = ['id_oc','correo_solicitante','id_requisicion','concepto_oc','subtotal_oc',
              'razon_social_proveedor','contabilizacion_con_oc','oc_anulada',
              'tipo_oc','codigo_ceco','fecha_causacion_sin_oc','fecha_causacion',
              'plazo_de_pago_oc','quien_causa_factura','status_solicitud',
              'quien_causa_sin_oc','fecha_causacion_con_oc']
    js     = ['archivo_factura','quien_envia_form','archivo_factura_xml','archivo_oc','planilla_seguridad_social']
    xm     = ['proveedor_factura_xml','tipo_impuesto_xml','correo_proveedor_xml',
              'telefono_proveedor_xml','cliente_xml','telefono_cliente_xml',
              'descripcion_factura_xml','qr_xml','correo_contacto_xml']

    for cols, fn in [(linked, clean_linked), (js, clean_json), (xm, clean_xml)]:
        exists = [c for c in cols if c in df.columns]
        if exists:
            df[exists] = df[exists].apply(fn)

    if 'plazo_de_pago_oc' in df.columns:
        df['plazo_de_pago_oc'] = df['plazo_de_pago_oc'].astype(str).str.replace('nan','0',regex=False)

    for c in ['fecha_creacion_registro','ultima_modificacion_registro','fecha_radicacion',
              'fecha_causacion_con_oc','fecha_causacion','fecha_factura',
              'fecha_factura_xml','fecha_vencimiento_xml','fecha_causacion_sin_oc']:
        if c in df.columns:
            df[c] = pd.to_datetime(df[c], errors='coerce').dt.tz_localize(None)

    if {'ultima_modificacion_registro','fecha_creacion_registro'}.issubset(df.columns):
        df['fecha_ultima_actualizacion'] = (
            df['ultima_modificacion_registro']
              .combine_first(df['fecha_creacion_registro'])
              .dt.tz_localize(None)
        )

    def extract_url(js):
        try:
            arr = json.loads(js)
            return arr[0].get('url') if isinstance(arr, list) and arr else None
        except:
            return None
    def extract_name(js):
        try:
            arr = json.loads(js)
            return arr[0].get('name') if isinstance(arr, list) and arr else None
        except:
            return None

    for fld, fn in [('archivo_factura', extract_url),
                    ('quien_envia_form', extract_name),
                    ('archivo_factura_xml', extract_url),
                    ('archivo_oc', extract_url)]:
        if fld in df.columns:
            df[fld] = df[fld].apply(fn)

    return df

# ————— 4️⃣ Interfaz Streamlit —————
st.set_page_config(page_title="Facturas")
st.title("🧾 Descarga de Facturas desde Airtable")

with st.form("form_facturas"):
    airtoken = st.text_input("🔑 Token Airtable", type="password")
    col1, col2 = st.columns(2)
    with col1:
        inicio = st.date_input("📅 Fecha inicial", value=pd.to_datetime("2024-01-01").date())
    with col2:
        fin    = st.date_input("📅 Fecha final",   value=datetime.today().date())
    go = st.form_submit_button("Ejecutar")

if go:
    ini_str = inicio.strftime("%Y-%m-%d")
    fin_str = fin.strftime("%Y-%m-%d")

    recs = fetch_all_records(AIRTABLE_URL, airtoken, ini_str, fin_str)
    if not recs:
        st.warning("⚠️ No se encontraron registros.")
    else:
        st.success(f"✅ {len(recs)} registros descargados.")
        st.write("🧹 Procesando datos…")
        df_proc = procesar_dataframe_facturas(recs, airtoken)

        csv_buf = BytesIO()
        df_proc.to_csv(csv_buf, index=False, quoting=csv.QUOTE_NONNUMERIC)
        st.download_button("📥 Descargar CSV procesado", csv_buf.getvalue(), "facturas.csv", "text/csv")

        xlsx_buf = BytesIO()
        df_proc.to_excel(xlsx_buf, index=False)
        st.download_button(
            "📥 Descargar Excel procesado",
            xlsx_buf.getvalue(),
            "facturas.xlsx",
            "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        )