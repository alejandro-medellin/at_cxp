# Notas_Credito.py
import streamlit as st
import pandas as pd
import requests
import time
import re
import json
import csv  # <— IMPORTAR CSV
from io import BytesIO
from datetime import datetime

# ————— Configuración —————
BASE_ID  = "appxk6WzoRfH7WKhG"
TABLE_ID = "tblApAOIHPsHQdwMb"
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
            return [f["name"] for f in tbl.get("fields", [])]
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
def procesar_dataframe_notascredito(records, token):
    campos = fetch_table_schema(token)
    rows = []
    for rec in records:
        flds = rec.get("fields", {}).copy()
        for c in campos:
            flds.setdefault(c, None)
        flds["Record ID"] = rec["id"]
        rows.append(flds)
    df = pd.DataFrame(rows)
    st.write("**Columnas antes de procesar:**", df.columns.tolist())

    # Normalizar nombres de columna
    def normalizar(col):
        col = col.strip().replace(" ", "_").replace("(", "").replace(")", "")
        return re.sub(r"[^\w_]", "", col).lower()
    df.columns = [normalizar(c) for c in df.columns]

    # Eliminar columnas innecesarias
    drop_cols = [
        'evento', 'quien_radica', 'documento_contable', 'empresa',
        'contabilizacion_notas_credito', 'proveedor_oc', 'requisicion',
        'solicitud_cuadro_de_recurso', 'quien_radica_bigquery',
        'causadores', 'causado_por', 'activacion', 'entradas_almacen',
        'asignado_a', 'consecutivo_radicacion', 'anexos_contrato_marco'
    ]
    df.drop(columns=[c for c in drop_cols if c in df.columns], inplace=True)

    # Renombrar columnas para BigQuery
    df.rename(columns={
        'oc': 'id_oc',
        'numero': 'numero_nota_credito',
        'oc_bigquery': 'numero_oc',
        'empresa_bigquery': 'empresa',
        'quien_registra_bigquery': 'quien_causa',
        'documento_contable_bigquery': 'documento_contable',
        'fecha_registro': 'fecha_causacion'
    }, inplace=True)

    # Limpiar linked records y campos JSON
    def clean_linked(s):
        return (s.astype(str)
                 .str.replace("[", "", regex=False)
                 .str.replace("]", "", regex=False)
                 .str.replace("'", "", regex=False))
    def clean_json(s):
        return s.astype(str).str.replace("'", '"', regex=False)

    linked_cols = [
        'id_oc', 'fecha_causacion', 'tipo_oc', 'subtotal_oc',
        'plazo_de_pago', 'concepto_oc', 'fecha_creacion_oc'
    ]
    json_cols = [
        'archivo', 'creado_por', 'cotizacion_final',
        'oc_creada_por', 'archivo_oc', 'usuario_que_sube_causacion'
    ]
    exist = [c for c in linked_cols if c in df.columns]
    if exist:
        df[exist] = df[exist].apply(clean_linked)
    exist = [c for c in json_cols if c in df.columns]
    if exist:
        df[exist] = df[exist].apply(clean_json)

    # Reemplazar nan en plazo_de_pago
    if 'plazo_de_pago' in df.columns:
        df['plazo_de_pago'] = df['plazo_de_pago'].astype(str).str.replace('nan', '0', regex=False)

    # Convertir fechas y quitar timezone
    date_cols = [
        'fecha_radicacion', 'fecha_causacion', 'fecha_creacion_oc',
        'fecha_nota_credito', 'fecha_creacion_registro',
        'ultima_modificacion_registro'
    ]
    for c in date_cols:
        if c in df.columns:
            df[c] = pd.to_datetime(df[c], errors='coerce').dt.tz_localize(None)

    # Ajustar tipos de datos
    type_map = {
        'consecutivo': 'Int64',
        'id_oc': 'string',
        'numero_nota_credito': 'string',
        'archivo': 'string',
        'valor_nota_credito': 'Float64',
        'creado_por': 'string',
        'tiene_oc': 'string',
        'misma_oc_nueva_factura': 'string',
        'numero_oc': 'string',
        'proveedor': 'string',
        'tipo_oc': 'string',
        'cotizacion_final': 'string',
        'oc_creada_por': 'string',
        'anular_oc': 'string',
        'subtotal_oc': 'Float64',
        'moneda_oc': 'string',
        'plazo_de_pago': 'Int64',
        'archivo_oc': 'string',
        'solicitud_cuadro_recurso': 'string',
        'empresa': 'string',
        'quien_causa': 'string',
        'documento_contable': 'string',
        'contabilizado': 'bool',
        'mes_radicacion': 'string',
        'usuario_que_sube_causacion': 'string',
        'record_id': 'string',
        'concepto_oc': 'string',
        'proveedor_sin_oc': 'string',
        'empresa_sin_oc': 'string'
    }
    df = df.astype({k: v for k, v in type_map.items() if k in df.columns})

    # Crear columna fecha_ultima_actualizacion
    if {'ultima_modificacion_registro','fecha_creacion_registro'}.issubset(df.columns):
        df['fecha_ultima_actualizacion'] = (
            df['ultima_modificacion_registro']
              .combine_first(df['fecha_creacion_registro'])
              .dt.tz_localize(None)
        )

    # Extraer URLs y nombres desde JSON
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

    for fld, fn in [
        ('archivo', extract_url),
        ('creado_por', extract_name),
        ('archivo_oc', extract_url),
        ('usuario_que_sube_causacion', extract_name)
    ]:
        if fld in df.columns:
            df[fld] = df[fld].apply(fn)

    st.write("**Columnas después de procesar:**", df.columns.tolist())
    st.success("✅ Preprocesamiento completado.")

    return df

# ————— 4️⃣ Interfaz Streamlit —————
st.set_page_config(page_title="Notas Crédito")
st.title("📉 Descarga de Notas Crédito desde Airtable")

with st.form("form_nc"):
    airtoken = st.text_input("🔑 Token Airtable", type="password")
    c1, c2 = st.columns(2)
    with c1:
        inicio = st.date_input("📅 Fecha inicial", value=pd.to_datetime("2024-01-01").date())
    with c2:
        fin = st.date_input("📅 Fecha final", value=datetime.today().date())
    go = st.form_submit_button("Ejecutar")

if go:
    ini_str = inicio.strftime('%Y-%m-%d')
    fin_str = fin.strftime('%Y-%m-%d')

    recs = fetch_all_records(AIRTABLE_URL, airtoken, ini_str, fin_str)
    if not recs:
        st.warning("⚠️ No se encontraron registros.")
    else:
        st.success(f"✅ {len(recs)} registros descargados.")
        st.write("🧹 Procesando datos…")
        df_proc = procesar_dataframe_notascredito(recs, airtoken)

        csv_buf = BytesIO()
        df_proc.to_csv(csv_buf, index=False, quoting=csv.QUOTE_NONNUMERIC)
        st.download_button("📥 Descargar CSV procesado", csv_buf.getvalue(), "notas_credito.csv", "text/csv")

        xlsx_buf = BytesIO()
        df_proc.to_excel(xlsx_buf, index=False)
        st.download_button(
            "📥 Descargar Excel procesado",
            xlsx_buf.getvalue(),
            "notas_credito.xlsx",
            "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        )
