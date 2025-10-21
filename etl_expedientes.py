# 5_etl_expedientes.py
import csv
import re
import os
from datetime import datetime
import pandas as pd

# =========================
# Diccionarios de normalización
# =========================

CAMARAS = {
    "CFP": "Cámara Nacional de Apelaciones en lo Criminal y Correccional Federal",
    "CCC": "Cámara Nacional de Apelaciones en lo Criminal y Correccional",
    "CAF": "Cámara Nacional de Apelaciones en lo Contencioso Administrativo Federal",
    "CPF": "Cámara Federal de Casación Penal",
    "FRO": "Cámara Federal de Apelaciones de Rosario",
    "CCF": "Cámara Nacional de Apelaciones en lo Civil y Comercial Federal",
    "CIV": "Cámara Nacional de Apelaciones en lo Civil",
    "FGR": "Cámara Federal de Apelaciones de General Roca",
    "FPO": "Cámara Federal de Apelaciones de Posadas",
    "FTU": "Cámara Federal de Apelaciones de Tucumán",
    "FCB": "Cámara Federal de Apelaciones de Córdoba",
    "FPA": "Cámara Federal de Apelaciones de Paraná",
    "FSA": "Cámara Federal de Apelaciones de Salta",
    "FBB": "Cámara Federal de Apelaciones de Bahía Blanca",
    "FCT": "Cámara Federal de Apelaciones de Corrientes",
    "FMZ": "Cámara Federal de Apelaciones de Mendoza",
    "FCR": "Cámara Federal de Apelaciones de Comodoro Rivadavia",
    "FSM": "Cámara Federal de Apelaciones de San Martín",
    "FLP": "Cámara Federal de Apelaciones de La Plata",
    "FMP": "Cámara Federal de Apelaciones de Mar del Plata",
    "FRE": "Cámara Federal de Apelaciones de Resistencia",
    "CSS": "Cámara Federal de la Seguridad Social",
    "CPN": "Cámara Nacional de Casación Penal",
    "CPE": "Cámara Nacional en lo Penal Económico",
    "COM": "Cámara Nacional de Apelaciones en lo Comercial",
    "CNE": "Cámara Nacional Electoral",
    "CNT": "Cámara Nacional de Apelaciones del Trabajo",
}

FUERO_POR_CAMARA = {
    "CFP": "Penal Federal", "CCC": "Penal Federal", "CPF": "Penal Federal",
    "FRO": "Penal Federal", "FGR": "Penal Federal", "FPO": "Penal Federal",
    "FTU": "Penal Federal", "FCB": "Penal Federal", "FPA": "Penal Federal",
    "FSA": "Penal Federal", "FBB": "Penal Federal", "FCT": "Penal Federal",
    "FMZ": "Penal Federal", "FCR": "Penal Federal", "FSM": "Penal Federal",
    "FLP": "Penal Federal", "FMP": "Penal Federal", "FRE": "Penal Federal",
    "CSS": "Penal Federal", "CPN": "Penal Federal", "CPE": "Penal Federal",
    "CCF": "Civil", "CIV": "Civil", "COM": "Comercial", "CNT": "Laboral",
    "CAF": "Contencioso Administrativo", "CNE": "Electoral"
}

# =========================
# Utilidades
# =========================

def safe_read_csv_pd(path):
    if not os.path.exists(path):
        print(f"Advertencia: no se encontró el archivo {path}. Se omite.")
        return pd.DataFrame()
    return pd.read_csv(path)

def safe_open(path, mode, **kwargs):
    if not os.path.exists(path) and "r" in mode:
        print(f"Advertencia: no se encontró el archivo {path}. Se omite.")
        return None
    return open(path, mode, **kwargs)

def parse_date(fecha_str):
    """Convierte fecha DD/MM/YYYY a YYYY-MM-DD, o devuelve '' si falla."""
    if not fecha_str:
        return ""
    try:
        return datetime.strptime(fecha_str.strip(), "%d/%m/%Y").strftime("%Y-%m-%d")
    except Exception:
        return ""

def limpiar_texto(texto):
    if texto is None:
        return None
    s = " ".join(str(texto).split())
    return s if s != "" else None

# === Funciones de inferencia / parsing del ETL anterior ===

def inferir_fuero_por_camara(numero_expediente):
    if not numero_expediente:
        return "Desconocido"
    sigla = str(numero_expediente).split()[0].upper()
    return FUERO_POR_CAMARA.get(sigla, "Desconocido")

def inferir_jurisdiccion_por_radicacion(radicacion):
    if not radicacion:
        return "Nacional"
    if "FEDERAL" in radicacion.upper():
        return "Federal"
    return "Nacional"

def extraer_camara_y_ano(numero_expediente):
    if not numero_expediente:
        return "Desconocida", None
    m = re.match(r"(\w+)\s+\d+/(\d{4})", str(numero_expediente))
    if m:
        sigla, anio = m.groups()
        camara = CAMARAS.get(sigla.upper(), "Desconocida")
        return camara, int(anio)
    return "Desconocida", None

def desarmar_radicacion(radicacion):
    """Formato esperado: 'DD/MM/YYYY | Tribunal XYZ | Fiscal: NN | Fiscalía ...' (tolerante)."""
    if not radicacion:
        return "", "", "", ""
    partes = [p.strip() for p in str(radicacion).split("|")]
    fecha = partes[0] if len(partes) > 0 else ""
    tribunal = partes[1] if len(partes) > 1 else ""
    fiscal, fiscalia = "", ""
    for p in partes[2:]:
        up = p.upper()
        if up.startswith("FISCAL:"):
            fiscal = p.split(":", 1)[-1].strip()
        elif up.startswith("FISCALIA") or up.startswith("FISCALÍA"):
            fiscalia = p.split(":", 1)[-1].strip() if ":" in p else p.strip()
    return fecha, tribunal, fiscal, fiscalia

# =========================
# 1) EXPEDIENTES
# =========================

def procesar_expedientes():
    print("Procesando expedientes...")
    # Entradas
    f_tramite = safe_open("5_expedientes.csv", "r", newline="", encoding="utf-8")
    f_term = safe_open("scraper_completas_terminadas_expedientes.csv", "r", newline="", encoding="utf-8")

    rows = []
    count_tramite = count_term = 0

    def procesar_reader(reader, estado_id):
        nonlocal rows, count_tramite, count_term
        for row in reader:
            numero = limpiar_texto(row.get("Expediente") or row.get("numero_expediente"))
            caratula = limpiar_texto(row.get("Carátula") or row.get("caratula"))
            ultima_act = limpiar_texto(row.get("Última actualización") or row.get("fecha_ultimo_mov") or row.get("fecha_ultimo_movimiento"))
            radicacion = row.get("Radicación del expediente") or row.get("radicacion") or ""
            delitos = limpiar_texto(row.get("Delitos") or row.get("delitos"))

            fecha_inicio, tribunal, fiscal, fiscalia = desarmar_radicacion(radicacion)
            camara, ano_inicio = extraer_camara_y_ano(numero)
            fuero = inferir_fuero_por_camara(numero)
            jurisdiccion = inferir_jurisdiccion_por_radicacion(radicacion)

            rows.append({
                "numero_expediente": numero,
                "caratula": caratula,
                "jurisdiccion": jurisdiccion,
                "tribunal": limpiar_texto(tribunal),
                "estado_procesal_id": estado_id,
                "fecha_inicio": parse_date(fecha_inicio),
                "fecha_ultimo_movimiento": parse_date(ultima_act),
                "camara_origen": camara,
                "ano_inicio": ano_inicio,
                "delitos": delitos,
                "fiscal": limpiar_texto(fiscal),
                "fiscalia": limpiar_texto(fiscalia),
                "fuero": fuero  # útil para dimensiones y tribunales
            })
            if estado_id == 1:
                count_tramite += 1
            else:
                count_term += 1

    if f_tramite:
        with f_tramite as f:
            procesar_reader(csv.DictReader(f), estado_id=1)
    if f_term:
        with f_term as f:
            procesar_reader(csv.DictReader(f), estado_id=2)

    print(f"Expedientes en trámite procesados: {count_tramite}")
    print(f"Expedientes terminados procesados: {count_term}")
    print(f"Total expedientes combinados: {len(rows)}")

    # Salida
    fieldnames = [
        "numero_expediente", "caratula", "jurisdiccion", "tribunal",
        "estado_procesal_id", "fecha_inicio", "fecha_ultimo_movimiento",
        "camara_origen", "ano_inicio", "delitos", "fiscal", "fiscalia"
    ]
    with open("etl_expedientes.csv", "w", newline="", encoding="utf-8") as f_out:
        writer = csv.DictWriter(f_out, fieldnames=fieldnames)
        writer.writeheader()
        for r in rows:
            # el campo fuero no va a expediente, queda para etapas siguientes
            writer.writerow({k: r.get(k) for k in fieldnames})

    # Devolver DF utilitario (para otras dimensiones)
    df = pd.DataFrame(rows)
    return df

# =========================
# 2) PARTES / LETRADOS / REPRESENTACIONES
# =========================

def procesar_intervinientes():
    print("Procesando intervinientes (partes/letrados/representaciones)...")
    df_t = safe_read_csv_pd("5_intervinientes.csv")
    df_T = safe_read_csv_pd("scraper_completas_terminadas_intervinientes.csv")

    # Etiqueta de estado (no se carga en tablas, pero puede servir a análisis)
    if not df_t.empty:
        df_t["estado_procesal_id"] = 1
    if not df_T.empty:
        df_T["estado_procesal_id"] = 2

    # Normalización básica de columnas esperadas
    for df in (df_t, df_T):
        if df.empty:
            continue
        # Renombrar si fuese necesario
        rename_map = {
            "Expediente": "numero_expediente",
            "Nombre": "nombre",
            "Rol": "rol",
            "Letrado": "letrado",
        }
        for k, v in rename_map.items():
            if k in df.columns and v not in df.columns:
                df.rename(columns={k: v}, inplace=True)
        # Limpiar textos
        for col in ["numero_expediente", "nombre", "rol", "letrado"]:
            if col in df.columns:
                df[col] = df[col].map(limpiar_texto)

    df_all = pd.concat([df_t, df_T], ignore_index=True)

    # PARTES
    partes_cols = ["numero_expediente", "nombre", "rol"]
    df_partes = pd.DataFrame(columns=partes_cols)
    if not df_all.empty:
        tmp = df_all.dropna(subset=["numero_expediente", "nombre"])
        tmp["rol"] = tmp["rol"].fillna("").str.lower()
        df_partes = tmp[partes_cols].drop_duplicates()
    df_partes.to_csv("etl_partes.csv", index=False)
    print(f"Partes procesadas: {len(df_partes)}")

    # LETRADOS (relación interviniente-letrado por expediente)
    letrados_cols = ["numero_expediente", "interviniente", "letrado"]
    df_letrados = pd.DataFrame(columns=letrados_cols)
    if not df_all.empty:
        tmp = df_all.dropna(subset=["numero_expediente", "nombre", "letrado"])
        df_letrados = tmp.rename(columns={"nombre": "interviniente"})[letrados_cols].drop_duplicates()
    df_letrados.to_csv("etl_letrados.csv", index=False)
    print(f"Relaciones letrado-parte procesadas: {len(df_letrados)}")

    # REPRESENTACIONES (para tabla representacion: numero_expediente, parte, letrado, rol)
    repr_cols = ["numero_expediente", "nombre_parte", "letrado", "rol"]
    df_repr = pd.DataFrame(columns=repr_cols)
    if not df_all.empty:
        tmp = df_all.dropna(subset=["numero_expediente", "nombre", "letrado"])
        tmp["rol"] = tmp["rol"].fillna("").map(limpiar_texto)
        df_repr = tmp.rename(columns={"nombre": "nombre_parte"})[repr_cols].drop_duplicates()
    df_repr.to_csv("etl_representaciones.csv", index=False)
    print(f"Representaciones generadas: {len(df_repr)}")

    return df_partes, df_letrados, df_repr

# =========================
# 3) RESOLUCIONES
# =========================

def procesar_resoluciones():
    print("Procesando resoluciones...")
    f_t = safe_open("5_resoluciones.csv", "r", newline="", encoding="utf-8")
    f_T = safe_open("scraper_completas_terminadas_resoluciones.csv", "r", newline="", encoding="utf-8")

    fieldnames_res = ["numero_expediente", "fecha", "nombre", "link"]
    out_rows = []
    count_t = count_T = 0
    seen = set()

    def process(reader):
        nonlocal out_rows, seen, count_t, count_T
        for row in reader:
            expediente = limpiar_texto(row.get("Expediente") or row.get("numero_expediente"))
            fecha = parse_date(row.get("Fecha") or row.get("fecha"))
            nombre = limpiar_texto(row.get("Nombre") or row.get("nombre"))
            link = limpiar_texto(row.get("Link") or row.get("link"))
            key = (expediente, fecha, nombre, link)
            if expediente and key not in seen:
                out_rows.append({
                    "numero_expediente": expediente,
                    "fecha": fecha,
                    "nombre": nombre,
                    "link": link
                })
                seen.add(key)

    if f_t:
        with f_t as f:
            before = len(out_rows)
            process(csv.DictReader(f))
            count_t = len(out_rows) - before
    if f_T:
        with f_T as f:
            before = len(out_rows)
            process(csv.DictReader(f))
            count_T = len(out_rows) - before

    with open("etl_resoluciones.csv", "w", newline="", encoding="utf-8") as f_out:
        writer = csv.DictWriter(f_out, fieldnames=fieldnames_res)
        writer.writeheader()
        for r in out_rows:
            writer.writerow(r)

    print(f"Resoluciones en trámite procesadas (nuevas): {count_t}")
    print(f"Resoluciones terminadas procesadas (nuevas): {count_T}")
    print(f"Total resoluciones combinadas: {len(out_rows)}")

# =========================
# 4) RADICACIONES
# =========================

def procesar_radicaciones():
    print("Procesando radicaciones...")
    df_t = safe_read_csv_pd("5_radicaciones.csv")
    df_T = safe_read_csv_pd("scraper_completas_terminadas_radicaciones.csv")

    for df in (df_t, df_T):
        if df.empty:
            continue
        # Renombrado flexible
        rename_map = {
            "Expediente": "numero_expediente",
            "Orden": "orden",
            "Fecha": "fecha_radicacion",
            "Tribunal": "tribunal",
            "Fiscal": "fiscal_nombre",
            "Fiscalía": "fiscalia",
            "Fiscalia": "fiscalia"
        }
        for k, v in rename_map.items():
            if k in df.columns and v not in df.columns:
                df.rename(columns={k: v}, inplace=True)

        # Limpieza
        for col in ["numero_expediente", "tribunal", "fiscal_nombre", "fiscalia"]:
            if col in df.columns:
                df[col] = df[col].map(limpiar_texto)
        if "fecha_radicacion" in df.columns:
            df["fecha_radicacion"] = df["fecha_radicacion"].map(parse_date)

    df_final = pd.concat([df_t, df_T], ignore_index=True)
    keep = ["numero_expediente", "orden", "fecha_radicacion", "tribunal", "fiscal_nombre", "fiscalia"]
    for k in keep:
        if k not in df_final.columns:
            df_final[k] = None
    df_final = df_final[keep].dropna(subset=["numero_expediente"]).drop_duplicates()

    df_final.to_csv("etl_radicaciones.csv", index=False)
    print(f"Radicaciones combinadas: {len(df_final)}")

# =========================
# 5) Dimensiones: FUEROS / JURISDICCIONES / TRIBUNALES
# =========================

def generar_dim_fueros(df_expedientes):
    print("Extrayendo fueros únicos...")
    vals = sorted(set([v for v in df_expedientes.get("fuero", []).tolist() if v]))
    with open("etl_fueros.csv", "w", newline="", encoding="utf-8") as f_out:
        writer = csv.DictWriter(f_out, fieldnames=["fuero_id", "nombre"])
        writer.writeheader()
        for i, nombre in enumerate(vals, start=1):
            writer.writerow({"fuero_id": i, "nombre": nombre})
    print(f"Fueros únicos: {len(vals)}")

def generar_dim_jurisdicciones(df_expedientes):
    print("Extrayendo jurisdicciones únicas...")
    # Mapeo simple: ambito = jurisdiccion (Federal/Nacional); provincia y dpto opcionales
    vals = sorted(set([v for v in df_expedientes.get("jurisdiccion", []).tolist() if v]))
    with open("etl_jurisdicciones.csv", "w", newline="", encoding="utf-8") as f_out:
        writer = csv.DictWriter(f_out, fieldnames=["jurisdiccion_id", "ambito", "provincia", "departamento_judicial"])
        writer.writeheader()
        for i, ambito in enumerate(vals, start=1):
            writer.writerow({
                "jurisdiccion_id": i,
                "ambito": ambito,
                "provincia": None,
                "departamento_judicial": "Comodoro Py"  # ajustar si tenés mejor dato
            })
    print(f"Jurisdicciones únicas: {len(vals)}")

def generar_dim_tribunales(df_expedientes, tribunales_full_path="tribunales_full.csv"):
    print("Extrayendo tribunales...")
    # Base desde expedientes
    tribunales = {}
    if not df_expedientes.empty:
        for _, row in df_expedientes.iterrows():
            nombre = limpiar_texto(row.get("tribunal"))
            if not nombre:
                continue
            if nombre not in tribunales:
                tribunales[nombre] = {
                    "nombre": nombre,
                    "instancia": "Primera Instancia",
                    "domicilio_sede": None,
                    "contacto": None,
                    "fuero": row.get("fuero"),
                    "jurisdiccion": row.get("jurisdiccion")
                }

    # Enriquecer con tribunales_full.csv si existe
    df_tr = safe_read_csv_pd(tribunales_full_path)
    if not df_tr.empty:
        # Intentar mapeo flexible de columnas
        # columnas posibles: tribunal, nombre, instancia, nivel, direccion, domicilio, telefono, email, fuero, jurisdiccion, localidad, contacto
        # normalizar nombres
        col_map = {}
        for c in df_tr.columns:
            lc = c.lower()
            if lc in ("tribunal", "nombre_tribunal", "nombre"):
                col_map.setdefault("nombre", c)
            elif "instancia" in lc or "nivel" in lc:
                col_map.setdefault("instancia", c)
            elif "direccion" in lc or "domicilio" in lc:
                col_map.setdefault("domicilio_sede", c)
            elif "telefono" in lc:
                col_map.setdefault("telefono", c)
            elif "email" in lc or "correo" in lc:
                col_map.setdefault("email", c)
            elif "fuero" in lc:
                col_map.setdefault("fuero", c)
            elif "jurisdic" in lc:
                col_map.setdefault("jurisdiccion", c)
            elif "localidad" in lc or "ciudad" in lc:
                col_map.setdefault("localidad", c)
            elif "contacto" in lc:
                col_map.setdefault("contacto", c)

        def get(row, key):
            col = col_map.get(key)
            return limpiar_texto(row[col]) if col and col in row else None

        for _, row in df_tr.iterrows():
            nombre = get(row, "nombre")
            if not nombre:
                continue
            if nombre not in tribunales:
                tribunales[nombre] = {
                    "nombre": nombre,
                    "instancia": get(row, "instancia") or "N/D",
                    "domicilio_sede": get(row, "domicilio_sede"),
                    "contacto": None,
                    "fuero": get(row, "fuero"),
                    "jurisdiccion": get(row, "jurisdiccion")
                }
            # armar contacto si hay tel/email
            tel = get(row, "telefono")
            mail = get(row, "email")
            contacto = " | ".join([p for p in [f"Tel: {tel}" if tel else None, f"Email: {mail}" if mail else None] if p])
            if contacto:
                tribunales[nombre]["contacto"] = contacto

    # Asignar IDs consistentes
    nombres_sorted = sorted(tribunales.keys())
    nombre_to_id = {n: i+1 for i, n in enumerate(nombres_sorted)}

    # necesitamos id de jurisdiccion (en etl_jurisdicciones.csv) para foreign key en init.sql?
    # En tu init.sql, tribunal tiene fk a jurisdiccion_id, pero acá exportamos CSV; el cargador resolverá esa FK.
    # Por ahora, mapeamos ambito→jurisdiccion_id en el cargador o seteamos 1 por default si no matchea.
    # Generamos etl_tribunales.csv con jurisdiccion_id estimado a 1 (luego el cargador puede mapear con etl_jurisdicciones).
    juris_map = {}
    if os.path.exists("etl_jurisdicciones.csv"):
        with open("etl_jurisdicciones.csv", newline="", encoding="utf-8") as f:
            r = csv.DictReader(f)
            for row in r:
                juris_map[row["ambito"]] = int(row["jurisdiccion_id"])

    with open("etl_tribunales.csv", "w", newline="", encoding="utf-8") as f_out:
        writer = csv.DictWriter(
            f_out,
            fieldnames=["tribunal_id", "nombre", "instancia", "domicilio_sede", "contacto", "jurisdiccion_id", "fuero"]
        )
        writer.writeheader()
        for nombre in nombres_sorted:
            t = tribunales[nombre]
            ambito = t.get("jurisdiccion") or "Federal"
            jurisdiccion_id = juris_map.get(ambito, 1)
            writer.writerow({
                "tribunal_id": nombre_to_id[nombre],
                "nombre": t.get("nombre"),
                "instancia": t.get("instancia"),
                "domicilio_sede": t.get("domicilio_sede"),
                "contacto": t.get("contacto"),
                "jurisdiccion_id": jurisdiccion_id,
                "fuero": t.get("fuero")
            })

    print(f"Tribunales únicos: {len(nombres_sorted)}")
    return nombre_to_id, tribunales_full_path

# =========================
# 6) JUECES y TRIBUNAL_JUEZ (desde tribunales_full.csv)
# =========================

def procesar_jueces_y_relaciones(nombre_to_id, tribunales_full_path="tribunales_full.csv"):
    print("Procesando jueces y relaciones tribunal-juez (tribunales_full.csv)...")
    df = safe_read_csv_pd(tribunales_full_path)
    if df.empty:
        print("Advertencia: tribunales_full.csv no encontrado o vacío. Se omite jueces/relaciones.")
        return

    # Intentar columnas típicas:
    # juez / magistrado / nombre_juez
    # cargo
    # situacion / situación
    # email / telefono
    # tribunal
    # Si no, algunas fuentes traen 'responsables' con formato 'Nombre: X | Cargo: Y | ...'
    cols = [c.lower() for c in df.columns]

    # Normalizar nombres útiles
    def pick(*names):
        for n in names:
            if n in df.columns:
                return n
        # buscar por lowercase aproximado
        for c in df.columns:
            lc = c.lower()
            for n in names:
                if n.lower() in lc:
                    return c
        return None

    col_tribunal = pick("tribunal", "nombre_tribunal", "juzgado", "camara", "cámara", "organismo", "dependencia")
    col_juez = pick("juez", "magistrado", "nombre_juez", "responsable", "nombre")
    col_cargo = pick("cargo", "funcion", "función", "puesto")
    col_situacion = pick("situacion", "situación", "condicion", "condición")
    col_email = pick("email", "correo", "mail")
    col_tel = pick("telefono", "teléfono", "tel")
    col_responsables = pick("responsables")

    # Parse de bloques tipo "responsables"
    def parsear_responsables(texto):
        if not isinstance(texto, str) or not texto.strip():
            return []
        bloques = [b.strip() for b in re.split(r";|\n", texto) if b.strip()]
        mags = []
        for b in bloques:
            m_nombre = re.search(r'Nombre:\s*([^|]+)', b, flags=re.I)
            m_cargo = re.search(r'Cargo:\s*([^|]+)', b, flags=re.I)
            m_email = re.search(r'Email:\s*([^|]+)', b, flags=re.I)
            m_tel = re.search(r'(Tel|Teléfono):\s*([^|]+)', b, flags=re.I)
            m_sit = re.search(r'Situación:\s*([^|]+)', b, flags=re.I)
            nombre = limpiar_texto(m_nombre.group(1)) if m_nombre else None
            if nombre:
                mags.append({
                    "nombre": nombre,
                    "cargo": limpiar_texto(m_cargo.group(1)) if m_cargo else None,
                    "email": limpiar_texto(m_email.group(1)) if m_email else None,
                    "telefono": limpiar_texto(m_tel.group(2)) if m_tel else None,
                    "situacion": limpiar_texto(m_sit.group(1)) if m_sit else None,
                })
        return mags

    jueces = {}
    relaciones = set()

    for _, row in df.iterrows():
        nombre_trib = limpiar_texto(row[col_tribunal]) if col_tribunal else None
        if not nombre_trib or nombre_trib not in nombre_to_id:
            continue
        tribunal_id = nombre_to_id[nombre_trib]

        # Caso 1: columnas explícitas
        if col_juez and pd.notna(row[col_juez]):
            nombre = limpiar_texto(row[col_juez])
            if nombre:
                jueces.setdefault(nombre, {"email": None, "telefono": None})
                if col_email and pd.notna(row[col_email]):
                    jueces[nombre]["email"] = limpiar_texto(row[col_email])
                if col_tel and pd.notna(row[col_tel]):
                    jueces[nombre]["telefono"] = limpiar_texto(row[col_tel])
                cargo = limpiar_texto(row[col_cargo]) if col_cargo and pd.notna(row[col_cargo]) else None
                situacion = limpiar_texto(row[col_situacion]) if col_situacion and pd.notna(row[col_situacion]) else None
                relaciones.add((tribunal_id, nombre, cargo, situacion))
            continue

        # Caso 2: campo 'responsables' con bloques
        if col_responsables and pd.notna(row[col_responsables]):
            for mag in parsear_responsables(row[col_responsables]):
                nombre = mag.get("nombre")
                if not nombre:
                    continue
                jueces.setdefault(nombre, {"email": mag.get("email"), "telefono": mag.get("telefono")})
                # no pisar datos válidos
                if not jueces[nombre].get("email") and mag.get("email"):
                    jueces[nombre]["email"] = mag.get("email")
                if not jueces[nombre].get("telefono") and mag.get("telefono"):
                    jueces[nombre]["telefono"] = mag.get("telefono")
                relaciones.add((tribunal_id, nombre, mag.get("cargo"), mag.get("situacion")))

    # Asignar IDs a jueces
    jueces_sorted = sorted(jueces.keys())
    juez_to_id = {n: i+1 for i, n in enumerate(jueces_sorted)}

    # etl_jueces.csv
    with open("etl_jueces.csv", "w", newline="", encoding="utf-8") as f_out:
        writer = csv.DictWriter(f_out, fieldnames=["juez_id", "nombre", "email", "telefono"])
        writer.writeheader()
        for nombre in jueces_sorted:
            data = jueces[nombre]
            writer.writerow({
                "juez_id": juez_to_id[nombre],
                "nombre": nombre,
                "email": data.get("email"),
                "telefono": data.get("telefono")
            })
    print(f"Jueces procesados: {len(jueces_sorted)}")

    # etl_tribunal_juez.csv
    with open("etl_tribunal_juez.csv", "w", newline="", encoding="utf-8") as f_out:
        writer = csv.DictWriter(f_out, fieldnames=["tribunal_id", "juez_id", "cargo", "situacion"])
        writer.writeheader()
        for tribunal_id, nombre, cargo, situacion in sorted(relaciones):
            writer.writerow({
                "tribunal_id": tribunal_id,
                "juez_id": juez_to_id.get(nombre),
                "cargo": cargo,
                "situacion": situacion or "Efectivo"
            })
    print(f"Relaciones tribunal-juez: {len(relaciones)}")

# =========================
# Main
# =========================

def main():
    print("=== Iniciando ETL completo ===")
    # 1) Expedientes
    df_expedientes = procesar_expedientes()
    print("etl_expedientes.csv generado.")

    # 2) Partes / Letrados / Representaciones
    procesar_intervinientes()
    print("etl_partes.csv, etl_letrados.csv y etl_representaciones.csv generados.")

    # 3) Resoluciones
    procesar_resoluciones()
    print("etl_resoluciones.csv generado.")

    # 4) Radicaciones
    procesar_radicaciones()
    print("etl_radicaciones.csv generado.")

    # 5) Dimensiones (a partir de expedientes)
    generar_dim_fueros(df_expedientes)
    generar_dim_jurisdicciones(df_expedientes)
    nombre_to_id, tr_path = generar_dim_tribunales(df_expedientes)

    # 6) Jueces y relaciones (desde tribunales_full.csv)
    procesar_jueces_y_relaciones(nombre_to_id, tribunales_full_path=tr_path)

    print("=== ETL finalizado correctamente ===")
    print("Archivos generados:")
    print(" - etl_expedientes.csv")
    print(" - etl_partes.csv")
    print(" - etl_letrados.csv")
    print(" - etl_representaciones.csv")
    print(" - etl_resoluciones.csv")
    print(" - etl_radicaciones.csv")
    print(" - etl_fueros.csv")
    print(" - etl_jurisdicciones.csv")
    print(" - etl_tribunales.csv")
    print(" - etl_jueces.csv")
    print(" - etl_tribunal_juez.csv")

if __name__ == "__main__":
    main()
