# 5_etl_expedientes.py
import csv
import re
import os
import unicodedata
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

def limpiar_texto(texto):
    if texto is None:
        return None
    s = " ".join(str(texto).split())
    return s if s != "" else None

def parse_date(fecha_str):
    if not fecha_str:
        return ""
    try:
        return datetime.strptime(fecha_str.strip(), "%d/%m/%Y").strftime("%Y-%m-%d")
    except Exception:
        return ""

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

# =========================
# Funciones auxiliares de inferencia
# =========================

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
    f_tramite = safe_open("5_expedientes.csv", "r", newline="", encoding="utf-8")
    f_term = safe_open("scraper_completas_terminadas_expedientes.csv", "r", newline="", encoding="utf-8")

    rows = []
    count_tramite = count_term = 0

    def procesar_reader(reader, estado_id):
        nonlocal rows, count_tramite, count_term
        for row in reader:
            numero = limpiar_texto(row.get("Expediente") or row.get("numero_expediente"))
            caratula = limpiar_texto(row.get("Carátula") or row.get("caratula"))
            ultima_act = limpiar_texto(row.get("Última actualización") or row.get("fecha_ultimo_mov"))
            radicacion = row.get("Radicación del expediente") or ""
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
                "estado_procesal": "En trámite" if estado_id == 1 else "Terminada",
                "fecha_inicio": parse_date(fecha_inicio),
                "fecha_ultimo_movimiento": parse_date(ultima_act),
                "camara_origen": camara,
                "ano_inicio": ano_inicio,
                "delitos": delitos,
                "fiscal": limpiar_texto(fiscal),
                "fiscalia": limpiar_texto(fiscalia),
                "fuero": fuero
            })
            if estado_id == 1: count_tramite += 1
            else: count_term += 1

    if f_tramite: 
        with f_tramite as f: procesar_reader(csv.DictReader(f), 1)
    if f_term: 
        with f_term as f: procesar_reader(csv.DictReader(f), 2)

    print(f"Expedientes en trámite: {count_tramite}, terminados: {count_term}")

    fieldnames = [
        "numero_expediente","caratula","jurisdiccion","tribunal","estado_procesal",
        "fecha_inicio","fecha_ultimo_movimiento","camara_origen","ano_inicio",
        "delitos","fiscal","fiscalia"
    ]
    with open("etl_expedientes.csv","w",newline="",encoding="utf-8") as f_out:
        writer=csv.DictWriter(f_out,fieldnames=fieldnames)
        writer.writeheader()
        for r in rows:
            writer.writerow({k:r.get(k) for k in fieldnames})
    return pd.DataFrame(rows)

# =========================
# 2) PARTES / LETRADOS / REPRESENTACIONES
# =========================

def procesar_intervinientes():
    print("Procesando intervinientes...")
    df_t=safe_read_csv_pd("5_intervinientes.csv")
    df_T=safe_read_csv_pd("scraper_completas_terminadas_intervinientes.csv")
    for df in (df_t,df_T):
        if df.empty: continue
        rename={"Expediente":"numero_expediente","Nombre":"nombre","Rol":"rol","Letrado":"letrado"}
        for k,v in rename.items():
            if k in df.columns: df.rename(columns={k:v},inplace=True)
        for c in ["numero_expediente","nombre","rol","letrado"]:
            if c in df.columns: df[c]=df[c].map(limpiar_texto)
    df_all=pd.concat([df_t,df_T],ignore_index=True)
    partes=df_all.dropna(subset=["numero_expediente","nombre"])[["numero_expediente","nombre","rol"]].drop_duplicates()
    partes.to_csv("etl_partes.csv",index=False)
    letrados=df_all.dropna(subset=["numero_expediente","nombre","letrado"]).rename(columns={"nombre":"interviniente"})[["numero_expediente","interviniente","letrado"]].drop_duplicates()
    letrados.to_csv("etl_letrados.csv",index=False)
    reprs=df_all.dropna(subset=["numero_expediente","nombre","letrado"]).rename(columns={"nombre":"nombre_parte"})[["numero_expediente","nombre_parte","letrado","rol"]].drop_duplicates()
    reprs.to_csv("etl_representaciones.csv",index=False)
    print(f"Partes:{len(partes)}, Letrados:{len(letrados)}, Representaciones:{len(reprs)}")

# =========================
# 3) RESOLUCIONES
# =========================

def procesar_resoluciones():
    print("Procesando resoluciones...")
    f_t=safe_open("5_resoluciones.csv","r",encoding="utf-8")
    f_T=safe_open("scraper_completas_terminadas_resoluciones.csv","r",encoding="utf-8")
    out=[]; seen=set()
    def process(reader):
        for r in reader:
            exp=limpiar_texto(r.get("Expediente")or r.get("numero_expediente"))
            if not exp: continue
            fecha=parse_date(r.get("Fecha")or r.get("fecha"))
            nom=limpiar_texto(r.get("Nombre")or r.get("nombre"))
            link=limpiar_texto(r.get("Link")or r.get("link"))
            key=(exp,fecha,nom,link)
            if key not in seen:
                seen.add(key)
                out.append({"numero_expediente":exp,"fecha":fecha,"nombre":nom,"link":link})
    if f_t: process(csv.DictReader(f_t))
    if f_T: process(csv.DictReader(f_T))
    with open("etl_resoluciones.csv","w",newline="",encoding="utf-8") as f:
        writer=csv.DictWriter(f,fieldnames=["numero_expediente","fecha","nombre","link"])
        writer.writeheader(); writer.writerows(out)
    print(f"Resoluciones combinadas: {len(out)}")

# =========================
# 4) RADICACIONES
# =========================

def procesar_radicaciones():
    print("Procesando radicaciones...")
    df_t=safe_read_csv_pd("5_radicaciones.csv")
    df_T=safe_read_csv_pd("scraper_completas_terminadas_radicaciones.csv")
    df=pd.concat([df_t,df_T],ignore_index=True)
    ren={"Expediente":"numero_expediente","Orden":"orden","Fecha":"fecha_radicacion",
         "Tribunal":"tribunal","Fiscal":"fiscal_nombre","Fiscalía":"fiscalia","Fiscalia":"fiscalia"}
    for k,v in ren.items():
        if k in df.columns: df.rename(columns={k:v},inplace=True)
    for c in ["numero_expediente","tribunal","fiscal_nombre","fiscalia"]:
        if c in df.columns: df[c]=df[c].map(limpiar_texto)
    if "fecha_radicacion" in df.columns:
        df["fecha_radicacion"]=df["fecha_radicacion"].map(parse_date)
    keep=["numero_expediente","orden","fecha_radicacion","tribunal","fiscal_nombre","fiscalia"]
    for k in keep:
        if k not in df.columns: df[k]=None
    df=df[keep].dropna(subset=["numero_expediente"]).drop_duplicates()
    df.to_csv("etl_radicaciones.csv",index=False)
    print(f"Radicaciones combinadas: {len(df)}")

# =========================
# 5) Dimensiones: FUEROS / JURISDICCIONES
# =========================

def generar_dim_fueros(df):
    vals=sorted(set([v for v in df.get("fuero",[]).tolist() if v]))
    with open("etl_fueros.csv","w",newline="",encoding="utf-8") as f:
        w=csv.DictWriter(f,fieldnames=["fuero_id","nombre"]);w.writeheader()
        for i,n in enumerate(vals,start=1): w.writerow({"fuero_id":i,"nombre":n})
    print(f"Fueros únicos: {len(vals)}")

def generar_dim_jurisdicciones(df):
    vals=sorted(set([v for v in df.get("jurisdiccion",[]).tolist() if v]))
    with open("etl_jurisdicciones.csv","w",newline="",encoding="utf-8") as f:
        w=csv.DictWriter(f,fieldnames=["jurisdiccion_id","ambito","provincia","departamento_judicial"])
        w.writeheader()
        for i,a in enumerate(vals,start=1):
            w.writerow({"jurisdiccion_id":i,"ambito":a,"provincia":None,"departamento_judicial":"Comodoro Py"})
    print(f"Jurisdicciones únicas: {len(vals)}")

# =========================
# 6) Dimensión Tribunales + Jueces (nueva versión)
# =========================

def _strip_accents(s):
    if s is None: return None
    s = str(s)
    nf = unicodedata.normalize('NFD', s)
    return ''.join(ch for ch in nf if unicodedata.category(ch) != 'Mn')

def _norm(s):
    if s is None: return None
    s=_strip_accents(s).lower()
    s=re.sub(r'[^a-z0-9\s]',' ',s)
    return re.sub(r'\s+',' ',s).strip() or None

def _es_dependencia(t):
    return t and (t.startswith('sala ') or t.startswith('secretaria') or 'jurisprudencia' in t)

def _tribunal_desde_path(path):
    if not path: return None
    parts=[p.strip() for p in str(path).split('>') if p.strip()]
    if not parts: return None
    last=parts[-1]
    if 'sala' in _norm(last) or 'secretaria' in _norm(last):
        return parts[-2] if len(parts)>1 else last
    return last

def generar_dim_tribunales(df, path="tribunales_full.csv"):
    print("Extrayendo tribunales (con jerarquía)...")
    tribunales = {}

    # --- Desde los expedientes ---
    for _, r in df.iterrows():
        n = limpiar_texto(r.get("tribunal"))
        if n:
            tribunales[_norm(n)] = {
                "nombre": n,
                "instancia": "Primera Instancia",
                "fuero": r.get("fuero"),
                "jurisdiccion": r.get("jurisdiccion")
            }

    # --- Desde tribunales_full.csv ---
    df_tr = safe_read_csv_pd(path)
    if not df_tr.empty:
        def pick(*cands):
            for c in df_tr.columns:
                for cand in cands:
                    if cand in c.lower():
                        return c
            return None

        col_t = pick('titulo', 'tribunal', 'nombre')
        col_p = pick('path')
        col_det = pick('detalle', 'direccion', 'domicilio')
        col_tel = pick('telefono', 'tel')
        col_mail = pick('email', 'correo')

        for _, r in df_tr.iterrows():
            titulo = str(r[col_t]).strip()
            pathv = str(r[col_p]).strip() if col_p in r and pd.notna(r[col_p]) else None
            nombre = titulo
            if _es_dependencia(_norm(titulo)):
                padre = _tribunal_desde_path(pathv)
                if padre:
                    nombre = padre
            key = _norm(nombre)
            if not key:
                continue

            tel = str(r[col_tel]).strip() if col_tel in r and pd.notna(r[col_tel]) else None
            mail = str(r[col_mail]).strip() if col_mail in r and pd.notna(r[col_mail]) else None
            contacto = " | ".join([p for p in [f"Tel: {tel}" if tel else None,
                                               f"Email: {mail}" if mail else None] if p])

            if key not in tribunales:
                tribunales[key] = {
                    "nombre": nombre,
                    "instancia": "N/D",
                    "domicilio_sede": r.get(col_det),
                    "contacto": contacto or None,
                    "fuero": None,  # ⚠️ este valor a veces queda en None
                    "jurisdiccion": "Federal"
                }

    # --- Asignar IDs ---
    keys = sorted(tribunales.keys())
    nombre_to_id = {k: i + 1 for i, k in enumerate(keys)}

    # --- Escribir CSV ---
    with open("etl_tribunales.csv", "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(
            f,
            fieldnames=[
                "tribunal_id", "nombre", "instancia",
                "domicilio_sede", "contacto", "jurisdiccion_id", "fuero"
            ]
        )
        w.writeheader()
        for k in keys:
            t = tribunales[k]

            # 🩵 Agregá esta línea justo acá:
            if not t.get("fuero"):
                t["fuero"] = "Desconocido"

            w.writerow({
                "tribunal_id": nombre_to_id[k],
                "nombre": t["nombre"],
                "instancia": t.get("instancia"),
                "domicilio_sede": t.get("domicilio_sede"),
                "contacto": t.get("contacto"),
                "jurisdiccion_id": 1,
                "fuero": t.get("fuero")
            })

    print(f"Tribunales únicos: {len(keys)}")
    return nombre_to_id


def procesar_jueces_y_relaciones(nombre_to_id, path="tribunales_full.csv"):
    print("Procesando jueces y relaciones tribunal-juez...")
    df = safe_read_csv_pd(path)
    if df.empty:
        print("Advertencia: tribunales_full.csv no encontrado o vacío. Se omite jueces/relaciones.")
        return

    # Función auxiliar para buscar columnas con nombres parecidos
    def pick(*cands):
        for c in df.columns:
            for cand in cands:
                if cand in c.lower():
                    return c
        return None

    # Detección flexible de columnas
    col_t = pick("titulo", "tribunal", "nombre")
    col_p = pick("path")
    col_r = pick("responsables")

    # =========================
    # Función de parseo robusta
    # =========================
    def parse_responsables(txt):
        if not isinstance(txt, str) or not txt.strip():
            return []
        res = []
        for b in [b.strip() for b in re.split(r";|\n", txt) if b.strip()]:
            # Nombre
            m_nom = re.search(r"Nombre:\s*([^|]+)", b, re.I)
            if not m_nom:
                continue

            # Cargo
            m_car = re.search(r"Cargo:\s*([^|]+)", b, re.I)

            # Teléfono: priorizar el que tiene números reales
            m_tel = re.search(r"Tel[eé]fono:\s*([\d\s/\-]+)", b, re.I)
            if not m_tel:
                # fallback: buscar un Tel: que contenga números
                m_tel = re.search(r"Tel:\s*([\d\s/\-]+)", b, re.I)

            # Email (puede haber más de uno)
            m_mai = re.findall(r"Email:\s*([^|;]+)", b, re.I)
            email = None
            for e in m_mai:
                e = limpiar_texto(e)
                if e and "@" in e:
                    email = e
                    break

            # Situación
            m_sit = re.search(r"Situación:\s*([^|;]+)", b, re.I)

            nombre = limpiar_texto(m_nom.group(1))
            cargo = limpiar_texto(m_car.group(1)) if m_car else None
            tel = limpiar_texto(m_tel.group(1)) if m_tel else None
            sit = limpiar_texto(m_sit.group(1)) if m_sit else None

            # Si el "teléfono" en realidad es el nombre, descartarlo
            if tel and nombre and nombre in tel:
                tel = None

            res.append({
                "nombre": nombre,
                "cargo": cargo,
                "telefono": tel,
                "email": email,
                "situacion": sit
            })
        return res

    # =========================
    # Procesamiento principal
    # =========================
    jueces = {}
    relaciones = set()
    skip = 0

    for _, r in df.iterrows():
        titulo = str(r[col_t]).strip() if col_t else None
        pathv = str(r[col_p]).strip() if col_p and pd.notna(r[col_p]) else None
        nombre_tribunal = titulo

        if _es_dependencia(_norm(titulo)):
            padre = _tribunal_desde_path(pathv)
            if padre:
                nombre_tribunal = padre

        tid = nombre_to_id.get(_norm(nombre_tribunal))
        if not tid:
            skip += 1
            continue

        if col_r and pd.notna(r[col_r]):
            for mag in parse_responsables(r[col_r]):
                nombre = mag["nombre"]
                if not nombre:
                    continue
                jueces.setdefault(nombre, {"email": mag.get("email"), "telefono": mag.get("telefono")})
                # Completar si faltan datos
                if not jueces[nombre].get("email") and mag.get("email"):
                    jueces[nombre]["email"] = mag["email"]
                if not jueces[nombre].get("telefono") and mag.get("telefono"):
                    jueces[nombre]["telefono"] = mag["telefono"]
                relaciones.add((tid, nombre, mag.get("cargo"), mag.get("situacion")))

    # Evitar duplicados exactos de nombre
    jueces = {k: v for k, v in sorted(jueces.items())}

    # =========================
    # Generar CSVs finales
    # =========================
    keys = sorted(jueces.keys())
    jmap = {n: i + 1 for i, n in enumerate(keys)}

    # etl_jueces.csv
    with open("etl_jueces.csv", "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=["juez_id", "nombre", "email", "telefono"])
        w.writeheader()
        for n in keys:
            data = jueces[n]
            w.writerow({
                "juez_id": jmap[n],
                "nombre": n,
                "email": data.get("email"),
                "telefono": data.get("telefono")
            })

    # etl_tribunal_juez.csv
    with open("etl_tribunal_juez.csv", "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=["tribunal_id", "juez_id", "cargo", "situacion"])
        w.writeheader()
        for tid, n, c, s in sorted(relaciones):
            w.writerow({
                "tribunal_id": tid,
                "juez_id": jmap[n],
                "cargo": c,
                "situacion": s or "Efectivo"
            })

    print(f"Jueces procesados: {len(keys)}, Relaciones: {len(relaciones)}, sin match: {skip}")


# =========================
# MAIN
# =========================

def main():
    print("=== Iniciando ETL completo ===")
    df_exp=procesar_expedientes()
    procesar_intervinientes()
    procesar_resoluciones()
    procesar_radicaciones()
    generar_dim_fueros(df_exp)
    generar_dim_jurisdicciones(df_exp)
    nombre_to_id=generar_dim_tribunales(df_exp)
    procesar_jueces_y_relaciones(nombre_to_id)
    print("=== ETL finalizado correctamente ===")

if __name__=="__main__":
    main()
