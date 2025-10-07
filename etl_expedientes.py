import csv
import re
from datetime import datetime

# === Diccionarios de normalización ===
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
    "CFP": "Penal Federal",
    "CCC": "Penal Federal",
    "CPF": "Penal Federal",
    "FRO": "Penal Federal",
    "FGR": "Penal Federal",
    "FPO": "Penal Federal",
    "FTU": "Penal Federal",
    "FCB": "Penal Federal",
    "FPA": "Penal Federal",
    "FSA": "Penal Federal",
    "FBB": "Penal Federal",
    "FCT": "Penal Federal",
    "FMZ": "Penal Federal",
    "FCR": "Penal Federal",
    "FSM": "Penal Federal",
    "FLP": "Penal Federal",
    "FMP": "Penal Federal",
    "FRE": "Penal Federal",
    "CSS": "Penal Federal",
    "CPN": "Penal Federal",
    "CPE": "Penal Federal",
    "CCC": "Penal Federal",
    "CCF": "Civil",
    "CIV": "Civil",
    "COM": "Comercial",
    "CNT": "Laboral",
    "CAF": "Contencioso Administrativo",
    "CNE": "Electoral"
}

# === Funciones de normalización ===
def inferir_fuero_por_camara(numero_expediente):
    sigla = numero_expediente.split()[0].upper()
    return FUERO_POR_CAMARA.get(sigla, "Desconocido")

def inferir_jurisdiccion_por_radicacion(radicacion):
    if "FEDERAL" in radicacion.upper():
        return "Federal"
    return "Nacional"

def extraer_camara_y_ano(numero_expediente):
    match = re.match(r"(\w+)\s+\d+/(\d{4})", numero_expediente)
    if match:
        sigla, ano = match.groups()
        camara = CAMARAS.get(sigla, "Desconocida")
        return camara, int(ano)
    return "Desconocida", None

def desarmar_radicacion(radicacion):
    partes = [p.strip() for p in radicacion.split("|")]

    fecha = partes[0] if len(partes) > 0 else ""
    tribunal = partes[1] if len(partes) > 1 else ""
    fiscal = ""
    fiscalia = ""

    for p in partes[2:]:
        if p.upper().startswith("FISCAL:"):
            fiscal = p.replace("Fiscal:", "").strip()
        elif p.upper().startswith("FISCALIA"):
            fiscalia = p.strip()

    return fecha, tribunal, fiscal, fiscalia

def extraer_partes(caratula, numero_expediente):
    """
    Extrae personas y su rol a partir de la carátula.
    """
    partes = []
    patron_roles = re.findall(r"(IMPUTADO|DENUNCIADO|DENUNCIANTE|QUERELLANTE):\s*(.*?)(?=(IMPUTADO|DENUNCIADO|DENUNCIANTE|QUERELLANTE|$))", caratula.upper(), re.DOTALL)
    
    for rol, bloque, _ in patron_roles:
        nombres = re.split(r"\s+Y\s+", bloque)
        for nombre in nombres:
            nombre = nombre.strip()
            if not nombre or any(x in nombre.upper() for x in ["OTROS", "NN", "TESTIGO DE IDENTIDAD RESERVADA"]):
                continue
            partes.append({
                "numero_expediente": numero_expediente,
                "nombre": nombre.title(),
                "rol": rol.lower()
            })
    return partes

def parse_date(fecha_str):
    """Convierte fecha DD/MM/YYYY a YYYY-MM-DD, o devuelve '' si falla"""
    if fecha_str:
        try:
            return datetime.strptime(fecha_str.strip(), "%d/%m/%Y").strftime("%Y-%m-%d")
        except Exception:
            return ""
    return ""

# === ETL Expedientes ===
with open("4_1_expedientes.csv", newline="", encoding="utf-8") as f_in, \
     open("etl_expedientes.csv", "w", newline="", encoding="utf-8") as f_out:

    reader = csv.DictReader(f_in)
    fieldnames = [
        "numero_expediente",
        "caratula",
        "fuero",
        "jurisdiccion",
        "tribunal",
        "estado",
        "fecha_inicio",
        "fecha_ultimo_movimiento",
        "camara_origen",
        "ano_inicio",
        "delitos",
        "fiscal",
        "fiscalia"
    ]
    writer = csv.DictWriter(f_out, fieldnames=fieldnames)
    writer.writeheader()

    for row in reader:
        numero = row["Expediente"]
        radicacion = row["Radicación del expediente"]

        fecha_inicio, tribunal, fiscal, fiscalia = desarmar_radicacion(radicacion)
        camara, ano_inicio = extraer_camara_y_ano(numero)
        fuero = inferir_fuero_por_camara(numero)
        jurisdiccion = inferir_jurisdiccion_por_radicacion(radicacion)

        writer.writerow({
            "numero_expediente": numero,
            "caratula": row["Carátula"],
            "fuero": fuero,
            "jurisdiccion": jurisdiccion,
            "tribunal": tribunal,
            "estado": row["Estado"],
            "fecha_inicio": parse_date(fecha_inicio),
            "fecha_ultimo_movimiento": parse_date(row["Última actualización"]),
            "camara_origen": camara,
            "ano_inicio": ano_inicio,
            "delitos": row["Delitos"],
            "fiscal": fiscal,
            "fiscalia": fiscalia
        })

print("✅ Expedientes procesados → etl_expedientes.csv listo")

# === ETL Intervinientes ===
with open("4_1_intervinientes.csv", newline="", encoding="utf-8") as f_in, \
        open("etl_partes.csv", "w", newline="", encoding="utf-8") as f_out:
        
    reader = csv.DictReader(f_in)
    partes_seen = set()
    fieldnames_partes = ["numero_expediente", "nombre", "rol"]
    writer_partes = csv.DictWriter(f_out, fieldnames=fieldnames_partes)
    writer_partes.writeheader()

    for row in reader:
        expediente = row["Expediente"].strip()
        rol = row["Rol"].strip().lower()
        nombre = row["Nombre"].strip().title()
        if nombre:
            key = (expediente, nombre, rol)
            if key not in partes_seen:
                writer_partes.writerow({
                    "numero_expediente": expediente,
                    "nombre": nombre,
                    "rol": rol
                })
                partes_seen.add(key)

with open("4_1_intervinientes.csv", newline="", encoding="utf-8") as f_in, \
        open("etl_letrados.csv", "w", newline="", encoding="utf-8") as f_out:
        
    reader = csv.DictReader(f_in)
    letrados_seen = set()
    fieldnames_letrados = ["numero_expediente", "interviniente", "letrado"]
    writer_letrados = csv.DictWriter(f_out, fieldnames=fieldnames_letrados)
    writer_letrados.writeheader()

    for row in reader:
        expediente = row["Expediente"].strip()
        nombre = row["Nombre"].strip().title()
        if nombre:
            letrado = row["Letrado"].strip().title()
            if letrado:
                key = (expediente, nombre, letrado)
                if key not in letrados_seen:
                    writer_letrados.writerow({
                        "numero_expediente": expediente,
                        "interviniente": nombre,
                        "letrado": letrado
                    })
                    letrados_seen.add(key)

print("✅ Intervinientes normalizados → etl_partes.csv y etl_letrados.csv listos")

# === ETL Resoluciones ===
with open("4_1_resoluciones.csv", newline="", encoding="utf-8") as f_in, \
     open("etl_resoluciones.csv", "w", newline="", encoding="utf-8") as f_out:

    reader = csv.DictReader(f_in)
    fieldnames_res = ["numero_expediente", "fecha", "nombre", "link"]
    writer = csv.DictWriter(f_out, fieldnames=fieldnames_res)
    writer.writeheader()

    resoluciones_seen = set()

    for row in reader:
        expediente = row["Expediente"].strip()
        fecha = parse_date(row["Fecha"].strip())
        nombre = row["Nombre"].strip()
        link = row["Link"].strip()

        key = (expediente, fecha, nombre, link)
        if key not in resoluciones_seen:
            writer.writerow({
                "numero_expediente": expediente,
                "fecha": fecha,
                "nombre": nombre,
                "link": link
            })
            resoluciones_seen.add(key)

print("✅ Resoluciones normalizadas → etl_resoluciones.csv listo")

# === ETL Tribunales ===
with open("etl_expedientes.csv", newline="", encoding="utf-8") as f_in, \
     open("etl_tribunales.csv", "w", newline="", encoding="utf-8") as f_out:

    reader = csv.DictReader(f_in)
    writer = csv.DictWriter(f_out, fieldnames=["nombre", "fuero"])
    writer.writeheader()

    tribunales_seen = set()
    for row in reader:
        nombre = row["tribunal"].strip()
        fuero = row["fuero"].strip()
        if nombre and (nombre, fuero) not in tribunales_seen:
            writer.writerow({"nombre": nombre, "fuero": fuero})
            tribunales_seen.add((nombre, fuero))

print("✅ Tribunales extraídos → etl_tribunales.csv listo")
