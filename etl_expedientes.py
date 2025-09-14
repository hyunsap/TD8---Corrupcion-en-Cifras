import csv
import re

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
    if "FEDERAL" in radicacion or "Federal" in radicacion:
        return "Federal"
    return "Nacional"


def limpiar_tribunal(texto):
    partes = texto.split("|")
    if len(partes) > 1:
        tribunal = partes[1].strip()
    else:
        tribunal = texto.strip()
    return tribunal

def extraer_camara_y_ano(numero_expediente):
    match = re.match(r"(\w+)\s+\d+/(\d{4})", numero_expediente)
    if match:
        sigla, ano = match.groups()
        camara = CAMARAS.get(sigla, "Desconocida")
        return camara, int(ano)
    return "Desconocida", None

# === ETL ===
input_file = "4_expedientes.csv"
output_file = "etl_expedientes.csv"

with open(input_file, newline="", encoding="utf-8") as f_in, \
     open(output_file, "w", newline="", encoding="utf-8") as f_out:

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
        "camara_origen"
    ]
    writer = csv.DictWriter(f_out, fieldnames=fieldnames)
    writer.writeheader()

    for row in reader:
        numero = row["Expediente"]
        radicacion = row["Radicación del expediente"]

        tribunal = limpiar_tribunal(radicacion)
        camara, ano_inicio = extraer_camara_y_ano(numero)
        fuero = inferir_fuero_por_camara(numero)
        jurisdiccion = inferir_jurisdiccion_por_radicacion(tribunal)
        fecha_inicio = str(ano_inicio) if ano_inicio else ""

        writer.writerow({
            "numero_expediente": numero,
            "caratula": row["Carátula"],
            "fuero": fuero,
            "jurisdiccion": jurisdiccion,
            "tribunal": tribunal,
            "estado": row["Estado"],
            "fecha_inicio": ano_inicio,
            "fecha_ultimo_movimiento": row["Última actualización"],
            "camara_origen": camara
        })

print("✅ ETL completado → etl_expedientes.csv listo")
