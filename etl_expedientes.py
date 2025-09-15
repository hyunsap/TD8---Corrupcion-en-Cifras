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
    Roles detectados: imputado, denunciado, denunciante, querellante
    Ignora 'otros', 'nn' y 'testigo de identidad reservada'.
    """
    partes = []

    # Patrón para detectar rol: busca ROL: NOMBRES
    patron_roles = re.findall(r"(IMPUTADO|DENUNCIADO|DENUNCIANTE|QUERELLANTE):\s*(.*?)(?=(IMPUTADO|DENUNCIADO|DENUNCIANTE|QUERELLANTE|$))", caratula.upper(), re.DOTALL)
    
    for rol, bloque, _ in patron_roles:
        # Separar nombres por " y " pero no por comas internas en apellidos
        nombres = re.split(r"\s+Y\s+", bloque)
        for nombre in nombres:
            nombre = nombre.strip()
            # Filtrar nombres inválidos
            if not nombre or any(x in nombre.upper() for x in ["OTROS", "NN", "TESTIGO DE IDENTIDAD RESERVADA"]):
                continue
            partes.append({
                "numero_expediente": numero_expediente,
                "nombre": nombre.title(),
                "rol": rol.lower()
            })

    return partes



# === ETL Expedientes ===
with open("4_expedientes.csv", newline="", encoding="utf-8") as f_in, \
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
            "fecha_inicio": fecha_inicio,
            "fecha_ultimo_movimiento": row["Última actualización"],
            "camara_origen": camara,
            "ano_inicio": ano_inicio,
            "delitos": row["Delitos"],
            "fiscal": fiscal,
            "fiscalia": fiscalia
        })

print("✅ Expedientes procesados → etl_expedientes.csv listo")

# === ETL Partes (desde la carátula del expediente) ===
with open("4_expedientes.csv", newline="", encoding="utf-8") as f_in, \
     open("etl_partes.csv", "w", newline="", encoding="utf-8") as f_out:

    reader = csv.DictReader(f_in)
    fieldnames = ["numero_expediente", "nombre", "rol"]
    writer = csv.DictWriter(f_out, fieldnames=fieldnames)
    writer.writeheader()

    for row in reader:
        numero = row["Expediente"]
        caratula = row["Carátula"]
        partes = extraer_partes(caratula, numero)

        if partes:
            for p in partes:
                writer.writerow(p)
        else:
            # fallback si no se detecta patrón
            writer.writerow({
                "numero_expediente": numero,
                "nombre": caratula.strip(),
                "rol": "interviniente"
            })

print("✅ Partes procesadas → etl_partes.csv listo")



# === ETL Letrados ===
with open("4_letrados.csv", newline="", encoding="utf-8") as f_in, \
     open("etl_letrados.csv", "w", newline="", encoding="utf-8") as f_out:

    reader = csv.DictReader(f_in)
    fieldnames = ["numero_expediente", "interviniente", "letrado"]
    writer = csv.DictWriter(f_out, fieldnames=fieldnames)
    writer.writeheader()

    for row in reader:
        writer.writerow({
            "numero_expediente": row["Expediente"],
            "interviniente": row["Imputado"],
            "letrado": row["Letrado"]
        })

print("✅ Letrados procesados → etl_letrados.csv listo")
