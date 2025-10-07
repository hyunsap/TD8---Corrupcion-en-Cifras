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

def parse_date(fecha_str):
    """Convierte fecha DD/MM/YYYY a YYYY-MM-DD, o devuelve '' si falla"""
    if fecha_str:
        try:
            return datetime.strptime(fecha_str.strip(), "%d/%m/%Y").strftime("%Y-%m-%d")
        except Exception:
            return ""
    return ""

print("=" * 60)
print("🚀 ETL SISTEMA JUDICIAL - PROCESAMIENTO COMPLETO")
print("=" * 60)

# === ETL Expedientes ===
print("\n📥 Procesando expedientes...")
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

    count = 0
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
        count += 1

print(f"✅ {count} expedientes procesados → etl_expedientes.csv")

# === ETL Intervinientes (Partes) ===
print("\n📥 Procesando partes...")
with open("4_1_intervinientes.csv", newline="", encoding="utf-8") as f_in, \
        open("etl_partes.csv", "w", newline="", encoding="utf-8") as f_out:
        
    reader = csv.DictReader(f_in)
    partes_seen = set()
    fieldnames_partes = ["numero_expediente", "nombre", "rol"]
    writer_partes = csv.DictWriter(f_out, fieldnames=fieldnames_partes)
    writer_partes.writeheader()

    count = 0
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
                count += 1

print(f"✅ {count} partes procesadas → etl_partes.csv")

# === ETL Letrados ===
print("\n📥 Procesando letrados...")
with open("4_1_intervinientes.csv", newline="", encoding="utf-8") as f_in, \
        open("etl_letrados.csv", "w", newline="", encoding="utf-8") as f_out:
        
    reader = csv.DictReader(f_in)
    letrados_seen = set()
    fieldnames_letrados = ["numero_expediente", "interviniente", "letrado"]
    writer_letrados = csv.DictWriter(f_out, fieldnames=fieldnames_letrados)
    writer_letrados.writeheader()

    count = 0
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
                    count += 1

print(f"✅ {count} relaciones letrado-parte procesadas → etl_letrados.csv")

# === ETL Resoluciones ===
print("\n📥 Procesando resoluciones...")
with open("4_1_resoluciones.csv", newline="", encoding="utf-8") as f_in, \
     open("etl_resoluciones.csv", "w", newline="", encoding="utf-8") as f_out:

    reader = csv.DictReader(f_in)
    fieldnames_res = ["numero_expediente", "fecha", "nombre", "link"]
    writer = csv.DictWriter(f_out, fieldnames=fieldnames_res)
    writer.writeheader()

    resoluciones_seen = set()
    count = 0

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
            count += 1

print(f"✅ {count} resoluciones procesadas → etl_resoluciones.csv")

# === ETL Fueros (nuevo) ===
print("\n📥 Extrayendo fueros únicos...")
fueros_dict = {}
fuero_id_counter = 1

with open("etl_expedientes.csv", newline="", encoding="utf-8") as f_in:
    reader = csv.DictReader(f_in)
    for row in reader:
        fuero = row["fuero"].strip()
        if fuero and fuero not in fueros_dict:
            fueros_dict[fuero] = fuero_id_counter
            fuero_id_counter += 1

with open("etl_fueros.csv", "w", newline="", encoding="utf-8") as f_out:
    writer = csv.DictWriter(f_out, fieldnames=["fuero_id", "nombre"])
    writer.writeheader()
    for fuero, fid in fueros_dict.items():
        writer.writerow({"fuero_id": fid, "nombre": fuero})

print(f"✅ {len(fueros_dict)} fueros únicos → etl_fueros.csv")

# === ETL Jurisdicciones (nuevo) ===
print("\n📥 Extrayendo jurisdicciones únicas...")
jurisdicciones_dict = {}
jurisdiccion_id_counter = 1

with open("etl_expedientes.csv", newline="", encoding="utf-8") as f_in:
    reader = csv.DictReader(f_in)
    for row in reader:
        jurisdiccion = row["jurisdiccion"].strip()
        if jurisdiccion and jurisdiccion not in jurisdicciones_dict:
            ambito = jurisdiccion  # "Federal" o "Nacional"
            jurisdicciones_dict[jurisdiccion] = {
                'id': jurisdiccion_id_counter,
                'ambito': ambito,
                'provincia': None,
                'departamento': 'Comodoro Py'  # Puedes ajustar esto
            }
            jurisdiccion_id_counter += 1

with open("etl_jurisdicciones.csv", "w", newline="", encoding="utf-8") as f_out:
    writer = csv.DictWriter(f_out, fieldnames=["jurisdiccion_id", "ambito", "provincia", "departamento_judicial"])
    writer.writeheader()
    for jdata in jurisdicciones_dict.values():
        writer.writerow({
            "jurisdiccion_id": jdata['id'],
            "ambito": jdata['ambito'],
            "provincia": jdata['provincia'],
            "departamento_judicial": jdata['departamento']
        })

print(f"✅ {len(jurisdicciones_dict)} jurisdicciones únicas → etl_jurisdicciones.csv")

# === ETL Tribunales (mejorado con nueva estructura) ===
print("\n📥 Extrayendo tribunales...")
tribunales_dict = {}
tribunal_id_counter = 1

with open("etl_expedientes.csv", newline="", encoding="utf-8") as f_in:
    reader = csv.DictReader(f_in)
    for row in reader:
        nombre = row["tribunal"].strip()
        fuero = row["fuero"].strip()
        jurisdiccion = row["jurisdiccion"].strip()
        
        if nombre and nombre not in tribunales_dict:
            tribunales_dict[nombre] = {
                'tribunal_id': tribunal_id_counter,
                'nombre': nombre,
                'instancia': 'Primera Instancia',  # Puedes inferir esto mejor
                'domicilio_sede': 'Comodoro Py 2002, CABA',  # Ajustar según datos
                'contacto': None,
                'jurisdiccion_id': jurisdicciones_dict.get(jurisdiccion, {}).get('id', 1),
                'fuero': fuero
            }
            tribunal_id_counter += 1

with open("etl_tribunales.csv", "w", newline="", encoding="utf-8") as f_out:
    writer = csv.DictWriter(f_out, fieldnames=[
        "tribunal_id", "nombre", "instancia", "domicilio_sede", "contacto", "jurisdiccion_id", "fuero"
    ])
    writer.writeheader()
    for tdata in tribunales_dict.values():
        writer.writerow(tdata)

print(f"✅ {len(tribunales_dict)} tribunales únicos → etl_tribunales.csv")

# ============================================================
# === ETL JUECES Y TRIBUNALES (desde scraper_jueces.csv) ===
# ============================================================

def limpiar_texto(texto):
    """Limpia espacios extras y caracteres especiales"""
    if not texto or texto == '':
        return None
    return ' '.join(str(texto).split())

def parsear_path_jueces(path):
    """Parsea el campo path para extraer fuero y contexto jurisdiccional"""
    if not path:
        return None, None
    
    partes = [p.strip() for p in str(path).split('>')]
    
    fuero = partes[0] if len(partes) > 0 else None
    contexto = partes[1] if len(partes) > 1 else None
    
    return fuero, contexto

def parsear_detalle_jueces(detalle):
    """Parsea el campo detalle y extrae dirección, localidad, teléfono y email"""
    if not detalle:
        return {}
    
    partes = [p.strip() for p in str(detalle).split('|')]
    
    return {
        'direccion': limpiar_texto(partes[0]) if len(partes) > 0 else None,
        'localidad': limpiar_texto(partes[1]) if len(partes) > 1 else None,
        'telefono': limpiar_texto(partes[2]) if len(partes) > 2 else None,
        'email': limpiar_texto(partes[3]) if len(partes) > 3 else None
    }

def parsear_responsables_jueces(texto_responsables):
    """Parsea el campo responsables y extrae información de cada juez/magistrado"""
    if not texto_responsables:
        return []
    
    magistrados = []
    bloques = texto_responsables.split(';')
    
    for bloque in bloques:
        bloque = bloque.strip()
        if not bloque:
            continue
            
        magistrado = {}
        
        # Extraer nombre
        match_nombre = re.search(r'Nombre:\s*([^|]+?)(?=\s*\||Cargo:|$)', bloque)
        if match_nombre:
            nombre = limpiar_texto(match_nombre.group(1))
            # Limpiar prefijos Dr./Dra.
            nombre = re.sub(r'^(Dr\.|Dra\.)\s*', '', nombre).strip()
            magistrado['nombre'] = nombre
        
        # Extraer cargo (último encontrado)
        matches_cargo = re.findall(r'Cargo:\s*([^|]+?)(?=\s*\||Tel:|Teléfono:|Email:|Situación:|$)', bloque)
        if matches_cargo:
            cargo = limpiar_texto(matches_cargo[-1])  # Tomar el último
            if 'Tel:' in cargo or 'Email:' in cargo:
                cargo = re.split(r'Tel:|Email:', cargo)[0].strip()
            magistrado['cargo'] = cargo
        
        # Extraer teléfono
        match_telefono = re.search(r'Teléfono:\s*([^|]+?)(?=\s*\||Email:|Cargo:|Situación:|$)', bloque)
        if match_telefono:
            telefono = limpiar_texto(match_telefono.group(1))
            if telefono and not telefono.startswith('Dr'):
                magistrado['telefono'] = telefono
        
        # Extraer email
        match_email = re.search(r'Email:\s*([^|]+?)(?=\s*\||Cargo:|Situación:|$)', bloque)
        if match_email:
            email = limpiar_texto(match_email.group(1))
            if email and '@' in email:
                magistrado['email'] = email
        
        # Extraer situación
        match_situacion = re.search(r'Situación:\s*([^|;]+)', bloque)
        if match_situacion:
            magistrado['situacion'] = limpiar_texto(match_situacion.group(1))
        
        if magistrado and 'nombre' in magistrado:
            magistrados.append(magistrado)
    
    return magistrados

print("\n📥 Procesando jueces y tribunales desde scraper...")
try:
    jueces_nuevos = {}
    tribunales_jueces_dict = {}
    tribunal_juez_counter = 1
    juez_counter = 1
    
    # Leer el CSV de scraper
    with open("scraper_jueces.csv", newline="", encoding="utf-8") as f_in:
        reader = csv.DictReader(f_in)
        
        for row in reader:
            # Parsear información básica
            fuero_path, contexto = parsear_path_jueces(row.get('path'))
            detalle_info = parsear_detalle_jueces(row.get('detalle'))
            
            # Determinar jurisdicción (simplificado)
            if fuero_path and 'TODO EL PAÍS' in fuero_path.upper():
                ambito_juris = 'Nacional'
            else:
                ambito_juris = 'Federal'
            
            # Buscar jurisdiccion_id correspondiente
            jurisdiccion_id = jurisdicciones_dict.get(ambito_juris, {}).get('id', 1)
            
            # Crear/actualizar tribunal
            nombre_tribunal = limpiar_texto(row.get('titulo'))
            
            if nombre_tribunal and nombre_tribunal not in tribunales_dict:
                # Determinar fuero basado en el contexto
                if contexto:
                    if 'PENAL' in contexto.upper():
                        fuero_tribunal = 'Penal Federal'
                    elif 'CIVIL' in contexto.upper():
                        fuero_tribunal = 'Civil'
                    elif 'COMERCIAL' in contexto.upper():
                        fuero_tribunal = 'Comercial'
                    elif 'TRABAJO' in contexto.upper() or 'LABORAL' in contexto.upper():
                        fuero_tribunal = 'Laboral'
                    else:
                        fuero_tribunal = 'Penal Federal'  # Default
                else:
                    fuero_tribunal = 'Penal Federal'
                
                # Construir contacto
                contacto_parts = []
                if detalle_info.get('telefono'):
                    contacto_parts.append(f"Tel: {detalle_info['telefono']}")
                if detalle_info.get('email'):
                    contacto_parts.append(f"Email: {detalle_info['email']}")
                contacto_str = ' | '.join(contacto_parts) if contacto_parts else None
                
                tribunales_dict[nombre_tribunal] = {
                    'tribunal_id': tribunal_id_counter,
                    'nombre': nombre_tribunal,
                    'instancia': f"Nivel {row.get('nivel', 'N/A')}",
                    'domicilio_sede': detalle_info.get('direccion'),
                    'contacto': contacto_str,
                    'jurisdiccion_id': jurisdiccion_id,
                    'fuero': fuero_tribunal
                }
                tribunal_id_actual = tribunal_id_counter
                tribunal_id_counter += 1
            else:
                tribunal_id_actual = tribunales_dict[nombre_tribunal]['tribunal_id']
            
            # Procesar jueces/magistrados
            magistrados = parsear_responsables_jueces(row.get('responsables'))
            
            for mag_data in magistrados:
                nombre_juez = mag_data.get('nombre')
                if not nombre_juez:
                    continue
                
                # Agregar juez si no existe
                if nombre_juez not in jueces_nuevos:
                    jueces_nuevos[nombre_juez] = {
                        'juez_id': juez_counter,
                        'nombre': nombre_juez,
                        'email': mag_data.get('email'),
                        'telefono': mag_data.get('telefono')
                    }
                    juez_counter += 1
                
                # Crear relación tribunal-juez
                juez_id = jueces_nuevos[nombre_juez]['juez_id']
                key = (tribunal_id_actual, juez_id)
                
                if key not in tribunales_jueces_dict:
                    tribunales_jueces_dict[key] = {
                        'tribunal_id': tribunal_id_actual,
                        'juez_id': juez_id,
                        'cargo': mag_data.get('cargo'),
                        'situacion': mag_data.get('situacion', 'Efectivo')
                    }
    
    # Escribir CSV de jueces
    with open("etl_jueces.csv", "w", newline="", encoding="utf-8") as f_out:
        writer = csv.DictWriter(f_out, fieldnames=["juez_id", "nombre", "email", "telefono"])
        writer.writeheader()
        for juez_data in jueces_nuevos.values():
            writer.writerow(juez_data)
    
    print(f"✅ {len(jueces_nuevos)} jueces procesados → etl_jueces.csv")
    
    # Escribir CSV de tribunal_juez
    with open("etl_tribunal_juez.csv", "w", newline="", encoding="utf-8") as f_out:
        writer = csv.DictWriter(f_out, fieldnames=["tribunal_id", "juez_id", "cargo", "situacion"])
        writer.writeheader()
        for rel_data in tribunales_jueces_dict.values():
            writer.writerow(rel_data)
    
    print(f"✅ {len(tribunales_jueces_dict)} relaciones tribunal-juez → etl_tribunal_juez.csv")
    
    # Reescribir etl_tribunales.csv con los tribunales actualizados
    with open("etl_tribunales.csv", "w", newline="", encoding="utf-8") as f_out:
        writer = csv.DictWriter(f_out, fieldnames=[
            "tribunal_id", "nombre", "instancia", "domicilio_sede", "contacto", "jurisdiccion_id", "fuero"
        ])
        writer.writeheader()
        for tdata in tribunales_dict.values():
            writer.writerow(tdata)
    
    print(f"✅ {len(tribunales_dict)} tribunales actualizados → etl_tribunales.csv")

except FileNotFoundError:
    print("⚠️  Archivo scraper_jueces.csv no encontrado, saltando procesamiento de jueces...")
except Exception as e:
    print(f"❌ Error procesando jueces: {e}")
    import traceback
    traceback.print_exc()

# === Resumen final ===
print("\n" + "=" * 60)
print("🎉 ETL COMPLETADO EXITOSAMENTE")
print("=" * 60)
print("\n📊 Archivos generados:")
print("   • etl_expedientes.csv")
print("   • etl_partes.csv")
print("   • etl_letrados.csv")
print("   • etl_resoluciones.csv")
print("   • etl_fueros.csv")
print("   • etl_jurisdicciones.csv")
print("   • etl_tribunales.csv")
print("\n💡 Siguiente paso: ejecutar el script de carga a PostgreSQL")
print("=" * 60)