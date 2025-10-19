import psycopg2
import csv

# ==============================
# CONEXIÓN
# ==============================
conn = psycopg2.connect(
    dbname="corrupcion_db",
    user="admin",
    password="td8corrupcion",
    host="localhost",
    port="5432"
)
cur = conn.cursor()

print("=" * 60)
print("🔌 Conectado a PostgreSQL: corrupcion_db")
print("=" * 60)

# ==============================
# CARGA FUEROS
# ==============================
print("\n📥 Cargando fueros...")
try:
    with open("etl_fueros.csv", newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        count = 0
        for row in reader:
            cur.execute("""
                INSERT INTO fuero (fuero_id, nombre)
                VALUES (%s, %s)
                ON CONFLICT (nombre) DO UPDATE SET nombre = EXCLUDED.nombre;
            """, (row["fuero_id"], row["nombre"]))
            count += 1
    conn.commit()
    print(f"✅ {count} fueros cargados")
except FileNotFoundError:
    print("⚠️  Archivo etl_fueros.csv no encontrado, saltando...")
except Exception as e:
    print(f"❌ Error cargando fueros: {e}")
    conn.rollback()

# ==============================
# CARGA JURISDICCIONES
# ==============================
print("\n📥 Cargando jurisdicciones...")
try:
    with open("etl_jurisdicciones.csv", newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        count = 0
        for row in reader:
            cur.execute("""
                INSERT INTO jurisdiccion (jurisdiccion_id, ambito, provincia, departamento_judicial)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (jurisdiccion_id) DO UPDATE SET
                    ambito = EXCLUDED.ambito,
                    provincia = EXCLUDED.provincia,
                    departamento_judicial = EXCLUDED.departamento_judicial;
            """, (
                row["jurisdiccion_id"],
                row["ambito"],
                row["provincia"] if row["provincia"] else None,
                row["departamento_judicial"] if row["departamento_judicial"] else None
            ))
            count += 1
    conn.commit()
    print(f"✅ {count} jurisdicciones cargadas")
except FileNotFoundError:
    print("⚠️  Archivo etl_jurisdicciones.csv no encontrado, saltando...")
except Exception as e:
    print(f"❌ Error cargando jurisdicciones: {e}")
    conn.rollback()

# ==============================
# CARGA ESTADOS PROCESALES
# ==============================
print("\n📥 Creando estados procesales...")
try:
    estados = [
        ('En trámite', 'Sustanciación'),
        ('Terminadas', 'Finalizada'),
        ('Archivado', 'Finalizada'),
        ('Elevado', 'Sustanciación'),
        ('Suspendido', 'Suspendida')
    ]
    count = 0
    for nombre, etapa in estados:
        cur.execute("""
            INSERT INTO estado_procesal (nombre, etapa)
            VALUES (%s, %s)
            ON CONFLICT (nombre) DO NOTHING;
        """, (nombre, etapa))
        count += 1
    conn.commit()
    print(f"✅ {count} estados procesales verificados")
except Exception as e:
    print(f"❌ Error creando estados: {e}")
    conn.rollback()

# ==============================
# CARGA TRIBUNALES
# ==============================
print("\n📥 Cargando tribunales...")
try:
    with open("etl_tribunales.csv", newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        count = 0
        for row in reader:
            fuero = row.get("fuero") or "Desconocido"

            cur.execute("""
                INSERT INTO tribunal (tribunal_id, nombre, instancia, domicilio_sede, contacto, fuero, jurisdiccion_id)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (tribunal_id) DO UPDATE SET
                    nombre = EXCLUDED.nombre,
                    instancia = EXCLUDED.instancia,
                    domicilio_sede = EXCLUDED.domicilio_sede,
                    contacto = EXCLUDED.contacto,
                    fuero = EXCLUDED.fuero,
                    jurisdiccion_id = EXCLUDED.jurisdiccion_id;
            """, (
                row["tribunal_id"],
                row["nombre"],
                row.get("instancia"),
                row.get("domicilio_sede"),
                row.get("contacto"),
                fuero,
                row["jurisdiccion_id"]
            ))
            count += 1
    conn.commit()
    print(f"✅ {count} tribunales cargados")
except FileNotFoundError:
    print("⚠️  Archivo etl_tribunales.csv no encontrado, saltando...")
except Exception as e:
    print(f"❌ Error cargando tribunales: {e}")
    conn.rollback()

# ==============================
# CARGA EXPEDIENTES
# ==============================
print("\n📥 Cargando expedientes...")
try:
    with open("etl_expedientes.csv", newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        count = 0
        for row in reader:
            # Buscar tribunal_id (opcional)
            tribunal_id = None
            if row.get("tribunal"):
                cur.execute("SELECT tribunal_id FROM tribunal WHERE nombre = %s", (row["tribunal"],))
                res = cur.fetchone()
                if res:
                    tribunal_id = res[0]

            cur.execute("""
                INSERT INTO expediente (
                    numero_expediente, caratula, jurisdiccion, tribunal, estado,
                    fecha_inicio, fecha_ultimo_movimiento, camara_origen, ano_inicio,
                    delitos, fiscal, fiscalia, id_tribunal
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (numero_expediente) DO UPDATE SET
                    caratula = EXCLUDED.caratula,
                    fecha_ultimo_movimiento = EXCLUDED.fecha_ultimo_movimiento,
                    estado = EXCLUDED.estado,
                    tribunal = EXCLUDED.tribunal;
            """, (
                row["numero_expediente"],
                row["caratula"],
                row["jurisdiccion"],
                row["tribunal"],
                row["estado"],
                row["fecha_inicio"] if row["fecha_inicio"] else None,
                row["fecha_ultimo_movimiento"] if row["fecha_ultimo_movimiento"] else None,
                row["camara_origen"],
                row["ano_inicio"],
                row["delitos"],
                row["fiscal"],
                row["fiscalia"],
                tribunal_id
            ))
            count += 1
    conn.commit()
    print(f"✅ {count} expedientes cargados")
except FileNotFoundError:
    print("⚠️  Archivo etl_expedientes.csv no encontrado")
except Exception as e:
    print(f"❌ Error cargando expedientes: {e}")
    conn.rollback()

# ==============================
# CARGA PARTES
# ==============================
print("\n📥 Cargando partes e intervinientes...")
try:
    with open("etl_partes.csv", newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        count_partes = 0
        count_roles = 0
        
        for row in reader:
            cur.execute("""
                INSERT INTO parte (numero_expediente, nombre_razon_social, tipo_persona)
                VALUES (%s, %s, %s)
                ON CONFLICT DO NOTHING
                RETURNING parte_id;
            """, (row["numero_expediente"], row["nombre"], 'fisica'))
            
            res = cur.fetchone()
            if res:
                parte_id = res[0]
                count_partes += 1
            else:
                cur.execute("""
                    SELECT parte_id FROM parte 
                    WHERE numero_expediente = %s AND nombre_razon_social = %s;
                """, (row["numero_expediente"], row["nombre"]))
                res = cur.fetchone()
                if res:
                    parte_id = res[0]
                else:
                    continue
            
            cur.execute("""
                INSERT INTO rol_parte (parte_id, nombre)
                VALUES (%s, %s)
                ON CONFLICT DO NOTHING;
            """, (parte_id, row["rol"]))
            count_roles += 1
    
    conn.commit()
    print(f"✅ {count_partes} partes y {count_roles} roles cargados")
except FileNotFoundError:
    print("⚠️  Archivo etl_partes.csv no encontrado")
except Exception as e:
    print(f"❌ Error cargando partes: {e}")
    conn.rollback()

# ==============================
# CARGA LETRADOS
# ==============================
print("\n📥 Cargando letrados y representaciones...")
try:
    with open("etl_letrados.csv", newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        count_letrados = 0
        count_repr = 0
        
        for row in reader:
            cur.execute("""
                INSERT INTO letrado (nombre)
                VALUES (%s)
                ON CONFLICT (nombre) DO NOTHING
                RETURNING letrado_id;
            """, (row["letrado"],))
            
            res = cur.fetchone()
            if res:
                count_letrados += 1
            
            cur.execute("SELECT letrado_id FROM letrado WHERE nombre = %s", (row["letrado"],))
            letrado_id = cur.fetchone()[0]
            
            cur.execute("""
                SELECT parte_id FROM parte 
                WHERE numero_expediente = %s AND nombre_razon_social = %s;
            """, (row["numero_expediente"], row["interviniente"]))
            
            res = cur.fetchone()
            if res:
                parte_id = res[0]
                cur.execute("""
                    INSERT INTO representacion (numero_expediente, parte_id, letrado_id, rol)
                    VALUES (%s, %s, %s, %s)
                    ON CONFLICT DO NOTHING;
                """, (row["numero_expediente"], parte_id, letrado_id, None))
                count_repr += 1
    
    conn.commit()
    print(f"✅ {count_letrados} letrados nuevos y {count_repr} representaciones cargadas")
except FileNotFoundError:
    print("⚠️  Archivo etl_letrados.csv no encontrado")
except Exception as e:
    print(f"❌ Error cargando letrados: {e}")
    conn.rollback()

# ==============================
# CARGA RESOLUCIONES
# ==============================
print("\n📥 Cargando resoluciones...")
try:
    with open("etl_resoluciones.csv", newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        count = 0
        for row in reader:
            cur.execute("""
                INSERT INTO resolucion (numero_expediente, nombre, fecha, link)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT DO NOTHING;
            """, (
                row["numero_expediente"],
                row["nombre"],
                row["fecha"] if row["fecha"] else None,
                row["link"]
            ))
            count += 1
    conn.commit()
    print(f"✅ {count} resoluciones cargadas")
except FileNotFoundError:
    print("⚠️  Archivo etl_resoluciones.csv no encontrado")
except Exception as e:
    print(f"❌ Error cargando resoluciones: {e}")
    conn.rollback()

# ==============================
# CARGA RADICACIONES (NUEVO)
# ==============================
print("\n📥 Cargando radicaciones...")
try:
    with open("etl_radicaciones.csv", newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        count = 0
        for row in reader:
            cur.execute("""
                INSERT INTO radicacion (
                    numero_expediente, 
                    orden, 
                    fecha_radicacion, 
                    tribunal, 
                    fiscal_nombre, 
                    fiscalia
                )
                VALUES (%s, %s, %s, %s, %s, %s)
                ON CONFLICT (numero_expediente, orden) DO UPDATE SET
                    fecha_radicacion = EXCLUDED.fecha_radicacion,
                    tribunal = EXCLUDED.tribunal,
                    fiscal_nombre = EXCLUDED.fiscal_nombre,
                    fiscalia = EXCLUDED.fiscalia;
            """, (
                row["numero_expediente"],
                int(row["orden"]) if row["orden"] else None,
                row["fecha_radicacion"] if row["fecha_radicacion"] else None,
                row["tribunal"],
                row["fiscal_nombre"],
                row["fiscalia"]
            ))
            count += 1
    conn.commit()
    print(f"✅ {count} radicaciones cargadas")
except FileNotFoundError:
    print("⚠️  Archivo etl_radicaciones.csv no encontrado")
except Exception as e:
    print(f"❌ Error cargando radicaciones: {e}")
    conn.rollback()

# ==============================
# CARGA JUECES
# ==============================
print("\n📥 Cargando jueces...")
try:
    with open("etl_jueces.csv", newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        count = 0
        for row in reader:
            cur.execute("""
                INSERT INTO juez (juez_id, nombre, email, telefono)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (nombre) DO UPDATE SET
                    email = COALESCE(EXCLUDED.email, juez.email),
                    telefono = COALESCE(EXCLUDED.telefono, juez.telefono);
            """, (
                row["juez_id"],
                row["nombre"],
                row.get("email"),
                row.get("telefono")
            ))
            count += 1
    conn.commit()
    print(f"✅ {count} jueces cargados")
except FileNotFoundError:
    print("⚠️  Archivo etl_jueces.csv no encontrado")
except Exception as e:
    print(f"❌ Error cargando jueces: {e}")
    conn.rollback()

# ==============================
# CARGA RELACIÓN TRIBUNAL-JUEZ
# ==============================
print("\n📥 Cargando relaciones tribunal-juez...")
try:
    with open("etl_tribunal_juez.csv", newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        count = 0
        for row in reader:
            cur.execute("""
                INSERT INTO tribunal_juez (tribunal_id, juez_id, cargo, situacion)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (tribunal_id, juez_id) DO UPDATE SET
                    cargo = EXCLUDED.cargo,
                    situacion = EXCLUDED.situacion;
            """, (
                row["tribunal_id"],
                row["juez_id"],
                row.get("cargo"),
                row.get("situacion") or 'Efectivo'
            ))
            count += 1
    conn.commit()
    print(f"✅ {count} relaciones tribunal-juez cargadas")
except FileNotFoundError:
    print("⚠️  Archivo etl_tribunal_juez.csv no encontrado")
except Exception as e:
    print(f"❌ Error cargando relaciones tribunal-juez: {e}")
    conn.rollback()

# ==============================
# CIERRE
# ==============================
cur.close()
conn.close()
print("\n" + "=" * 60)
print("🎉 CARGA COMPLETA FINALIZADA")
print("=" * 60)
print("\n📊 Tablas cargadas:")
print("   • Fueros")
print("   • Jurisdicciones")
print("   • Estados Procesales")
print("   • Tribunales")
print("   • Expedientes")
print("   • Partes e Intervinientes")
print("   • Letrados y Representaciones")
print("   • Resoluciones")
print("   • Radicaciones ← NUEVO")
print("   • Jueces")
print("   • Relaciones Tribunal-Juez")
print("=" * 60)