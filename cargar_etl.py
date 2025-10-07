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

# ==============================
# CARGA TRIBUNALES
# ==============================
print("📥 Cargando tribunales...")
with open("etl_tribunales.csv", newline="", encoding="utf-8") as f:
    reader = csv.DictReader(f)
    for row in reader:
        cur.execute("""
            INSERT INTO tribunal (nombre, fuero, jurisdiccion_id)
            VALUES (%s, %s, %s)
            ON CONFLICT (nombre) DO UPDATE SET fuero = EXCLUDED.fuero;
        """, (row["nombre"], row["fuero"], 1))  # 👈 siempre jurisdicción Comodoro Py
conn.commit()
print("✅ Tribunales cargados en DB")

# ==============================
# CARGA EXPEDIENTES
# ==============================
print("📥 Cargando expedientes...")
with open("etl_expedientes.csv", newline="", encoding="utf-8") as f:
    reader = csv.DictReader(f)
    for row in reader:
        id_tribunal = None
        if row["tribunal"]:
            cur.execute("SELECT tribunal_id FROM tribunal WHERE nombre = %s", (row["tribunal"],))
            res = cur.fetchone()
            if res:
                id_tribunal = res[0]

        cur.execute("""
            INSERT INTO expediente (
                numero_expediente, caratula, jurisdiccion,
                tribunal, estado, fecha_inicio, fecha_ultimo_movimiento,
                camara_origen, ano_inicio, delitos, fiscal, fiscalia, id_tribunal
            )
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            ON CONFLICT (numero_expediente) DO NOTHING;
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
            id_tribunal
        ))
conn.commit()
print("✅ Expedientes cargados en DB")

# ==============================
# CARGA PARTES
# ==============================
print("📥 Cargando partes...")
with open("etl_partes.csv", newline="", encoding="utf-8") as f:
    reader = csv.DictReader(f)
    for row in reader:
        cur.execute("""
            INSERT INTO parte (numero_expediente, nombre_razon_social)
            VALUES (%s, %s)
            ON CONFLICT DO NOTHING;
        """, (row["numero_expediente"], row["nombre"]))

        cur.execute("""
            INSERT INTO rol_parte (parte_id, nombre)
            SELECT parte_id, %s FROM parte WHERE numero_expediente = %s AND nombre_razon_social = %s
            ON CONFLICT DO NOTHING;
        """, (row["rol"], row["numero_expediente"], row["nombre"]))
conn.commit()
print("✅ Partes cargadas en DB")

# ==============================
# CARGA LETRADOS
# ==============================
print("📥 Cargando letrados...")
with open("etl_letrados.csv", newline="", encoding="utf-8") as f:
    reader = csv.DictReader(f)
    for row in reader:
        cur.execute("""
            INSERT INTO letrado (nombre)
            VALUES (%s)
            ON CONFLICT (nombre) DO NOTHING;
        """, (row["letrado"],))

        cur.execute("""
            INSERT INTO representacion (numero_expediente, parte_id, letrado_id, rol)
            SELECT %s, p.parte_id, l.letrado_id, NULL
            FROM parte p, letrado l
            WHERE p.numero_expediente = %s AND p.nombre_razon_social = %s AND l.nombre = %s
            ON CONFLICT DO NOTHING;
        """, (
            row["numero_expediente"],
            row["numero_expediente"],
            row["interviniente"],
            row["letrado"]
        ))
conn.commit()
print("✅ Letrados cargados en DB")

# ==============================
# CARGA RESOLUCIONES
# ==============================
print("📥 Cargando resoluciones...")
with open("etl_resoluciones.csv", newline="", encoding="utf-8") as f:
    reader = csv.DictReader(f)
    for row in reader:
        cur.execute("""
            INSERT INTO resolucion (numero_expediente, fecha, nombre, link)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT DO NOTHING;
        """, (
            row["numero_expediente"],
            row["fecha"] if row["fecha"] else None,
            row["nombre"],
            row["link"]
        ))
conn.commit()
print("✅ Resoluciones cargadas en DB")

# ==============================
# CIERRE
# ==============================
cur.close()
conn.close()
print("🎉 Carga completa finalizada")
