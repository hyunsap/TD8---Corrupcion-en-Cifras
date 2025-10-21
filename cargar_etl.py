import psycopg2
import csv
import os

# ============================================
# Configuración de conexión
# ============================================
DB_CONFIG = {
    "dbname": os.getenv("DB_NAME", "corrupcion_db"),
    "user": os.getenv("DB_USER", "admin"),
    "password": os.getenv("DB_PASSWORD", "td8corrupcion"),
    "host": os.getenv("DB_HOST", "localhost"),
    "port": os.getenv("DB_PORT", "5432"),
}

def conectar_db():
    return psycopg2.connect(**DB_CONFIG)

def parse_nullable(value):
    """Convierte strings vacíos en None"""
    if value is None or str(value).strip() == "":
        return None
    return value

def parse_nullable_date(value):
    """Convierte cadenas vacías a None (para campos DATE)"""
    if not value or str(value).strip() == "":
        return None
    return value

# ============================================
# Funciones de carga
# ============================================

def cargar_fuero(conn):
    print("Cargando fueros...")
    count = 0
    try:
        with conn.cursor() as cur, open("etl_fueros.csv", newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                cur.execute("""
                    INSERT INTO fuero (fuero_id, nombre)
                    VALUES (%s, %s)
                    ON CONFLICT (nombre) DO NOTHING
                """, (parse_nullable(row["fuero_id"]), parse_nullable(row["nombre"])))
                count += 1
        conn.commit()
        print(f"Fueros insertados: {count}")
    except Exception as e:
        conn.rollback()
        print(f"[Error] No se pudo cargar fueros: {e}")

def cargar_jurisdiccion(conn):
    print("Cargando jurisdicciones...")
    count = 0
    try:
        with conn.cursor() as cur, open("etl_jurisdicciones.csv", newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                cur.execute("""
                    INSERT INTO jurisdiccion (jurisdiccion_id, ambito, provincia, departamento_judicial)
                    VALUES (%s, %s, %s, %s)
                    ON CONFLICT (jurisdiccion_id) DO NOTHING
                """, (
                    parse_nullable(row["jurisdiccion_id"]),
                    parse_nullable(row["ambito"]),
                    parse_nullable(row["provincia"]),
                    parse_nullable(row["departamento_judicial"])
                ))
                count += 1
        conn.commit()
        print(f"Jurisdicciones insertadas: {count}")
    except Exception as e:
        conn.rollback()
        print(f"[Error] No se pudo cargar jurisdicciones: {e}")

def cargar_tribunal(conn):
    print("Cargando tribunales...")
    count = 0
    try:
        with conn.cursor() as cur, open("etl_tribunales.csv", newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                cur.execute("""
                    INSERT INTO tribunal (
                        tribunal_id, nombre, instancia, domicilio_sede,
                        contacto, jurisdiccion_id, fuero
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (nombre) DO NOTHING
                """, (
                    parse_nullable(row["tribunal_id"]),
                    parse_nullable(row["nombre"]),
                    parse_nullable(row["instancia"]),
                    parse_nullable(row["domicilio_sede"]),
                    parse_nullable(row["contacto"]),
                    parse_nullable(row["jurisdiccion_id"]),
                    parse_nullable(row["fuero"])
                ))
                count += 1
        conn.commit()
        print(f"Tribunales insertados: {count}")
    except Exception as e:
        conn.rollback()
        print(f"[Error] No se pudo cargar tribunales: {e}")

def cargar_expediente(conn):
    print("Cargando expedientes...")
    count = 0
    try:
        with conn.cursor() as cur, open("etl_expedientes.csv", newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                cur.execute("""
                    INSERT INTO expediente (
                        numero_expediente, caratula, jurisdiccion, tribunal,
                        estado_procesal_id, fecha_inicio, fecha_ultimo_movimiento,
                        camara_origen, ano_inicio, delitos, fiscal, fiscalia
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (numero_expediente) DO NOTHING
                """, (
                    parse_nullable(row["numero_expediente"]),
                    parse_nullable(row["caratula"]),
                    parse_nullable(row["jurisdiccion"]),
                    parse_nullable(row["tribunal"]),
                    parse_nullable(row["estado_procesal_id"]),
                    parse_nullable_date(row["fecha_inicio"]),
                    parse_nullable_date(row["fecha_ultimo_movimiento"]),
                    parse_nullable(row["camara_origen"]),
                    parse_nullable(row["ano_inicio"]),
                    parse_nullable(row["delitos"]),
                    parse_nullable(row["fiscal"]),
                    parse_nullable(row["fiscalia"])
                ))
                count += 1
        conn.commit()
        print(f"Expedientes insertados: {count}")
    except Exception as e:
        conn.rollback()
        print(f"[Error] No se pudo cargar expedientes: {e}")

def cargar_parte_y_rol(conn):
    print("Cargando partes y roles...")
    parte_count = rol_count = 0
    try:
        with conn.cursor() as cur, open("etl_partes.csv", newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                cur.execute("""
                    INSERT INTO parte (numero_expediente, nombre_razon_social)
                    VALUES (%s, %s)
                    RETURNING parte_id
                """, (parse_nullable(row["numero_expediente"]), parse_nullable(row["nombre"])))
                parte_id = None
                try:
                    parte_id = cur.fetchone()[0]
                except:
                    pass
                parte_count += 1

                if parte_id and row.get("rol"):
                    cur.execute("""
                        INSERT INTO rol_parte (parte_id, nombre)
                        VALUES (%s, %s)
                        ON CONFLICT DO NOTHING
                    """, (parte_id, parse_nullable(row["rol"])))
                    rol_count += 1
        conn.commit()
        print(f"Partes insertadas: {parte_count}, Roles insertados: {rol_count}")
    except Exception as e:
        conn.rollback()
        print(f"[Error] No se pudo cargar partes/roles: {e}")

def cargar_letrado(conn):
    print("Cargando letrados...")
    count = 0
    try:
        with conn.cursor() as cur, open("etl_letrados.csv", newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                nombre = parse_nullable(row.get("letrado"))
                if not nombre:
                    continue
                cur.execute("""
                    INSERT INTO letrado (nombre)
                    VALUES (%s)
                    ON CONFLICT (nombre) DO NOTHING
                """, (nombre,))
                count += 1
        conn.commit()
        print(f"Letrados insertados: {count}")
    except Exception as e:
        conn.rollback()
        print(f"[Error] No se pudo cargar letrados: {e}")

def cargar_representacion(conn):
    print("Cargando representaciones...")
    count = 0
    try:
        with conn.cursor() as cur, open("etl_representaciones.csv", newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                cur.execute("SELECT parte_id FROM parte WHERE nombre_razon_social = %s AND numero_expediente = %s",
                            (parse_nullable(row["nombre_parte"]), parse_nullable(row["numero_expediente"])))
                parte_id = cur.fetchone()
                cur.execute("SELECT letrado_id FROM letrado WHERE nombre = %s",
                            (parse_nullable(row["letrado"]),))
                letrado_id = cur.fetchone()

                if parte_id and letrado_id:
                    cur.execute("""
                        INSERT INTO representacion (numero_expediente, parte_id, letrado_id, rol)
                        VALUES (%s, %s, %s, %s)
                        ON CONFLICT DO NOTHING
                    """, (
                        parse_nullable(row["numero_expediente"]),
                        parte_id[0],
                        letrado_id[0],
                        parse_nullable(row.get("rol"))
                    ))
                    count += 1
        conn.commit()
        print(f"Representaciones insertadas: {count}")
    except Exception as e:
        conn.rollback()
        print(f"[Error] No se pudo cargar representaciones: {e}")

def cargar_resolucion(conn):
    print("Cargando resoluciones...")
    count = 0
    try:
        with conn.cursor() as cur, open("etl_resoluciones.csv", newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                cur.execute("""
                    INSERT INTO resolucion (numero_expediente, fecha, nombre, link)
                    VALUES (%s, %s, %s, %s)
                    ON CONFLICT DO NOTHING
                """, (
                    parse_nullable(row["numero_expediente"]),
                    parse_nullable_date(row["fecha"]),
                    parse_nullable(row["nombre"]),
                    parse_nullable(row["link"])
                ))
                count += 1
        conn.commit()
        print(f"Resoluciones insertadas: {count}")
    except Exception as e:
        conn.rollback()
        print(f"[Error] No se pudo cargar resoluciones: {e}")

def cargar_radicacion(conn):
    print("Cargando radicaciones...")
    count = 0
    try:
        with conn.cursor() as cur, open("etl_radicaciones.csv", newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                cur.execute("""
                    INSERT INTO radicacion (
                        numero_expediente, orden, fecha_radicacion,
                        tribunal, fiscal_nombre, fiscalia
                    )
                    VALUES (%s, %s, %s, %s, %s, %s)
                    ON CONFLICT (numero_expediente, orden) DO NOTHING
                """, (
                    parse_nullable(row["numero_expediente"]),
                    parse_nullable(row["orden"]),
                    parse_nullable_date(row["fecha_radicacion"]),
                    parse_nullable(row["tribunal"]),
                    parse_nullable(row["fiscal_nombre"]),
                    parse_nullable(row["fiscalia"])
                ))
                count += 1
        conn.commit()
        print(f"Radicaciones insertadas: {count}")
    except Exception as e:
        conn.rollback()
        print(f"[Error] No se pudo cargar radicaciones: {e}")

def cargar_juez(conn):
    print("Cargando jueces...")
    count = 0
    try:
        with conn.cursor() as cur, open("etl_jueces.csv", newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                cur.execute("""
                    INSERT INTO juez (juez_id, nombre, email, telefono)
                    VALUES (%s, %s, %s, %s)
                    ON CONFLICT (nombre) DO NOTHING
                """, (
                    parse_nullable(row["juez_id"]),
                    parse_nullable(row["nombre"]),
                    parse_nullable(row["email"]),
                    parse_nullable(row["telefono"])
                ))
                count += 1
        conn.commit()
        print(f"Jueces insertados: {count}")
    except Exception as e:
        conn.rollback()
        print(f"[Error] No se pudo cargar jueces: {e}")

def cargar_tribunal_juez(conn):
    print("Cargando relaciones tribunal-juez...")
    count = 0
    try:
        with conn.cursor() as cur, open("etl_tribunal_juez.csv", newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                cur.execute("""
                    INSERT INTO tribunal_juez (tribunal_id, juez_id, cargo, situacion)
                    VALUES (%s, %s, %s, %s)
                    ON CONFLICT DO NOTHING
                """, (
                    parse_nullable(row["tribunal_id"]),
                    parse_nullable(row["juez_id"]),
                    parse_nullable(row["cargo"]),
                    parse_nullable(row["situacion"])
                ))
                count += 1
        conn.commit()
        print(f"Relaciones tribunal-juez insertadas: {count}")
    except Exception as e:
        conn.rollback()
        print(f"[Error] No se pudo cargar tribunal-juez: {e}")

# ============================================
# Main
# ============================================

def main():
    print("=== Iniciando carga a base de datos ===")
    conn = conectar_db()
    try:
        cargar_fuero(conn)
        cargar_jurisdiccion(conn)
        cargar_tribunal(conn)
        cargar_expediente(conn)
        cargar_parte_y_rol(conn)
        cargar_letrado(conn)
        cargar_representacion(conn)
        cargar_resolucion(conn)
        cargar_radicacion(conn)
        cargar_juez(conn)
        cargar_tribunal_juez(conn)
        print("=== Carga completa (con manejo de errores) ===")
    finally:
        conn.close()

if __name__ == "__main__":
    main()
