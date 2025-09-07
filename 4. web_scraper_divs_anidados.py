import asyncio
from playwright.async_api import async_playwright
from bs4 import BeautifulSoup
import csv
import psycopg2

# ==============================
# Función para guardar en la DB
# ==============================
from datetime import datetime

def guardar_en_db(resultados):
    conn = psycopg2.connect(
        dbname="corrupcion_db",
        user="admin",
        password="td8corrupcion",
        host="localhost",
        port="5432"
    )
    cur = conn.cursor()

    # Insertar un fuero por defecto (si no tienes datos específicos)
    cur.execute("""
        INSERT INTO fuero (nombre) VALUES ('Federal') 
        ON CONFLICT (nombre) DO NOTHING 
        RETURNING fuero_id;
    """)
    row = cur.fetchone()
    fuero_id = row[0] if row else cur.execute("SELECT fuero_id FROM fuero WHERE nombre = 'Federal';") or cur.fetchone()[0]

    # Insertar una jurisdicción por defecto
    cur.execute("""
        INSERT INTO jurisdiccion (ambito) VALUES ('Federal') 
        ON CONFLICT DO NOTHING 
        RETURNING jurisdiccion_id;
    """)
    row = cur.fetchone()
    jurisdiccion_id = row[0] if row else cur.execute("SELECT jurisdiccion_id FROM jurisdiccion WHERE ambito = 'Federal';") or cur.fetchone()[0]

    # Insertar un tribunal por defecto
    cur.execute("""
        INSERT INTO tribunal (nombre, jurisdiccion_id) VALUES ('Tribunal Federal', %s) 
        ON CONFLICT DO NOTHING 
        RETURNING tribunal_id;
    """, (jurisdiccion_id,))
    row = cur.fetchone()
    tribunal_id = row[0] if row else cur.execute("SELECT tribunal_id FROM tribunal WHERE nombre = 'Tribunal Federal';") or cur.fetchone()[0]

    # Insertar un estado procesal por defecto
    cur.execute("""
        INSERT INTO estado_procesal (nombre) VALUES (%s) 
        ON CONFLICT (nombre) DO NOTHING 
        RETURNING estado_procesal_id;
    """, (resultados[0]["Estado"],))
    row = cur.fetchone()
    estado_procesal_id = row[0] if row else cur.execute("SELECT estado_procesal_id FROM estado_procesal WHERE nombre = %s;", (resultados[0]["Estado"],)) or cur.fetchone()[0]

    for r in resultados:
        # Convertir Última actualización a formato de fecha
        try:
            fecha_ultimo_mov = datetime.strptime(r["Última actualización"], "%d/%m/%Y").date()
        except (ValueError, KeyError):
            fecha_ultimo_mov = None  # O usa una fecha por defecto

        # Insertar expediente
        cur.execute("""
            INSERT INTO expediente (numero_expediente, caratula, fecha_inicio, fecha_ultimo_movimiento, fuero_id, tribunal_id, estado_procesal_id)
            VALUES (%s, %s, CURRENT_DATE, %s, %s, %s, %s)
            ON CONFLICT (numero_expediente) DO NOTHING
            RETURNING numero_expediente;
        """, (
            r["Expediente"],
            r["Carátula"],
            fecha_ultimo_mov,
            fuero_id,
            tribunal_id,
            estado_procesal_id
        ))

        # Verificar si se insertó el expediente
        row = cur.fetchone()
        if not row:
            continue  # El expediente ya existía, pasar al siguiente

        # Insertar imputados y letrados
        for (imp, letrs) in r.get("__imputados__", []):
            # Insertar parte (imputado)
            cur.execute("""
                INSERT INTO parte (documento_cuit, numero_expediente, tipo_persona, nombre_razon_social)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (documento_cuit, numero_expediente) DO NOTHING
                RETURNING documento_cuit;
            """, (imp, r["Expediente"], "fisica", imp))  # Asumimos tipo_persona='fisica' y documento_cuit=nombre por falta de datos
            row_p = cur.fetchone()
            if not row_p:
                continue

            # Insertar rol_parte
            cur.execute("""
                INSERT INTO rol_parte (numero_expediente, documento_cuit, nombre)
                VALUES (%s, %s, %s)
                ON CONFLICT DO NOTHING;
            """, (r["Expediente"], imp, "imputado"))

            # Insertar letrados
            for l in letrs:
                cur.execute("""
                    INSERT INTO letrado (nombre)
                    VALUES (%s)
                    ON CONFLICT (nombre) DO NOTHING
                    RETURNING letrado_id;
                """, (l,))
                row_l = cur.fetchone()
                if row_l:
                    letrado_id = row_l[0]
                else:
                    cur.execute("SELECT letrado_id FROM letrado WHERE nombre = %s;", (l,))
                    letrado_id = cur.fetchone()[0]

                # Nota: Aquí falta la inserción en parte_letrado porque la tabla no existe en init.sql
                # Si quieres usar parte_letrado, debes crear la tabla (ver más abajo)

    conn.commit()
    cur.close()
    conn.close()

# ==============================
# Scraper
# ==============================
async def run():
    url = "https://www.csjn.gov.ar/tribunales-federales-nacionales/causas-de-corrupcion.html"
    resultados = []
    vistos = set()

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        page = await browser.new_page()
        await page.goto(url)

        pagina = 1
        while True:
            print(f"Procesando página {pagina}...")
            await page.wait_for_selector("div.result")
            content = await page.content()
            soup = BeautifulSoup(content, "html.parser")
            bloques = soup.find_all("div", class_="result")

            if not bloques:
                print("No se encontraron más expedientes.")
                break

            for bloque in bloques:
                info_items = bloque.find("ul", class_="info").find_all("li")
                datos = {}
                imputados = []
                resoluciones = []

                for item in info_items:
                    etiqueta = item.find("span")
                    if not etiqueta:
                        continue
                    clave = etiqueta.get_text(strip=True).replace(":", "")
                    valor = item.get_text(strip=True).replace(etiqueta.get_text(strip=True), "").strip()
                    datos[clave] = valor

                # Intervinientes
                panel_interv = bloque.select_one("div.ver-todos-panel")
                if panel_interv:
                    li_imputados = panel_interv.select("div.item-especial-largo-2 ul li")
                    for li in li_imputados:
                        imputado_nombre = "".join(li.find_all(string=True, recursive=False)).strip()
                        letrados_imputado = [l.get_text(strip=True) for l in li.select("div.item")]
                        imputados.append((imputado_nombre, letrados_imputado))

                identificador = datos.get("Expediente")
                if identificador and identificador not in vistos:
                    datos["__imputados__"] = imputados
                    resultados.append(datos)
                    vistos.add(identificador)

            # Siguiente página
            try:
                boton_siguiente = await page.query_selector("a.page-link:has-text('Siguiente')")
                if not boton_siguiente:
                    break
                await boton_siguiente.click()
                await page.wait_for_timeout(2000)
                pagina += 1
            except Exception:
                break

        await browser.close()

    # Guardar CSV (opcional)
    with open("expedientes.csv", "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=["Expediente", "Carátula", "Estado", "Última actualización"])
        writer.writeheader()
        for r in resultados:
            fila = {k: r.get(k, "") for k in ["Expediente", "Carátula", "Estado", "Última actualización"]}
            writer.writerow(fila)

    # Guardar en DB
    guardar_en_db(resultados)
    print("✅ Datos guardados en expedientes.csv y en PostgreSQL")


if __name__ == "__main__":
    asyncio.run(run())
