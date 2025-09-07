import asyncio
from playwright.async_api import async_playwright
from bs4 import BeautifulSoup
import asyncpg
import csv
import warnings
from datetime import datetime

warnings.filterwarnings("ignore", category=FutureWarning)

# === Helpers ===
async def obtener_o_crear_id(conn, tabla, columna, valor, extra=None):
    """
    Devuelve el id de un valor existente o lo inserta si no existe.
    extra = dict con otras columnas necesarias para INSERT.
    """
    if valor is None or str(valor).strip() == "":
        valor = "Desconocido"

    row = await conn.fetchrow(f"SELECT {tabla}_id FROM {tabla} WHERE {columna} = $1", valor)
    if row:
        return row[f"{tabla}_id"]

    campos = [columna]
    valores = [valor]
    placeholders = ["$1"]

    if extra:
        for i, (k, v) in enumerate(extra.items(), start=2):
            campos.append(k)
            valores.append(v)
            placeholders.append(f"${i}")

    sql = f"INSERT INTO {tabla} ({', '.join(campos)}) VALUES ({', '.join(placeholders)}) RETURNING {tabla}_id"
    row = await conn.fetchrow(sql, *valores)
    return row[f"{tabla}_id"]

# === Scraper ===
async def scrape_tab(page, tab_selector, estado_label, container_selector, vistos, resultados):
    await page.click(tab_selector)
    await page.wait_for_selector(f"{container_selector} div.result")
    await page.wait_for_timeout(1500)

    pagina = 1
    while True:
        print(f"Procesando página {pagina} ({estado_label})...")

        content = await page.content()
        soup = BeautifulSoup(content, "html.parser")
        bloques = soup.select(f"{container_selector} div.result")

        if not bloques:
            print("No se encontraron más expedientes.")
            break

        for bloque in bloques:
            info_items = bloque.find("ul", class_="info").find_all("li")
            datos = {}
            imputados = []
            resoluciones = []

            for item in info_items:
                for btn in item.select("div.ver-todos, div.ver-menos, div.ver-todos-2, div.ver-menos-2"):
                    btn.decompose()

            for item in info_items:
                etiqueta = item.find("span")
                if not etiqueta:
                    continue
                clave = etiqueta.get_text(strip=True).replace(":", "")
                if clave == "Carátula":
                    valor = "".join(item.find_all(string=True, recursive=False)).strip()
                    datos[clave] = valor
                else:
                    valor = item.get_text(strip=True).replace(etiqueta.get_text(strip=True), "").strip()
                    datos[clave] = valor

            radicacion_div = bloque.select_one("div.item-especial-largo.soy-first-item-largo")
            if radicacion_div:
                partes = [
                    radicacion_div.select_one("div.t1a"),
                    radicacion_div.select_one("div.t2a"),
                    radicacion_div.select_one("div.t3a"),
                    radicacion_div.select_one("div.t4a")
                ]
                datos["Radicación del expediente"] = " | ".join([p.get_text(strip=True) for p in partes if p])

            panel_interv = bloque.select_one("div.ver-todos-panel")
            if panel_interv:
                li_imputados = panel_interv.select("div.item-especial-largo-2 ul li")
                for li in li_imputados:
                    imputado_nombre = "".join(li.find_all(string=True, recursive=False)).strip()
                    letrados_imputado = [l.get_text(strip=True) for l in li.select("div.item")]
                    imputados.append((imputado_nombre, letrados_imputado))

            panel_res = bloque.select_one("li:-soup-contains('Resolución/es') div.ver-todos-panel")
            if panel_res:
                resol_items = panel_res.select("div.item")
                for r in resol_items:
                    texto = r.get_text(strip=True)
                    if texto:
                        resoluciones.append(texto)

            claves_interes = ["Expediente", "Carátula", "Delitos", "Radicación del expediente", "Estado", "Última actualización"]
            for clave in claves_interes:
                datos.setdefault(clave, "")

            identificador = datos.get("Expediente")
            if identificador and (identificador, estado_label) not in vistos:
                datos["__imputados__"] = imputados
                datos["__resoluciones__"] = resoluciones
                datos["EstadoSolapa"] = estado_label
                resultados.append(datos)
                vistos.add((identificador, estado_label))

        try:
            boton_siguiente = await page.query_selector(f"{container_selector} a.page-link:has-text('Siguiente')")
            if not boton_siguiente:
                break
            await boton_siguiente.click()
            await page.wait_for_timeout(2000)
            pagina += 1
        except Exception:
            break

# === Guardado en DB ===
async def guardar_en_db(pool, resultados):
    async with pool.acquire() as conn:
        for r in resultados:
            fuero_id = await obtener_o_crear_id(conn, "fuero", "nombre", "Desconocido")
            jurisdiccion_id = await obtener_o_crear_id(conn, "jurisdiccion", "ambito", "Desconocido",
                                                      {"provincia": "Desconocida", "departamento_judicial": "Desconocido"})
            tribunal_id = await obtener_o_crear_id(conn, "tribunal", "nombre", "Desconocido", {"jurisdiccion_id": jurisdiccion_id})
            secretaria_id = await obtener_o_crear_id(conn, "secretaria", "nombre", "Desconocido", {"tribunal_id": tribunal_id})
            estado_procesal_id = await obtener_o_crear_id(conn, "estado_procesal", "nombre", r.get("Estado", "Desconocido"))

            # 1) Insertar expediente SIN tipo_delito
            await conn.execute("""
                INSERT INTO expediente (numero_expediente, caratula, fecha_inicio, fecha_ultimo_movimiento,
                                        fuero_id, tribunal_id, secretaria_id, estado_procesal_id)
                VALUES ($1, $2, CURRENT_DATE, $3, $4, $5, $6, $7)
                ON CONFLICT (numero_expediente) DO NOTHING
            """, r["Expediente"], r["Carátula"], r["Última actualización"] or None,
                 fuero_id, tribunal_id, secretaria_id, estado_procesal_id)

            # 2) Manejo de delitos (pueden venir múltiples separados por coma)
            if r.get("Delitos"):
                delitos = [d.strip() for d in r["Delitos"].split(",") if d.strip()]
                for d in delitos:
                    # Insertar en tipo_delito si no existe
                    await conn.execute("""
                        INSERT INTO tipo_delito (tipo)
                        VALUES ($1)
                        ON CONFLICT (tipo) DO NOTHING
                    """, d)

                    # 3) Insertar relación expediente-delito
                    await conn.execute("""
                        INSERT INTO expediente_delito (numero_expediente, tipo_delito)
                        VALUES ($1, $2)
                        ON CONFLICT DO NOTHING
                    """, r["Expediente"], d)

            # === Partes (imputados) ===
            for imp, letrados in r.get("__imputados__", []):
                await conn.execute("""
                    INSERT INTO parte (documento_cuit, numero_expediente, tipo_persona, nombre_razon_social)
                    VALUES ($1, $2, 'fisica', $3)
                    ON CONFLICT DO NOTHING
                """, "CUIT_DESCONOCIDO", r["Expediente"], imp)

                for l in letrados:
                    letrado_id = await obtener_o_crear_id(conn, "letrado", "nombre", l)
                    # Relacionar parte con letrado (si tenés tabla intermedia parte_letrado)
                    await conn.execute("""
                        INSERT INTO representacion (numero_expediente, parte_id, letrado_id, rol)
                        VALUES ($1, $2, $3, 'defensor')
                        ON CONFLICT DO NOTHING
                    """, r["Expediente"], parte_id, letrado_id)

            # === Resoluciones (las guardamos como plazos por ahora) ===
            for res in r.get("__resoluciones__", []):
                await conn.execute("""
                    INSERT INTO plazo (numero_expediente, tipo, fecha_inicio, fecha_vencimiento, dias_habiles, estado)
                    VALUES ($1, $2, CURRENT_DATE, CURRENT_DATE, 0, 'vigente')
                    ON CONFLICT DO NOTHING
                """, r["Expediente"], res[:95])  # truncar por si es muy largo

# === Exportación a CSV ===
def guardar_csv(resultados):
    # Expedientes
    fieldnames = ["Expediente", "Carátula", "Delitos", "Radicación del expediente", "Estado", "Última actualización", "EstadoSolapa"]
    with open("5_expedientes.csv", "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for r in resultados:
            fila = {k: r.get(k, "") for k in fieldnames}
            writer.writerow(fila)

    # Imputados
    with open("5_imputados.csv", "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["Expediente", "EstadoSolapa", "Imputado"])
        for r in resultados:
            for (imp, _) in r.get("__imputados__", []):
                writer.writerow([r["Expediente"], r["EstadoSolapa"], imp])

    # Letrados
    with open("5_letrados.csv", "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["Expediente", "EstadoSolapa", "Imputado", "Letrado"])
        for r in resultados:
            for (imp, letrs) in r.get("__imputados__", []):
                for l in letrs:
                    writer.writerow([r["Expediente"], r["EstadoSolapa"], imp, l])

    # Resoluciones
    with open("5_resoluciones.csv", "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["Expediente", "EstadoSolapa", "Resolución"])
        for r in resultados:
            for res in r.get("__resoluciones__", []):
                writer.writerow([r["Expediente"], r["EstadoSolapa"], res])

# === Run ===
async def run():
    url = "https://www.csjn.gov.ar/tribunales-federales-nacionales/causas-de-corrupcion.html"
    resultados = []
    vistos = set()

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        page = await browser.new_page()
        await page.goto(url)

        await scrape_tab(page, "#btn-solapa-2", "Terminadas", "#solapa-2", vistos, resultados)
        await scrape_tab(page, "#btn-solapa-1", "En trámite", "#solapa-1", vistos, resultados)

        await browser.close()

    pool = await asyncpg.create_pool(
        user="admin",
        password="td8corrupcion",
        database="corrupcion_db",
        host="localhost",
        port=5432
    )

    await guardar_en_db(pool, resultados)
    await pool.close()
    guardar_csv(resultados)

    print("✅ Datos guardados en la base de datos y en CSVs")

if __name__ == "__main__":
    asyncio.run(run())
