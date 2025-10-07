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

# === Scraper con container_selector ===
async def scrape_tab(page, tab_selector, estado_label, container_selector, vistos, resultados):
    print(f"Iniciando scraping de {estado_label}...")

    await page.click(tab_selector)
    await page.wait_for_timeout(2000)
    await page.wait_for_selector(f"{container_selector} div.result", timeout=10000)

    pagina = 1
    while True:
        print(f"Procesando página {pagina} ({estado_label})...")

        content = await page.content()
        soup = BeautifulSoup(content, "html.parser")
        bloques = soup.select(f"{container_selector} div.result")

        if not bloques:
            print(f"No se encontraron más expedientes en {estado_label}")
            break

        for bloque in bloques:
            try:
                info_items = bloque.find("ul", class_="info")
                if not info_items:
                    continue
                info_items = info_items.find_all("li")

                datos, resoluciones = {}, []

                # limpiar botones
                for item in info_items:
                    for btn in item.select("div.ver-todos, div.ver-menos, div.ver-todos-2, div.ver-menos-2"):
                        btn.decompose()

                # parsear claves
                for item in info_items:
                    etiqueta = item.find("span")
                    if not etiqueta:
                        continue
                    clave = etiqueta.get_text(strip=True).replace(":", "")
                    if clave == "Carátula":
                        valor = "".join(item.find_all(string=True, recursive=False)).strip()
                    else:
                        valor = item.get_text(strip=True).replace(etiqueta.get_text(strip=True), "").strip()
                    datos[clave] = valor

                # radicación
                radicacion_div = bloque.select_one("div.item-especial-largo.soy-first-item-largo")
                if radicacion_div:
                    partes = [
                        radicacion_div.select_one("div.t1a"),
                        radicacion_div.select_one("div.t2a"),
                        radicacion_div.select_one("div.t3a"),
                        radicacion_div.select_one("div.t4a")
                    ]
                    datos["Radicación del expediente"] = " | ".join([p.get_text(strip=True) for p in partes if p])

                # intervinientes
                intervinientes = {"Imputado": [], "Denunciado": [], "Denunciante": [], "Querellante": []}
                try:
                    panel_interv = bloque.select_one("div.ver-todos-panel")
                    if panel_interv:
                        secciones = panel_interv.select("div.item-especial-largo-2")
                        for sec in secciones:
                            titulo = sec.find("div", class_="resalta")
                            if not titulo:
                                continue
                            titulo_txt = titulo.get_text(strip=True).upper()
                            participantes = []

                            for li in sec.select("ul li"):
                                nombre_parts = [
                                    t.strip()
                                    for t in li.find_all(string=True, recursive=False)
                                    if t.strip()
                                ]
                                nombre = " ".join(nombre_parts)

                                letrados_panel = li.select_one("div.ver-todos-panel-2")
                                letrados = []
                                if letrados_panel:
                                    letrados = [l.get_text(strip=True) for l in letrados_panel.select("div.item")]

                                participantes.append((nombre, letrados))

                            if "IMPUTADO" in titulo_txt:
                                intervinientes["Imputado"].extend(participantes)
                            elif "DENUNCIADO" in titulo_txt:
                                intervinientes["Denunciado"].extend(participantes)
                            elif "DENUNCIANTE" in titulo_txt:
                                intervinientes["Denunciante"].extend(participantes)
                            elif "QUERELLANTE" in titulo_txt:
                                intervinientes["Querellante"].extend(participantes)
                except Exception as e:
                    print(f"[WARN] Error parseando intervinientes: {e}")

                # resoluciones
                try:
                    panel_res = bloque.select_one("li:has(span:contains('Resolución/es')) div.ver-todos-panel")
                    if panel_res:
                        for r in panel_res.select("div.item"):
                            texto = r.get_text(strip=True)
                            if texto:
                                resoluciones.append(texto)
                except Exception as e:
                    print(f"[WARN] Error parseando resoluciones: {e}")

                # asegurar claves mínimas
                claves_interes = ["Expediente", "Carátula", "Delitos",
                                  "Radicación del expediente", "Estado", "Última actualización"]
                for clave in claves_interes:
                    datos.setdefault(clave, "")

                identificador = datos.get("Expediente")
                if identificador and (identificador, estado_label) not in vistos:
                    datos["__intervinientes__"] = intervinientes
                    datos["__resoluciones__"] = resoluciones
                    datos["EstadoSolapa"] = estado_label
                    resultados.append(datos)
                    vistos.add((identificador, estado_label))

            except Exception as e:
                print(f"[WARN] Error procesando un expediente: {e}")
                continue

        # siguiente página
        try:
            boton_siguiente = await page.query_selector(f"{container_selector} a.page-link:has-text('Siguiente')")
            if not boton_siguiente:
                break
            await boton_siguiente.click()
            await page.wait_for_timeout(2000)
            pagina += 1
        except Exception:
            break

    print(f"Completado {estado_label}. Páginas: {pagina}")

# === Guardado en DB ===
async def guardar_en_db(pool, resultados):
    async with pool.acquire() as conn:
        for r in resultados:
            fuero_id = await obtener_o_crear_id(conn, "fuero", "nombre", "Desconocido")
            jurisdiccion_id = await obtener_o_crear_id(
                conn, "jurisdiccion", "ambito", "Desconocido",
                {"provincia": "Desconocida", "departamento_judicial": "Desconocido"}
            )
            tribunal_id = await obtener_o_crear_id(
                conn, "tribunal", "nombre", "Desconocido",
                {"jurisdiccion_id": jurisdiccion_id}
            )
            secretaria_id = await obtener_o_crear_id(
                conn, "secretaria", "nombre", "Desconocido",
                {"tribunal_id": tribunal_id}
            )
            estado_procesal_id = await obtener_o_crear_id(
                conn, "estado_procesal", "nombre", r.get("Estado", "Desconocido")
            )

            fecha_ultimo_mov = None
            if r.get("Última actualización"):
                try:
                    fecha_ultimo_mov = datetime.strptime(r["Última actualización"], "%d/%m/%Y").date()
                except ValueError:
                    fecha_ultimo_mov = None

            await conn.execute("""
                INSERT INTO expediente (numero_expediente, caratula, fecha_inicio, fecha_ultimo_movimiento,
                                        fuero_id, tribunal_id, secretaria_id, estado_procesal_id)
                VALUES ($1, $2, CURRENT_DATE, $3, $4, $5, $6, $7)
                ON CONFLICT (numero_expediente) DO NOTHING
            """, r["Expediente"], r["Carátula"], fecha_ultimo_mov,
                 fuero_id, tribunal_id, secretaria_id, estado_procesal_id)

            if r.get("Delitos"):
                for d in [d.strip() for d in r["Delitos"].split(",") if d.strip()]:
                    await conn.execute("INSERT INTO tipo_delito (tipo) VALUES ($1) ON CONFLICT (tipo) DO NOTHING", d)
                    await conn.execute("""
                        INSERT INTO expediente_delito (numero_expediente, tipo_delito)
                        VALUES ($1, $2)
                        ON CONFLICT DO NOTHING
                    """, r["Expediente"], d)

            # guardamos solo imputados en DB
            for imp, letrados in r.get("__intervinientes__", {}).get("Imputado", []):
                row = await conn.fetchrow("""
                    SELECT parte_id FROM parte
                    WHERE numero_expediente = $1 AND nombre_razon_social = $2
                """, r["Expediente"], imp)
                if not row:
                    row = await conn.fetchrow("""
                        INSERT INTO parte (documento_cuit, numero_expediente, tipo_persona, nombre_razon_social)
                        VALUES ($1, $2, 'fisica', $3)
                        RETURNING parte_id
                    """, "CUIT_DESCONOCIDO", r["Expediente"], imp)
                parte_id = row["parte_id"]

                for l in letrados:
                    letrado_id = await obtener_o_crear_id(conn, "letrado", "nombre", l)
                    await conn.execute("""
                        INSERT INTO representacion (numero_expediente, parte_id, letrado_id, rol)
                        VALUES ($1, $2, $3, 'defensor')
                        ON CONFLICT DO NOTHING
                    """, r["Expediente"], parte_id, letrado_id)

            for res in r.get("__resoluciones__", []):
                await conn.execute("""
                    INSERT INTO plazo (numero_expediente, tipo, fecha_inicio, fecha_vencimiento, dias_habiles, estado)
                    VALUES ($1, $2, CURRENT_DATE, CURRENT_DATE, 0, 'vigente')
                    ON CONFLICT DO NOTHING
                """, r["Expediente"], res[:95])

# === Guardado en CSV ===
def guardar_csv(resultados):
    # Expedientes
    fieldnames = ["Expediente", "Carátula", "Delitos",
                  "Radicación del expediente", "Estado",
                  "Última actualización"]
    with open("7_expedientes.csv", "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for r in resultados:
            fila = {k: r.get(k, "") for k in fieldnames}
            writer.writerow(fila)

    # Intervinientes
    with open("7_intervinientes.csv", "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["Expediente", "Rol", "Nombre", "Letrado"])
        for r in resultados:
            for rol, personas in r.get("__intervinientes__", {}).items():
                for (nombre, letrs) in personas:
                    if letrs:
                        for l in letrs:
                            writer.writerow([r["Expediente"], rol, nombre, l])
                    else:
                        writer.writerow([r["Expediente"], rol, nombre, ""])

    # Resoluciones
    with open("7_resoluciones.csv", "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["Expediente", "Resolución"])
        for r in resultados:
            for res in r.get("__resoluciones__", []):
                writer.writerow([r["Expediente"], res])

# === Run principal con selección ===
async def run(scrapear_en_tramite=True, scrapear_terminadas=False):
    url = "https://www.csjn.gov.ar/tribunales-federales-nacionales/causas-de-corrupcion.html"
    resultados, vistos = [], set()

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        page = await browser.new_page()
        await page.goto(url)

        if scrapear_en_tramite:
            print("=== Scrapeando causas EN TRÁMITE ===")
            await scrape_tab(page, "#btn-solapa-1", "En trámite", "#solapa-1", vistos, resultados)

        if scrapear_terminadas:
            print("=== Scrapeando causas TERMINADAS ===")
            await scrape_tab(page, "#btn-solapa-2", "Terminadas", "#solapa-2", vistos, resultados)

        await browser.close()

    # Guardar en DB
    pool = await asyncpg.create_pool(user="admin", password="td8corrupcion",
                                     database="corrupcion_db", host="localhost", port=5432)
    await guardar_en_db(pool, resultados)
    await pool.close()

    # Guardar en CSV
    guardar_csv(resultados)
    print("✅ Guardado en DB y CSV")

# === Ejecución ===
if __name__ == "__main__":
    asyncio.run(run(scrapear_en_tramite=True, scrapear_terminadas=False))
