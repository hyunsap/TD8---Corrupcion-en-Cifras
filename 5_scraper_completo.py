import asyncio
from playwright.async_api import async_playwright
from bs4 import BeautifulSoup
import csv

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
            
            # Expandir todos los "Ver más" de radicaciones antes de obtener el HTML
            bloques_radicacion = await page.query_selector_all("div.result")
            for idx, bloque in enumerate(bloques_radicacion):
                try:
                    # Buscar el botón "Ver MÁS" de radicaciones (clase: ver-todos soy-ver-todos)
                    ver_mas_btn = await bloque.query_selector("div.ver-todos.soy-ver-todos")
                    if ver_mas_btn:
                        is_visible = await ver_mas_btn.is_visible()
                        if is_visible:
                            await ver_mas_btn.click()
                            await page.wait_for_timeout(300)
                except Exception as e:
                    print(f"  No se pudo expandir radicaciones en bloque {idx}: {e}")
            
            # Ahora obtener el contenido HTML con todas las radicaciones expandidas
            content = await page.content()
            soup = BeautifulSoup(content, "html.parser")
            bloques = soup.find_all("div", class_="result")

            if not bloques:
                print("No se encontraron más expedientes.")
                break

            for bloque in bloques:
                info_items = bloque.find("ul", class_="info").find_all("li")
                datos = {}
                imputados, denunciados, denunciantes, querellantes = [], [], [], []
                resoluciones = []
                radicaciones = []  # Nueva lista para todas las radicaciones

                # Eliminar botones irrelevantes
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

                # === CAPTURAR TODAS LAS RADICACIONES ===
                radicacion_li = bloque.select_one("li:has(span.s2:contains('Radicación del expediente'))")
                
                if radicacion_li:
                    # Primera radicación (más reciente)
                    primera_rad = radicacion_li.select_one("div.item-especial-largo.soy-first-item-largo")
                    if primera_rad:
                        t1 = primera_rad.select_one("div.t1a")
                        t2 = primera_rad.select_one("div.t2a")
                        t3 = primera_rad.select_one("div.t3a")
                        t4 = primera_rad.select_one("div.t4a")
                        
                        radicacion = {
                            "orden": 1,
                            "fecha": t1.get_text(strip=True) if t1 else "",
                            "juzgado": t2.get_text(strip=True) if t2 else "",
                            "fiscal": t3.get_text(strip=True) if t3 else "",
                            "fiscalia": t4.get_text(strip=True) if t4 else ""
                        }
                        radicaciones.append(radicacion)
                    
                    # Radicaciones históricas (en el panel expandible)
                    panel_rad = radicacion_li.select_one("div.ver-todos-panel.panel-item-largo")
                    if panel_rad:
                        items_historicos = panel_rad.select("div.item > div.item-especial-largo")
                        
                        for idx, item_rad in enumerate(items_historicos, start=2):
                            t1 = item_rad.select_one("div.t1a")
                            t2 = item_rad.select_one("div.t2a")
                            t3 = item_rad.select_one("div.t3a")
                            t4 = item_rad.select_one("div.t4a")
                            
                            radicacion = {
                                "orden": idx,
                                "fecha": t1.get_text(strip=True) if t1 else "",
                                "juzgado": t2.get_text(strip=True) if t2 else "",
                                "fiscal": t3.get_text(strip=True) if t3 else "",
                                "fiscalia": t4.get_text(strip=True) if t4 else ""
                            }
                            radicaciones.append(radicacion)
                
                # Guardar la radicación más reciente en el campo original (para compatibilidad)
                if radicaciones:
                    primera = radicaciones[0]
                    datos["Radicación del expediente"] = f"{primera['fecha']} | {primera['juzgado']} | {primera['fiscal']} | {primera['fiscalia']}"

                # Intervinientes
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
                            imputados.extend(participantes)
                        elif "DENUNCIADO" in titulo_txt:
                            denunciados.extend(participantes)
                        elif "DENUNCIANTE" in titulo_txt:
                            denunciantes.extend(participantes)
                        elif "QUERELLANTE" in titulo_txt:
                            querellantes.extend(participantes)

                # Resoluciones
                panel_res = bloque.select_one("li:has(span:contains('Resolución/es')) div.ver-todos-panel")
                if panel_res:
                    resol_items = panel_res.select("div.item a")
                    for a in resol_items:
                        texto = a.get_text(strip=True)
                        href = a.get("href", "").strip()

                        if texto:
                            if ":" in texto:
                                fecha, nombre = texto.split(":", 1)
                                fecha = fecha.strip()
                                nombre = nombre.strip()
                            else:
                                fecha, nombre = "", texto
                            resoluciones.append({
                                "fecha": fecha,
                                "nombre": nombre,
                                "link": href
                            })

                # Asegurar claves
                claves_interes = [
                    "Expediente", "Carátula", "Delitos",
                    "Radicación del expediente", "Estado",
                    "Última actualización"
                ]
                for clave in claves_interes:
                    datos.setdefault(clave, "")

                identificador = datos.get("Expediente")
                if identificador and identificador not in vistos:
                    datos["__imputados__"] = imputados
                    datos["__denunciados__"] = denunciados
                    datos["__denunciantes__"] = denunciantes
                    datos["__querellantes__"] = querellantes
                    datos["__resoluciones__"] = resoluciones
                    datos["__radicaciones__"] = radicaciones  # Guardar todas las radicaciones
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

    # === Exportación ===
    if resultados:
        # Expedientes
        fieldnames = [
            "Expediente", "Carátula", "Delitos",
            "Radicación del expediente", "Estado",
            "Última actualización"
        ]
        with open("5_expedientes.csv", "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            for r in resultados:
                fila = {k: r.get(k, "") for k in fieldnames}
                writer.writerow(fila)

        # Intervinientes
        with open("5_intervinientes.csv", "w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow(["Expediente", "Rol", "Nombre", "Letrado"])
            for r in resultados:
                for rol in ["imputados", "denunciados", "denunciantes", "querellantes"]:
                    for (persona, letrs) in r.get(f"__{rol}__", []):
                        if letrs:
                            for l in letrs:
                                writer.writerow([r["Expediente"], rol[:-1].capitalize(), persona, l])
                        else:
                            writer.writerow([r["Expediente"], rol[:-1].capitalize(), persona, ""])

        # Resoluciones
        with open("5_resoluciones.csv", "w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow(["Expediente", "Fecha", "Nombre", "Link"])
            for r in resultados:
                for res in r.get("__resoluciones__", []):
                    writer.writerow([
                        r["Expediente"],
                        res["fecha"],
                        res["nombre"],
                        res["link"]
                    ])

        # === NUEVO: Radicaciones (historial completo) ===
        with open("5_radicaciones.csv", "w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow(["Expediente", "Orden", "Fecha", "Juzgado", "Fiscal", "Fiscalia"])
            for r in resultados:
                for rad in r.get("__radicaciones__", []):
                    writer.writerow([
                        r["Expediente"],
                        rad["orden"],
                        rad["fecha"],
                        rad["juzgado"],
                        rad["fiscal"],
                        rad["fiscalia"]
                    ])

    print("✅ Datos guardados en:")
    print("5_expedientes.csv")
    print("5_intervinientes.csv")
    print("5_resoluciones.csv")
    print("5_radicaciones.csv")


asyncio.run(run())