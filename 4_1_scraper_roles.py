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

                # Última radicación
                radicacion_div = bloque.select_one("div.item-especial-largo.soy-first-item-largo")
                if radicacion_div:
                    partes = [
                        radicacion_div.select_one("div.t1a"),
                        radicacion_div.select_one("div.t2a"),
                        radicacion_div.select_one("div.t3a"),
                        radicacion_div.select_one("div.t4a")
                    ]
                    datos["Radicación del expediente"] = " | ".join([p.get_text(strip=True) for p in partes if p])

                # Intervinientes (imputados, denunciados, denunciantes, querellantes)
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
                            # Nombre = texto directo del <li> (sin spans/divs internos)
                            nombre_parts = [
                                t.strip()
                                for t in li.find_all(string=True, recursive=False)
                                if t.strip()
                            ]
                            nombre = " ".join(nombre_parts)

                            # Buscar letrados dentro de div.ver-todos-panel-2
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
                    resol_items = panel_res.select("div.item")
                    for r in resol_items:
                        texto = r.get_text(strip=True)
                        if texto:
                            resoluciones.append(texto)

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
        with open("4_1_expedientes.csv", "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            for r in resultados:
                fila = {k: r.get(k, "") for k in fieldnames}
                writer.writerow(fila)

        # Intervinientes unificados
        with open("4_1_intervinientes.csv", "w", newline="", encoding="utf-8") as f:
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
        with open("4_1_resoluciones.csv", "w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow(["Expediente", "Resolución"])
            for r in resultados:
                for res in r.get("__resoluciones__", []):
                    writer.writerow([r["Expediente"], res])

    print("✅ Datos guardados en 4_1_expedientes.csv, 4_1_intervinientes.csv y 4_1_resoluciones.csv")


asyncio.run(run())
