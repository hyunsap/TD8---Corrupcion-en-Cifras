import asyncio
from playwright.async_api import async_playwright
from bs4 import BeautifulSoup
import csv

async def scrape_tab(page, tab_selector, estado_label, container_selector, vistos, resultados):
    # Clic en la solapa
    await page.click(tab_selector)
    # Esperar a que cargue contenido en esa solapa
    await page.wait_for_selector(f"{container_selector} div.result")
    await page.wait_for_timeout(2000)  # pequeño delay extra

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

            # Intervinientes e imputados
            panel_interv = bloque.select_one("div.ver-todos-panel")
            if panel_interv:
                li_imputados = panel_interv.select("div.item-especial-largo-2 ul li")
                for li in li_imputados:
                    imputado_nombre = "".join(li.find_all(string=True, recursive=False)).strip()
                    letrados_imputado = [l.get_text(strip=True) for l in li.select("div.item")]
                    imputados.append((imputado_nombre, letrados_imputado))

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
            if identificador and (identificador, estado_label) not in vistos:
                datos["__imputados__"] = imputados
                datos["__resoluciones__"] = resoluciones
                datos["EstadoSolapa"] = estado_label
                resultados.append(datos)
                vistos.add((identificador, estado_label))

        # Siguiente página (si existe dentro de esta solapa)
        try:
            boton_siguiente = await page.query_selector(f"{container_selector} a.page-link:has-text('Siguiente')")
            if not boton_siguiente:
                break
            await boton_siguiente.click()
            await page.wait_for_timeout(2000)
            pagina += 1
        except Exception:
            break

async def run():
    url = "https://www.csjn.gov.ar/tribunales-federales-nacionales/causas-de-corrupcion.html"
    resultados = []
    vistos = set()

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        page = await browser.new_page()
        await page.goto(url)

        # Terminadas → solapa-2
        await scrape_tab(page, "#btn-solapa-2", "Terminadas", "#solapa-2", vistos, resultados)

        # En trámite → solapa-1
        await scrape_tab(page, "#btn-solapa-1", "En trámite", "#solapa-1", vistos, resultados)



        await browser.close()

    # === Exportación ===
    if resultados:
        fieldnames = [
            "Expediente", "Carátula", "Delitos",
            "Radicación del expediente", "Estado",
            "Última actualización", "EstadoSolapa"
        ]
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

    print("✅ Datos guardados en 5_expedientes.csv, 5_imputados.csv, 5_letrados.csv y 5_resoluciones.csv")


asyncio.run(run())
