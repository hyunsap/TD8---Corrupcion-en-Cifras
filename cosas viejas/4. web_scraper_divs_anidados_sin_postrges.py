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
                        # Solo texto directo, sin hijos
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

                # Intervinientes e imputados con letrados
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
                if identificador and identificador not in vistos:
                    datos["__imputados__"] = imputados
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
        with open("4_expedientes.csv", "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            for r in resultados:
                fila = {k: r.get(k, "") for k in fieldnames}
                writer.writerow(fila)

        # Imputados (solo nombres)
        with open("4_imputados.csv", "w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow(["Expediente", "Imputado"])
            for r in resultados:
                for (imp, _) in r.get("__imputados__", []):
                    writer.writerow([r["Expediente"], imp])

        # Letrados (relación imputado-letrado)
        with open("4_letrados.csv", "w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow(["Expediente", "Imputado", "Letrado"])
            for r in resultados:
                for (imp, letrs) in r.get("__imputados__", []):
                    for l in letrs:
                        writer.writerow([r["Expediente"], imp, l])

        # Resoluciones
        with open("4_resoluciones.csv", "w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow(["Expediente", "Resolución"])
            for r in resultados:
                for res in r.get("__resoluciones__", []):
                    writer.writerow([r["Expediente"], res])

    print("✅ Datos guardados en 4_expedientes.csv, 4_imputados.csv, 4_letrados.csv y 4_resoluciones.csv")


asyncio.run(run())
