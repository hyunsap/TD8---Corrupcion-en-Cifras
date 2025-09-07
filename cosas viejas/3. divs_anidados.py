import asyncio
from playwright.async_api import async_playwright
from bs4 import BeautifulSoup
import csv

async def run():
    url = "https://www.csjn.gov.ar/tribunales-federales-nacionales/causas-de-corrupcion.html"
    resultados = []
    vistos = set()  # expedientes ya procesados

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        page = await browser.new_page()
        await page.goto(url)

        pagina = 1
        while True:
            print(f"Procesando página {pagina}...")

            # Esperar que carguen los resultados
            await page.wait_for_selector("div.result")

            # Extraer HTML y parsear con BeautifulSoup
            content = await page.content()
            soup = BeautifulSoup(content, "html.parser")
            bloques = soup.find_all("div", class_="result")

            if not bloques:
                print("No se encontraron más expedientes, deteniendo.")
                break

            for bloque in bloques:
                info_items = bloque.find("ul", class_="info").find_all("li")
                datos = {}

                intervinientes = []
                letrados = []

                for item in info_items:
                    etiqueta = item.find("span")
                    if not etiqueta:
                        continue

                    clave = etiqueta.get_text(strip=True).replace(":", "")

                    # Caso especial: Carátula
                    if clave == "Carátula":
                        # Solo el texto directo (sin divs hijos)
                        valor = "".join(item.find_all(string=True, recursive=False)).strip()
                        datos[clave] = valor

                        # Extraer intervinientes y letrados en listas separadas
                        bloques_interv = item.find_all("div", class_="intervinientes")
                        for b in bloques_interv:
                            intervinientes.append(b.get_text(" ", strip=True))

                        bloques_letr = item.find_all("div", class_="letrados")
                        for b in bloques_letr:
                            letrados.append(b.get_text(" ", strip=True))

                    else:
                        valor = item.get_text(strip=True).replace(etiqueta.get_text(strip=True), "").strip()
                        datos[clave] = valor

                # aseguramos que siempre tenga las mismas claves
                claves_interes = [
                    "Expediente", 
                    "Carátula", 
                    "Delitos", 
                    "Radicación del expediente", 
                    "Estado", 
                    "Resolución/es", 
                    "Última actualización"
                ]
                for clave in claves_interes:
                    datos.setdefault(clave, "")

                identificador = datos.get("Expediente")
                if identificador and identificador not in vistos:
                    datos["__intervinientes__"] = intervinientes
                    datos["__letrados__"] = letrados
                    resultados.append(datos)
                    vistos.add(identificador)
                    
            # Buscar si existe el botón "Siguiente"
            try:
                boton_siguiente = await page.query_selector("a.page-link:has-text('Siguiente')")
                if not boton_siguiente:
                    print("No hay más páginas.")
                    break
                await boton_siguiente.click()
                await page.wait_for_timeout(2000)  # espera 2 segundos
                pagina += 1
            except Exception:
                print("No se pudo avanzar a la siguiente página.")
                break

        await browser.close()

    # === Exportación ===
    if resultados:
        # Expedientes
        fieldnames = [
            "Expediente","Carátula","Delitos",
            "Radicación del expediente","Estado",
            "Resolución/es","Última actualización"
        ]

        with open("expedientes.csv", "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            for r in resultados:
                fila = {k: r.get(k, "") for k in fieldnames}  # solo columnas principales
                writer.writerow(fila)

        # Intervinientes
        with open("intervinientes.csv", "w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow(["Expediente","Interviniente"])
            for r in resultados:
                for i in r.get("__intervinientes__", []):
                    writer.writerow([r["Expediente"], i])

        # Letrados
        with open("letrados.csv", "w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow(["Expediente","Letrado"])
            for r in resultados:
                for l in r.get("__letrados__", []):
                    writer.writerow([r["Expediente"], l])

    print("✅ Datos guardados en expedientes.csv, intervinientes.csv y letrados.csv")


asyncio.run(run())
