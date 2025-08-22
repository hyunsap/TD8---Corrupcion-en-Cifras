import asyncio
from playwright.async_api import async_playwright
from bs4 import BeautifulSoup
import csv

async def run():
    url = "https://www.csjn.gov.ar/tribunales-federales-nacionales/causas-de-corrupcion.html"
    resultados = []
    vistos = set()  # Expedientes ya procesados

    # columnas fijas que queremos en el CSV
    claves_interes = [
        "Expediente", 
        "Carátula", 
        "Delitos", 
        "Radicación del expediente", 
        "Estado", 
        "Resolución/es", 
        "Última actualización"
    ]

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

                for item in info_items:
                    etiqueta = item.find("span")
                    if etiqueta:
                        clave = etiqueta.get_text(strip=True).replace(":", "")
                        valor = item.get_text(strip=True).replace(etiqueta.get_text(strip=True), "").strip()
                        datos[clave] = valor

                # asegurar que estén todas las claves_interes (aunque vacías)
                for clave in claves_interes:
                    datos.setdefault(clave, "")

                # usar el campo "Expediente" como identificador único
                identificador = datos.get("Expediente")
                if identificador and identificador not in vistos:
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

    # Guardar en CSV con solo las columnas fijas
    if resultados:
        with open("expedientes_limpiospopo.csv", "w", newline="", encoding="utf-8-sig") as f:
            writer = csv.DictWriter(f, fieldnames=claves_interes)
            writer.writeheader()
            for fila in resultados:
                fila_limpia = {k: fila.get(k, "") for k in claves_interes}
                writer.writerow(fila_limpia)

    print("Datos guardados en expedientes_limpiospopo.csv")

asyncio.run(run())
