import asyncio
import csv
from playwright.async_api import async_playwright
from bs4 import BeautifulSoup

URL = "https://www.pjn.gov.ar/guia"


async def extraer_jueces(page):
    """Extrae jueces y cargos de la vista actual"""
    await page.wait_for_selector("div.dependencia-card", timeout=60000)
    html = await page.content()
    soup = BeautifulSoup(html, "html.parser")

    jueces = []
    for card in soup.select("div.dependencia-card"):
        titulo = card.select_one("div.card-header")
        body = card.select_one("div.card-body")
        if titulo:
            cargo = titulo.get_text(strip=True)
            detalle = body.get_text(" ", strip=True) if body else ""
            jueces.append({"cargo": cargo, "detalle": detalle})
    return jueces


async def recorrer_fuero(page, nombre_fuero, writer):
    """Entra en un fuero (Nacional/Federal) y recorre sus dependencias"""
    print(f"== Entrando a {nombre_fuero} ==")

    await page.wait_for_selector("div.dependencia-card")
    html = await page.content()
    soup = BeautifulSoup(html, "html.parser")

    dependencias = soup.select("div.dependencia-card")
    for idx, dep in enumerate(dependencias):
        titulo = dep.select_one("div.card-header")
        if not titulo:
            continue

        nombre_dep = titulo.get_text(strip=True)
        print(f"  -> Abriendo {nombre_dep}")

        # Click en la dependencia
        cards = page.locator("div.dependencia-card")
        await cards.nth(idx).click()
        await page.wait_for_timeout(1500)  # dejar que Angular renderice

        # Extraer jueces dentro de esa dependencia
        jueces = await extraer_jueces(page)

        for j in jueces:
            writer.writerow([nombre_fuero, nombre_dep, j["cargo"], j["detalle"]])

        # Volver atrás
        await page.go_back()
        await page.wait_for_selector("div.dependencia-card")


async def main():
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=False)  # cambiar a True si no querés ver el navegador
        page = await browser.new_page()
        await page.goto(URL, timeout=60000)

        # Esperar que aparezcan los fueros
        await page.wait_for_selector("div.dependencia-card")

        with open("scraper_jueces.csv", "w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow(["fuero", "tribunal", "cargo", "detalle"])

            # Buscar los cards de "Fueros Nacionales" y "Fueros Federales"
            cards = page.locator("div.dependencia-card")
            count = await cards.count()

            for i in range(count):
                texto = await cards.nth(i).inner_text()
                if "Fueros Nacionales" in texto or "Fueros Federales" in texto:
                    nombre_fuero = texto.strip()
                    await cards.nth(i).click()
                    await recorrer_fuero(page, nombre_fuero, writer)
                    await page.go_back()
                    await page.wait_for_selector("div.dependencia-card")
                    
        await browser.close()


if __name__ == "__main__":
    asyncio.run(main())
