<<<<<<< HEAD
import asyncio
from playwright.async_api import async_playwright
import csv

resultados = []

EXCLUIR = ["SECRETARÍA GENERAL", "SECRETARÍA ELECTORAL DE LA CAPITAL FEDERAL", "PRENSA Y CEREMONIAL",
           "Dirección de Informática Jurídica".upper(), "Oficina Judicial".upper(), "Oficina de Sorteos".upper(),
           "Equipo Interdisciplinario de Ejecución Penal".upper(),
           "Dirección de Control y Asistencia de Ejecución Penal".upper(),
           "Justicia Federal de la Seguridad Social".upper()]

async def scrape_cards(page, nivel=0, path="", filtro_primer_nivel=None):
    try:
        await page.wait_for_selector("guia-subdependencias div.dependencia-card", timeout=2000)
    except:
        return

    cards = page.locator("guia-subdependencias div.dependencia-card")
    count = await cards.count()
    if count == 0:
        return

    for i in range(count):
        try:
            if await cards.nth(i).count() == 0:
                continue

            # --- obtener título ---
            header = ""
            try:
                header = await cards.nth(i).locator(".card-header").inner_text(timeout=1000)
            except:
                header = "(sin título)"

            titulo = header.strip()
            titulo_upper = titulo.upper()

            # --- filtrar cards no deseadas ---
            if any(palabra in titulo_upper for palabra in EXCLUIR):
                continue

            # --- filtrar primer nivel si se indica ---
            if nivel == 0 and filtro_primer_nivel and titulo not in filtro_primer_nivel:
                continue

            # --- FILTRO ADICIONAL PARA NIVEL 1 ---
            if nivel == 1:
                if not ("JUSTICIA NACIONAL EN LO CRIMINAL Y CORRECCIONAL FEDERAL" in titulo_upper or
                        "JUSTICIA FEDERAL DE CASACIÓN PENAL" in titulo_upper):
                    continue

            # --- intentar entrar en la card ---
            try:
                await cards.nth(i).click()
                await page.wait_for_timeout(1500)

                # =============================
                # EXTRAER INFO DE LA CARD ABIERTA
                # =============================
                info_block = page.locator("guia-dependencia-info .dependencia")
                if await info_block.count() > 0:
                    titulo_card = await info_block.locator("h5.titulo-guia").inner_text()
                    detalle_card = await info_block.locator("p.texto").inner_text()
                    # limpiar detalle: reemplazar saltos de línea por " | "
                    detalle_card = " | ".join([line.strip() for line in detalle_card.splitlines() if line.strip()])

                    # --- integrantes dentro de la card abierta ---
                    integrantes = []
                    personas = await page.locator("guia-integrantes div.persona").all()
                    for persona in personas:
                        nombre = await persona.locator(".texto-nombre").inner_text()
                        cargo = await persona.locator(".texto-cargo").inner_text() if await persona.locator(".texto-cargo").count() > 0 else ""
                        correo = await persona.locator("a").first.inner_text() if await persona.locator("a").first.count() > 0 else ""
                        telefono = await persona.locator("p.texto").nth(0).inner_text() if await persona.locator("p.texto").count() > 0 else ""

                        ficha_data = {}
                        if await persona.locator("a.boton-minus-plus").count() > 0:
                            boton = persona.locator("a.boton-minus-plus")
                            await boton.click()
                            await page.wait_for_selector("div.persona div.ficha", timeout=2000)
                            ficha_rows = await persona.locator("div.ficha .row-ficha").all()
                            for row in ficha_rows:
                                labels = await row.locator(".p-label").all_text_contents()
                                values = await row.locator(".p-value").all_text_contents()
                                for lbl, val in zip(labels, values):
                                    ficha_data[lbl.strip()] = val.strip()
                            try:
                                await boton.click()
                            except:
                                pass

                        integrantes.append({
                            "nombre": nombre.strip(),
                            "cargo": cargo.strip(),
                            "telefono": telefono.strip(),
                            "correo": correo.strip(),
                            "ficha": ficha_data
                        })

                    # solo agregar si hay información de integrantes o detalle
                    if integrantes or detalle_card:
                        resp_limpios = []
                        for r in integrantes:
                            ficha_str = " | ".join([f"{k}: {v}" for k, v in r['ficha'].items()])
                            fila = f"Nombre: {r['nombre']} | Cargo: {r['cargo']} | Tel: {r['telefono']} | Email: {r['correo']} | {ficha_str}"
                            resp_limpios.append(fila)
                        resp_str_abierta = "; ".join(resp_limpios)

                        resultados.append({
                            "nivel": nivel,
                            "path": path,
                            "titulo": titulo_card.strip(),
                            "detalle": detalle_card.strip(),
                            "responsables": resp_str_abierta
                        })

                # =============================
                # SUBCARDS
                # =============================
                try:
                    await page.wait_for_selector("guia-subdependencias div.dependencia-card", timeout=2000)
                    nuevo_path = f"{path} > {titulo}" if path else titulo
                    await scrape_cards(page, nivel+1, nuevo_path, filtro_primer_nivel)
                except:
                    pass

                # --- volver al nivel anterior ---
                boton_volver = page.locator("button.btn.button-primary")
                if await boton_volver.count() > 0:
                    await boton_volver.click()
                    await page.wait_for_timeout(1500)

            except Exception as e:
                print(f"⚠️ No se pudo entrar a {titulo}: {e}")

        except Exception as e:
            print(f"⚠️ Error procesando card en nivel {nivel}: {e}")

async def run():
    url = "https://www.pjn.gov.ar/guia"

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=False)
        page = await browser.new_page()
        await page.goto(url)

        filtro = ["FUEROS FEDERALES", "FUEROS CON COMPETENCIA EN TODO EL PAÍS"]

        await scrape_cards(page, filtro_primer_nivel=filtro)

        # --- escribir CSV con fieldnames correctos ---
        with open("tribunales_full.csv", "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=["nivel", "path", "titulo", "detalle", "responsables"])
            writer.writeheader()
            writer.writerows(resultados)

        print(f"✅ Scrap completo: {len(resultados)} registros guardados en tribunales_full.csv")
        await browser.close()

asyncio.run(run())
=======
import asyncio
from playwright.async_api import async_playwright
import csv

resultados = []

EXCLUIR = ["SECRETARÍA GENERAL", "SECRETARÍA ELECTORAL DE LA CAPITAL FEDERAL", "PRENSA Y CEREMONIAL",
           "Dirección de Informática Jurídica".upper(), "Oficina Judicial".upper(), "Oficina de Sorteos".upper(),
           "Equipo Interdisciplinario de Ejecución Penal".upper(),
           "Dirección de Control y Asistencia de Ejecución Penal".upper(),
           "Justicia Federal de la Seguridad Social".upper()]

async def scrape_cards(page, nivel=0, path="", filtro_primer_nivel=None):
    try:
        await page.wait_for_selector("guia-subdependencias div.dependencia-card", timeout=2000)
    except:
        return

    cards = page.locator("guia-subdependencias div.dependencia-card")
    count = await cards.count()
    if count == 0:
        return

    for i in range(count):
        try:
            if await cards.nth(i).count() == 0:
                continue

            # --- obtener título ---
            header = ""
            try:
                header = await cards.nth(i).locator(".card-header").inner_text(timeout=1000)
            except:
                header = "(sin título)"

            titulo = header.strip()
            titulo_upper = titulo.upper()

            # --- filtrar cards no deseadas ---
            if any(palabra in titulo_upper for palabra in EXCLUIR):
                continue

            # --- filtrar primer nivel si se indica ---
            if nivel == 0 and filtro_primer_nivel and titulo not in filtro_primer_nivel:
                continue

            # --- FILTRO ADICIONAL PARA NIVEL 1 ---
            if nivel == 1:
                if not ("JUSTICIA NACIONAL EN LO CRIMINAL Y CORRECCIONAL FEDERAL" in titulo_upper or
                        "JUSTICIA FEDERAL DE CASACIÓN PENAL" in titulo_upper):
                    continue

            # --- intentar entrar en la card ---
            try:
                await cards.nth(i).click()
                await page.wait_for_timeout(1500)

                # =============================
                # EXTRAER INFO DE LA CARD ABIERTA
                # =============================
                info_block = page.locator("guia-dependencia-info .dependencia")
                if await info_block.count() > 0:
                    titulo_card = await info_block.locator("h5.titulo-guia").inner_text()
                    detalle_card = await info_block.locator("p.texto").inner_text()
                    # limpiar detalle: reemplazar saltos de línea por " | "
                    detalle_card = " | ".join([line.strip() for line in detalle_card.splitlines() if line.strip()])

                    # --- integrantes dentro de la card abierta ---
                    integrantes = []
                    personas = await page.locator("guia-integrantes div.persona").all()
                    for persona in personas:
                        nombre = await persona.locator(".texto-nombre").inner_text()
                        cargo = await persona.locator(".texto-cargo").inner_text() if await persona.locator(".texto-cargo").count() > 0 else ""
                        correo = await persona.locator("a").first.inner_text() if await persona.locator("a").first.count() > 0 else ""
                        telefono = await persona.locator("p.texto").nth(0).inner_text() if await persona.locator("p.texto").count() > 0 else ""

                        ficha_data = {}
                        if await persona.locator("a.boton-minus-plus").count() > 0:
                            boton = persona.locator("a.boton-minus-plus")
                            await boton.click()
                            await page.wait_for_selector("div.persona div.ficha", timeout=2000)
                            ficha_rows = await persona.locator("div.ficha .row-ficha").all()
                            for row in ficha_rows:
                                labels = await row.locator(".p-label").all_text_contents()
                                values = await row.locator(".p-value").all_text_contents()
                                for lbl, val in zip(labels, values):
                                    ficha_data[lbl.strip()] = val.strip()
                            try:
                                await boton.click()
                            except:
                                pass

                        integrantes.append({
                            "nombre": nombre.strip(),
                            "cargo": cargo.strip(),
                            "telefono": telefono.strip(),
                            "correo": correo.strip(),
                            "ficha": ficha_data
                        })

                    # solo agregar si hay información de integrantes o detalle
                    if integrantes or detalle_card:
                        resp_limpios = []
                        for r in integrantes:
                            ficha_str = " | ".join([f"{k}: {v}" for k, v in r['ficha'].items()])
                            fila = f"Nombre: {r['nombre']} | Cargo: {r['cargo']} | Tel: {r['telefono']} | Email: {r['correo']} | {ficha_str}"
                            resp_limpios.append(fila)
                        resp_str_abierta = "; ".join(resp_limpios)

                        resultados.append({
                            "nivel": nivel,
                            "path": path,
                            "titulo": titulo_card.strip(),
                            "detalle": detalle_card.strip(),
                            "responsables": resp_str_abierta
                        })

                # =============================
                # SUBCARDS
                # =============================
                try:
                    await page.wait_for_selector("guia-subdependencias div.dependencia-card", timeout=2000)
                    nuevo_path = f"{path} > {titulo}" if path else titulo
                    await scrape_cards(page, nivel+1, nuevo_path, filtro_primer_nivel)
                except:
                    pass

                # --- volver al nivel anterior ---
                boton_volver = page.locator("button.btn.button-primary")
                if await boton_volver.count() > 0:
                    await boton_volver.click()
                    await page.wait_for_timeout(1500)

            except Exception as e:
                print(f"⚠️ No se pudo entrar a {titulo}: {e}")

        except Exception as e:
            print(f"⚠️ Error procesando card en nivel {nivel}: {e}")

async def run():
    url = "https://www.pjn.gov.ar/guia"

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=False)
        page = await browser.new_page()
        await page.goto(url)

        filtro = ["FUEROS FEDERALES", "FUEROS CON COMPETENCIA EN TODO EL PAÍS"]

        await scrape_cards(page, filtro_primer_nivel=filtro)

        # --- escribir CSV con fieldnames correctos ---
        with open("tribunales_full.csv", "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=["nivel", "path", "titulo", "detalle", "responsables"])
            writer.writeheader()
            writer.writerows(resultados)

        print(f"✅ Scrap completo: {len(resultados)} registros guardados en tribunales_full.csv")
        await browser.close()

asyncio.run(run())
>>>>>>> c5e62da595a3457b78501708a9fcc4e354276b87
