import asyncio
from playwright.async_api import async_playwright
from bs4 import BeautifulSoup
import csv

async def run():
    url = "https://www.csjn.gov.ar/tribunales-federales-nacionales/causas-de-corrupcion.html"
    resultados_terminadas = []
    vistos = set()

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        page = await browser.new_page()
        await page.goto(url)

        # PROCESAR CAUSAS TERMINADAS (Solapa 2)
        print("\n=== PROCESANDO CAUSAS TERMINADAS ===")
        await page.click("#btn-solapa-2")
        
        # Esperar a que la solapa 2 esté visible y tenga contenido
        await page.wait_for_selector("#solapa-2", state="visible")
        await page.wait_for_timeout(2000)
        
        # Verificar que haya resultados cargados en la solapa 2
        await page.wait_for_selector("#solapa-2 div.result", state="visible", timeout=10000)
        
        pagina = 1
        while True:
            print(f"Procesando página {pagina} de causas terminadas...")
            
            # Esperar a que los resultados estén visibles EN LA SOLAPA 2
            await page.wait_for_selector("#solapa-2 div.result", state="visible")
            await page.wait_for_timeout(1000)
            
            # Expandir radicaciones solo dentro de la solapa 2
            bloques_radicacion = await page.query_selector_all("#solapa-2 div.result")
            #print(f"  Encontrados {len(bloques_radicacion)} expedientes en esta página")
            
            for idx, bloque in enumerate(bloques_radicacion):
                try:
                    ver_mas_btn = await bloque.query_selector("div.ver-todos.soy-ver-todos")
                    if ver_mas_btn:
                        is_visible = await ver_mas_btn.is_visible()
                        if is_visible:
                            await ver_mas_btn.click()
                            await page.wait_for_timeout(300)
                except Exception as e:
                    print(f"  No se pudo expandir radicaciones en bloque {idx}: {e}")
            
            # Obtener contenido HTML
            content = await page.content()
            soup = BeautifulSoup(content, "html.parser")
            
            # Buscar solo dentro de la solapa 2
            solapa_2 = soup.find("div", id="solapa-2")
            if not solapa_2:
                print("No se encontró la solapa 2")
                break
                
            bloques = solapa_2.find_all("div", class_="result")

            if not bloques:
                print("No se encontraron más expedientes terminados.")
                break

            # Procesar cada bloque
            for bloque in bloques:
                datos = procesar_bloque(bloque)
                identificador = datos.get("Expediente")
                if identificador and identificador not in vistos:
                    datos["Estado_General"] = "TERMINADA"
                    resultados_terminadas.append(datos)
                    vistos.add(identificador)
                    #print(f"  ✓ {identificador}")

            # Intentar ir a la siguiente página
            try:
                # Verificar si existe el botón "Siguiente" en la solapa 2
                boton_siguiente = await page.query_selector("#solapa-2 a.page-link.next")
                
                if not boton_siguiente:
                    print("No hay botón 'Siguiente'. Fin de la paginación.")
                    break
                
                # Verificar si el botón está visible y habilitado
                is_visible = await boton_siguiente.is_visible()
                if not is_visible:
                    print("El botón 'Siguiente' no está visible. Fin de la paginación.")
                    break
                
                # Obtener el número de página actual antes del clic
                paginador_activo = await page.query_selector("#solapa-2 span.page-link.active")
                pagina_actual = await paginador_activo.inner_text() if paginador_activo else str(pagina)
                #print(f"  Página actual: {pagina_actual}, navegando a la siguiente...")
                
                # Hacer clic y esperar a que cambie el contenido
                await boton_siguiente.click()
                
                # Esperar a que el paginador cambie (indicando que se cargó la nueva página)
                await page.wait_for_function(
                    f"document.querySelector('#solapa-2 span.page-link.active')?.innerText !== '{pagina_actual}'",
                    timeout=10000
                )
                
                # Esperar un poco más para que termine de cargar
                await page.wait_for_timeout(2000)
                
                pagina += 1
                
            except Exception as e:
                print(f"Error al navegar a la siguiente página: {e}")
                break

        await browser.close()

    # === Exportación ===
    exportar_resultados(resultados_terminadas, "terminadas")
    
    print(f"\n✅ Total causas terminadas: {len(resultados_terminadas)}")


def procesar_bloque(bloque):
    """Extrae la información de un bloque de expediente"""
    info_items = bloque.find("ul", class_="info").find_all("li")
    datos = {}
    imputados, denunciados, denunciantes, querellantes = [], [], [], []
    resoluciones = []
    radicaciones = []

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

    # Capturar radicaciones
    radicacion_li = bloque.select_one("li:has(span.s2:contains('Radicación del expediente'))")
    
    if radicacion_li:
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

    datos["__imputados__"] = imputados
    datos["__denunciados__"] = denunciados
    datos["__denunciantes__"] = denunciantes
    datos["__querellantes__"] = querellantes
    datos["__resoluciones__"] = resoluciones
    datos["__radicaciones__"] = radicaciones

    return datos


def exportar_resultados(resultados, tipo):
    """Exporta los resultados a archivos CSV"""
    if not resultados:
        return

    prefix = f"scraper_completas_{tipo}"
    
    # Expedientes
    fieldnames = [
        "Expediente", "Carátula", "Delitos",
        "Radicación del expediente", "Estado", "Estado_General",
        "Última actualización"
    ]
    with open(f"{prefix}_expedientes.csv", "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for r in resultados:
            fila = {k: r.get(k, "") for k in fieldnames}
            writer.writerow(fila)

    # Intervinientes
    with open(f"{prefix}_intervinientes.csv", "w", newline="", encoding="utf-8") as f:
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
    with open(f"{prefix}_resoluciones.csv", "w", newline="", encoding="utf-8") as f:
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

    # Radicaciones
    with open(f"{prefix}_radicaciones.csv", "w", newline="", encoding="utf-8") as f:
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

    print(f"✅ Datos de {tipo} guardados en:")
    print(f"  - {prefix}_expedientes.csv")
    print(f"  - {prefix}_intervinientes.csv")
    print(f"  - {prefix}_resoluciones.csv")
    print(f"  - {prefix}_radicaciones.csv")


asyncio.run(run())