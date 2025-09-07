import requests
from bs4 import BeautifulSoup
import pandas as pd
import os

def scrape_expedientes(base_url, max_pages=5):
    resultados = []

    for page in range(1, max_pages + 1):
        print(f"Procesando página {page}...")
        url = f"{base_url}?page={page}"
        response = requests.get(url)

        if response.status_code != 200:
            print(f"Error al acceder a la página {page}")
            break

        soup = BeautifulSoup(response.text, "html.parser")

        # Cada bloque de expediente
        bloques = soup.find_all("div", class_="result")

        if not bloques:
            print("No hay más expedientes, deteniendo.")
            break

        for bloque in bloques:
            info_items = bloque.find("ul", class_="info").find_all("li")
            datos = {}

            for item in info_items:
                etiqueta = item.find("span")
                if etiqueta:
                    clave = etiqueta.get_text(strip=True).replace(":", "")
                    valor = item.get_text(" ", strip=True).replace(etiqueta.get_text(strip=True), "").strip()

                    # limpieza de basura visual
                    for ruido in ["VER MÁS", "VER MENOS", "CERRAR", "VER LETRADOS", "VER INTERVINIENTES"]:
                        valor = valor.replace(ruido, "")

                    valor = valor.strip()
                    datos[clave] = valor

            resultados.append(datos)

    return resultados


if __name__ == "__main__":
    base_url = "https://www.csjn.gov.ar/tribunales-federales-nacionales/causas-de-corrupcion.html"
    datos = scrape_expedientes(base_url, max_pages=5)

    # Crear DataFrame dinámico: pandas arma todas las columnas encontradas
    df = pd.DataFrame(datos)

    # Guardar en CSV
    output_path = os.path.expanduser("~/Documents/expedientes_all.csv")
    df.to_csv(output_path, index=False, encoding="utf-8-sig")
    print(f"Datos guardados en {output_path}")
