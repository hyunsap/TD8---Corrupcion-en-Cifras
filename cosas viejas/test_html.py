import requests
from bs4 import BeautifulSoup

url = "https://www.csjn.gov.ar/tribunales-federales-nacionales/causas-de-corrupcion.html"

# Pedimos la página
response = requests.get(url)
print("Status:", response.status_code)

# Creamos el objeto BeautifulSoup
soup = BeautifulSoup(response.text, "html.parser")

# 1) Contamos cuántos bloques de expedientes detectamos
bloques = soup.find_all("div", class_="result")
print("Cantidad de bloques encontrados:", len(bloques))

# 2) Imprimimos el texto del primer bloque para ver qué trae
if bloques:
    print("\nPrimer bloque (recortado a 1000 caracteres):\n")
    print(bloques[0].get_text(" ", strip=True)[:1000])
