import os
import re
from bs4 import BeautifulSoup
from datetime import datetime

# Directorio donde están los archivos con errores
current_dir = os.path.dirname(os.path.abspath(__file__))
path_error = 'data_error_files'
report_file = 'ErrorReport.txt'

cat_term_type = {
    "Sinónimos": "Sinonimos",
    "Palabras clave": "PalabrasClave"
}

map_encyclopedia_tags = {
    "EncyclopediaName": "title",
    "PLMCode": "Codigo",
    "Descripción": "RubroMaestro",
    "Attributes": "RubroMaestro",
    "HTMLContent": "rubroenc"
}

def analizar_html(filepath):
    errores = []
    with open(filepath, 'r', encoding='utf-8') as file:
        html_content = file.read()
    
    soup = BeautifulSoup(html_content, "html.parser")

    # === Validaciones ===
    # 1. <p class="h1"> con <span class="Codigo"> y patrón esperado
    main_tag = soup.find("p", class_="h1")
    if not main_tag:
        errores.append("No se encontró elemento <p class='h1'> que contiene el código PLM")
    else:
        tag_codigo = map_encyclopedia_tags.get("PLMCode")
        codigo_span = main_tag.find("span", class_=tag_codigo)
        if not codigo_span:
            errores.append(f"No se encontró span con class='{tag_codigo}' dentro de <p class='h1'>")
        else:
            full_text = codigo_span.get_text(strip=True)
            if not re.match(r'(.+?)\s*\[(.+?)\]', full_text):
                errores.append("No se encontró el patrón esperado en el texto del PLMCode")

    # 2. Atributos con múltiples sinonimos o palabras clave
    tag_attr = map_encyclopedia_tags.get("Attributes")
    rub_master = soup.find_all("p", class_=tag_attr)
    for tag in rub_master:
        pointer = tag.find_next_sibling("p", class_="Normal")
        if pointer:
            str_pnter = str(pointer)
            synonyms = cat_term_type.get("Sinónimos")
            if f"</span><span class=\"{synonyms}\">" in str_pnter:
                errores.append(f"Error en atributo: Sinonimos, se encontraron múltiples en HTML: {str_pnter}")
            keywords = cat_term_type.get("Palabras clave")
            if f"</span><span class=\"{keywords}\">" in str_pnter:
                errores.append(f"Error en atributo: PalabrasClave, se encontraron múltiples en HTML: {str_pnter}")
    
    # 3. Atributos rubroenc con <span class="h2"> repetidos en mismo párrafo
    rubroenc = map_encyclopedia_tags.get("HTMLContent")
    rub_enc = soup.find_all("p", class_=rubroenc)
    for tag in rub_enc:
        str_tag = str(tag)
        if "</span><span class=\"h2\">" in str_tag:
            errores.append(f"Error en atributo: {str_tag}, contiene múltiples <span class='h2'>")
    
    return errores

def generar_reporte(str_date):
    output_file = os.path.join(current_dir, path_error, report_file)
    with open(output_file.replace('.txt',f"-{str_date}.txt"), 'w', encoding='utf-8') as out:
        for filename in os.listdir(path_error):
            if filename.endswith(".html"):
                filepath = os.path.join(path_error, filename)
                errores = analizar_html(filepath)
                if errores:
                    out.write(f"Archivo: {filename}\n")
                    for err in errores:
                        out.write(f"  - {err}\n")
                    out.write("\n")
    print(f"✅ Reporte generado en: {output_file}")

if __name__ == "__main__":
    # Obtener el objeto datetime actual
    now = datetime.now()
    # Formato común (YYYYMMDD-HHMMSS)
    full_format = now.strftime("%Y%m%d-%H%M%S")
    generar_reporte(full_format)
