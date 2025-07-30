from bs4 import BeautifulSoup
import json
import re
import os

# === Rutas de entrada/salida ===
current_dir = os.path.dirname(os.path.abspath(__file__))
input_file = os.path.join(current_dir, '..', 'prcss_data_files', 'test_disease.html')
output_file = os.path.join(current_dir, '..', 'prcss_data_files', 'encyclopedia.json')

# Variable global para el ID incremental
current_encyclopedia_id = 0

def extract_data_from_html_local(filepath):
    global current_encyclopedia_id

    with open(filepath, "r", encoding="utf-8") as file:
        html_content = file.read()
    
    soup = BeautifulSoup(html_content, "html.parser")
    
    # ========== ESTRUCTURA PRINCIPAL ==========
    result = {
        "EncyclopediaId": current_encyclopedia_id,
        "PLMCode": "",
        "EncyclopediaName": "",
        "Description": "",
        "ReadingTime": "",
        "EncyclopediaImage": "",
        "EncyclopediaTypeId": 1,
        "AuthorId": "",
        "Active": 1,
        "MedicalEncyclopediaAttribute": []
    }
    
    # Título
    title_tag = soup.find("title")
    result["EncyclopediaName"] = title_tag.get_text(strip=True) if title_tag else ""
    
    # PLMCode
    codigo_tag = soup.find("span", class_="Codigo")
    if codigo_tag:
        match = re.search(r"\[(.*?)\]", codigo_tag.get_text())
        if match:
            result["PLMCode"] = match.group(1)
    
    # Descripción principal como HTML
    desc_rubro = soup.find("p", class_="RubroMaestro", string=lambda t: t and "Descripción" in t)
    if desc_rubro:
        desc_normal = desc_rubro.find_next_sibling("p", class_="Normal")
        if desc_normal:
            result["Description"] = str(desc_normal)
    
    # ========== MedicalEncyclopediaAttribute ==========
    attributes = []
    
    # Rubros principales
    rubro_maestro_map = {
        "Descripción": 1,
        "Sinónimos": 2,
        "Palabras clave que guíen a la enciclopedia": 3
    }
    
    for tag in soup.find_all("p", class_="RubroMaestro"):
        label = tag.get_text(strip=True).replace(":", "")
        attribute_id = rubro_maestro_map.get(label)
        if attribute_id:
            html_content = ''
            pointer = tag.find_next_sibling()
            while pointer and not (pointer.name == 'p' and pointer.get('class') == ['hr']):
                html_content += str(pointer)
                pointer = pointer.find_next_sibling()
            attributes.append({
                "AttributeGroupId": attribute_id,
                "EncyclopediaId": current_encyclopedia_id,  # Usamos el mismo ID aquí
                "Content": "",
                "HTMLContent": html_content.strip()
            })
    
    # Subrubros adicionales (rubroenc)
    rubroenc_map = {
        "Definición y causas": 4,
        "Síntomas y diagnóstico": 5,
        "Tratamiento y bienestar": 6
    }
    
    for tag in soup.find_all("p", class_="rubroenc"):
        span = tag.find("span", class_="h2")
        if span:
            label = span.get_text(strip=True).replace(":", "")
            attribute_id = rubroenc_map.get(label)
            if attribute_id:
                html_content = ''
                pointer = tag.find_next_sibling()
                while pointer and not (
                    pointer.name == 'p' and pointer.get('class') in [['hr'], ['rubroenc']]
                ):
                    html_content += str(pointer)
                    pointer = pointer.find_next_sibling()
                attributes.append({
                    "AttributeGroupId": attribute_id,
                    "EncyclopediaId": current_encyclopedia_id,  # Usamos el mismo ID aquí
                    "Content": "",
                    "HTMLContent": html_content.strip()
                })
    
    result["MedicalEncyclopediaAttribute"] = attributes
    
    # Incrementamos el ID para la próxima ejecución
    current_encyclopedia_id += 1
    
    return result

# === EJECUCIÓN LOCAL ===
if __name__ == "__main__":
    if not os.path.exists(input_file):
        raise FileNotFoundError(f"El archivo {input_file} no existe. Verifica la ruta.")
    
    data = extract_data_from_html_local(input_file)

    with open(output_file, "w", encoding="utf-8") as out:
        json.dump(data, out, ensure_ascii=False, indent=2)

    print(f"✅ Archivo JSON generado en: {output_file}")
