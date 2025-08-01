import json
import re
import os
from bs4 import BeautifulSoup

# === Rutas de entrada/salida ===
current_dir = os.path.dirname(os.path.abspath(__file__))
input_file = os.path.join(current_dir, '..', 'prcss_data_files', 'test_disease.html')
output_file = os.path.join(current_dir, '..', 'prcss_data_files', 'encyclopedia.json')

# Variable global para el ID incremental
current_encyclopedia_id = 1

# Mapeo para EncyclopediaTypes
encyclopedia_types = {
    "Enfermedades": 1,
    "Síntomas": 2,
    "Procedimientos Quirurgicos": 3,
    "Procedimientos Diagnosticos": 4
}

# Mapeo de grupos de atributos
rubro_maestro_map = {
    "Descripción": 2,
    "Sinónimos": 3,
    "Palabras clave": 4
}

rubroenc_map = {
    "Definición y causas": 5,
    "Síntomas y diagnóstico": 6,
    "Tratamiento y bienestar": 7,
    "Prevención y detección oportuna": 8,
    "Bibliografía": 9
}

entry_term_type = {
    "Sinónimos": 1,       # Synonym
    "Palabras clave": 2   # Related Term
}

cat_term_type = {
    "Sinonimos": 1,     # Synonym
    "PalabrasClave": 2  # Related Term
}

def get_attribute_id(label):
    lower_label = label.lower()
    for key in rubroenc_map:
        if key.lower() in lower_label:
            return rubroenc_map[key]
    return None

def get_entryterm_type_id(label):
    lower_label = label.lower()
    for key in entry_term_type:
        if key.lower() in lower_label:
            return entry_term_type[key]
    return None

def get_entryterm_by_id(id):
    for cnt, key in enumerate(cat_term_type):
        if id == cnt + 1:
            return key
    return None

def extract_data_from_html_local(filepath):
    global current_encyclopedia_id

    with open(filepath, "r", encoding="utf-8") as file:
        html_content = file.read()
    
    soup = BeautifulSoup(html_content, "html.parser")

    # ========== ESTRUCTURA PRINCIPAL ==========
    mdcl_ntrs_trms = {
        "MedicalTerm": [],
        "MedicalEncyclopediaTerm": [],
        "MedicalEntryTerm": []
    }
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
        "MedicalEncyclopediaAttribute": [],
        "MedicalEntriesTerms": mdcl_ntrs_trms
    }
    
    # Título
    title_tag = soup.find("title")
    result["EncyclopediaName"] = title_tag.get_text(strip=True) if title_tag else ""
    
    # PLMCode
    fthr_term = ""
    codigo_span = soup.find("span", class_="Codigo")
    if codigo_span:
        full_text = codigo_span.get_text(strip=True)
        match = re.match(r'(.+?)\s*\[(.+?)\]', full_text)
        if match:
            fthr_term = match.group(1)
            codigo = match.group(2)
            result["PLMCode"] = codigo
            print(f"Father Term: {fthr_term}, Código: {codigo}")
        else:
            print("No se encontró el patrón esperado en el texto")
    
    # Descripción principal como HTML
    desc_rubro = soup.find("p", class_="RubroMaestro", string=lambda t: t and "Descripción" in t)
    if desc_rubro:
        desc_normal = desc_rubro.find_next_sibling("p", class_="Normal")
        if desc_normal:
            print(f"desc_normal: {desc_normal}")
            result["Description"] = str(desc_normal)
    
    # ========== MedicalEncyclopediaAttribute ==========
    
    # Process Entries Terms
    # TODO: Buscar Term padre, si existe en MedicalTerm y recuperar ID
    term_id = 1
    added_mdcl = set()
    # Estructura para MedicalTerm [TermId, Term, NormalizedTerm]
    medical_term = [{"TermId": term_id, "Term": fthr_term, "NormalizedTerm": fthr_term.lower()}]
    medical_term_one = []
    medical_term_two = []
    added_mdcl.add(fthr_term)
    mdcl_encyclpd_trm = []
    mdcl_entries_terms = []
    # RubroMaestro detail
    ubro_soup = soup.find_all("p", class_="RubroMaestro")
    for tag in ubro_soup:
        label = tag.get_text(strip=True).replace(":", "")
        et_type_id = get_entryterm_type_id(label)
        if et_type_id:
            tbl = str.maketrans("áéíóú", "aeiou")
            str_lbl = label.translate(tbl)
            print(f"label: {str_lbl}")
            terms_span = soup.find('span', class_=get_entryterm_by_id(et_type_id))
            print(f"terms_span: {terms_span}")
            extrctd_txt = terms_span.get_text(strip=True)
            labels = [s.strip() for s in extrctd_txt.split('|')]
            count = 1
            for strp_lbl in labels:
                if (strp_lbl not in added_mdcl):
                    term_id = term_id+1
                    print(f"count: {count}, et_type_id: {et_type_id}, strp_lbl: {strp_lbl}")
                    # Estructura para MedicalTerm [TermId, Term, NormalizedTerm]
                    if et_type_id == 1:
                        medical_term_one.append({
                            "TermId": term_id,
                            "Term": strp_lbl,
                            "NormalizedTerm": strp_lbl.lower()
                        })
                    elif et_type_id == 2:
                        medical_term_two.append({
                            "TermId": term_id,
                            "Term": strp_lbl,
                            "NormalizedTerm": strp_lbl.lower()
                        })
                    added_mdcl.add(strp_lbl)
                    count = count+1
                elif strp_lbl == fthr_term:
                    # Estructura para MedicalEncyclopediaTerm [EncyclopediaId, TermId]
                    mdcl_encyclpd_trm.append({"EncyclopediaId": 1, "TermId": 1})
    incrmt = 0
    for cnt_one, mdcl_trm_one in enumerate(medical_term_one):
        incrmt = cnt_one
        # Estructura para MedicalEntryTerm [TermId, EntryTermId, EntryTermTypeId, IsPrimary]
        mdcl_entries_terms.append({"TermId": mdcl_trm_one["TermId"], "EntryTermId": cnt_one+1, "EntryTermTypeId": 1, "IsPrimary": False})
    cnt_inc = 1
    for cnt_two, mdcl_trm_two in enumerate(medical_term_two):
        cnt_inc = incrmt + cnt_two
        # Estructura para MedicalEntryTerm [TermId, EntryTermId, EntryTermTypeId, IsPrimary]
        mdcl_entries_terms.append({"TermId": mdcl_trm_two["TermId"], "EntryTermId": cnt_inc+1, "EntryTermTypeId": 2, "IsPrimary": False})
    print(f"medical_term: {medical_term}")
    #medical_term.extend(medical_term_one)
    medical_term = medical_term + medical_term_one
    mdcl_ntrs_trms["MedicalTerm"] = medical_term + medical_term_two
    mdcl_ntrs_trms["MedicalEncyclopediaTerm"] = mdcl_encyclpd_trm
    mdcl_ntrs_trms["MedicalEntryTerm"] = mdcl_entries_terms
    result["MedicalEntriesTerms"] = mdcl_ntrs_trms
    
    # Process Attributes
    attributes = []
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
                    "EncyclopediaId": current_encyclopedia_id,
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
