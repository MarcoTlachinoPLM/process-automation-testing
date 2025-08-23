import os
import re
from bs4 import BeautifulSoup
from datetime import datetime
# Importar funciones generales
from utils_tools import (
    normalize_string,
    tags_cln,
    print_log,
    init_cat_encyclopedias,
    get_plm_code_by_encycl_name
)
# Importar funciones para DB
from database_utils import (
    get_db_connection,
    get_catalogs_import,
    map_encyclopedia_tags,
    get_medical_attribute_fltrd,
    get_entryterm_type,
    get_entryterm_class
)

# Directorio donde están los archivos con errores
current_dir = os.path.dirname(os.path.abspath(__file__))
path_error = 'data_error_files'
path_files = 'data_import_files'
report_file = 'ErrorReport.txt'
catalog_name = 'catalog_encyclopedias.json'
path_catalogs = 'catalogs'

def move_file_to_processed(filepath, movepath):
    """Mueve archivo procesado a la carpeta 'processed'"""
    processed_dir = os.path.join(current_dir, movepath)
    if not os.path.exists(processed_dir):
        os.makedirs(processed_dir)
    filename = os.path.basename(filepath)
    destination = os.path.join(processed_dir, filename)
    os.rename(filepath, destination)
    print_log(f"Archivo movido a: {destination}")

def analizar_html(filepath):
    errores = []
    print_log(f"filepath: {filepath}")
    with open(filepath, 'r', encoding='utf-8') as file:
        html_content = file.read()
    
    soup = BeautifulSoup(html_content, "html.parser")
    
    # === Validaciones ===
    # 1. <p class="h1"> con <span class="Codigo"> y patrón esperado
    main_tag = soup.find("p", class_="h1")
    if not main_tag:
        errores.append("No se encontró elemento <p class='h1'> que contiene el PLMCode")
    else:
        tag_codigo = map_encyclopedia_tags.get("PLMCode")
        codigo_span = main_tag.find("span", class_=tag_codigo)
        print_log(f"codigo_span: {codigo_span}")
        try:
            if not codigo_span:
                fthr_term = main_tag.get_text(strip=True)
                print_log(f"get_plm_code_by_encycl_name({fthr_term}")
                str_code = get_plm_code_by_encycl_name(normalize_string(fthr_term))
                print_log(f"str_code: {str_code}")
                if not str_code:
                    errores.append(f"No se encontró 'Codigo' para {fthr_term} dentro de <p class='h1'>")
            else:
                full_text = codigo_span.get_text(strip=True)
                print_log(f"full_text: {full_text}")
                match = re.match(r'(.+?)\s*\[(.+?)\]', full_text)
                if match:
                    fthr_term = match.group(1)
                    mtch_code = match.group(2)
                    print_log(f"mtch_code: {mtch_code}")
                    if not mtch_code:
                        errores.append(f"Enciclopedia '{fthr_term}', no cuenta con PLMCode.")
                else:
                    print_log(f"get_plm_code_by_encycl_name({full_text})")
                    code = get_plm_code_by_encycl_name(normalize_string(full_text))
                    print_log(f"code: {code}")
                    if not code:
                        errores.append(f"Enciclopedia '{fthr_term}', no cuenta con PLMCode.")
        except Exception as e:
            print_log(f"Error al procesar archivo: {e}")
            errores.append(f"{e}")
    
    # 2. Atributos con múltiples sinonimos o palabras clave
    tag_master = map_encyclopedia_tags.get("Descripción")
    print_log(f"tag_master: {tag_master}")
    tag_attr = map_encyclopedia_tags.get("Attributes")
    rub_master = soup.find_all("p", class_=tag_attr)
    print_log(f"rub_master: {rub_master}")
    for tag in rub_master:
        label = tag.get_text(strip=True).replace(":", "")
        html_content = ''
        pointer = tag.find_next_sibling("p", class_="Normal")
        while pointer and not (
            pointer.name == 'p' and pointer.get('class') in [['hr'], [tag_master]]
        ):
            html_content += str(pointer)
            pointer = pointer.find_next_sibling()
        if html_content:
            print_log(f"* html_content: {html_content}")
        et_type_str = get_entryterm_type(label)
        if et_type_str:
            str_class = get_entryterm_class(label)
            terms_span = soup.find_all('span', class_=str_class)
            print_log(f"terms_span: {terms_span}")
            extrctd_txt = ""
            for trm_spn in terms_span:
                extrctd_txt = extrctd_txt + trm_spn.get_text(strip=True)
            labels = [s.strip() for s in extrctd_txt.split('|')]
            print_log(f"labels: {labels}")
            substr_replace = f"<span class=\"{str_class}\">"
            sbstr_rplcs = []
            print_log(f"tags_cln: {tags_cln}")
            for tag_cln in tags_cln:
                sbstr_rplcs.append(f"<span class=\"{tag_cln}\">")
            if len(labels) == 0 or '' in labels[0]:
                extrctd_txt = html_content.replace(f"<p class=\"Normal\">", '')
                extrctd_txt = extrctd_txt.replace(substr_replace, '')
                for sbstr_rplc in sbstr_rplcs:
                    extrctd_txt = extrctd_txt.replace(sbstr_rplc, '')
                extrctd_txt = extrctd_txt.replace(f"</span></p>", '')
                extrctd_txt = extrctd_txt.replace(f"</span> </p>", '')
                labels = [s.strip() for s in extrctd_txt.split('|')]
            print_log(f"* labels-len: {len(labels)}")
            for indx, strp_lbl in enumerate(labels):
                print_log(f"indx: {indx}, strp_lbl: {strp_lbl}")
                if "<span class=" in strp_lbl:
                    errores.append(f"Se encontró elemento '<span class=' en: {strp_lbl}")
    
    # 3. Atributos rubroenc con <span class="h2"> repetidos en mismo párrafo
    rubroenc = map_encyclopedia_tags.get("HTMLContent")
    rub_enc = soup.find_all("p", class_=rubroenc)
    print_log(f"rub_enc: {rub_enc}")
    attributes_not_found = set()
    sbstr_init = f"<p class=\"{rubroenc}\"><span class=\"h2\">"
    sbstr_rplc = "</span><span class=\"h2\">"
    sbstr_end = "</span></p>"
    sbstr_end_spc = "</span> </p>"
    for tag in rub_enc:
        str_tag = str(tag)
        if sbstr_init in str_tag:
            str_tag = str_tag.replace(sbstr_init, "")
        if sbstr_rplc in str_tag:
            str_tag = str_tag.replace(sbstr_rplc, "")
        if sbstr_end in str_tag:
            str_tag = str_tag.replace(sbstr_end, "")
        if sbstr_end_spc in str_tag:
            str_tag = str_tag.replace(sbstr_end_spc, "")
        if "<span" in str_tag:
            errores.append(f"Error en atributo: {str_tag}, contiene elemento: '<span'")
        print_log(f"rub_enc: {rub_enc}")
        label = str_tag.replace(":", "")
        attribute_id = get_medical_attribute_fltrd(label)
        if not attribute_id:
            attributes_not_found.add(label)
    list_not_found = list(attributes_not_found)
    if len(list_not_found) > 0:
        errores.append(f"Uno o varios atributos aun no estan dados de alta:{list_not_found}")
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
                else:
                    move_file_to_processed(filepath, path_files)
    print(f"✅ Reporte generado en: {output_file}")

if __name__ == "__main__":
    conn = get_db_connection()
    try:
        # Inicializar Catalogos
        init_cat_encyclopedias()
        get_catalogs_import(conn)
        # Obtener el objeto datetime actual
        now = datetime.now()
        # Formato común (YYYYMMDD-HHMMSS)
        full_format = now.strftime("%Y%m%d-%H%M%S")
        generar_reporte(full_format)
    except Exception as e:
        print(f"❌ Error: {e}")
    finally:
        conn.close()
