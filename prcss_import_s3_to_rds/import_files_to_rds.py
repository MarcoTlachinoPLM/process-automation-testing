import os
import re
import pyodbc
import configparser
from bs4 import BeautifulSoup
from datetime import datetime

# Configuración de la base de datos SQL Server
current_dir = os.path.dirname(os.path.abspath(__file__))
config = configparser.ConfigParser()
config.read(os.path.join(current_dir, 'config.ini'))
path_files = 'data_import_files'
path_move = 'data_processed_files'
path_error_move = 'data_error_files'

DB_CONFIG = {
    "server": config["database"]["server"],
    "database": config["database"]["database"],
    "username": config["database"]["username"],
    "password": config["database"]["password"]
}

def get_db_connection():
    """Establece conexión con SQL Server usando pyodbc"""
    conn_str = (
        f"DRIVER={{ODBC Driver 17 for SQL Server}};"
        f"SERVER={DB_CONFIG['server']};"
        f"DATABASE={DB_CONFIG['database']};"
        f"UID={DB_CONFIG['username']};"
        f"PWD={DB_CONFIG['password']}"
    )
    return pyodbc.connect(conn_str)

# Habilitar/Dehabilitar LOGs
ENABLE_LOGS = True

# Mapeo de grupos de atributos
MAIN_MDCL_ATTR_GRP = {}
FLTRD_MDCL_ATTR_GRP = {}
CAT_ET_TYPE = {}

entry_term_type = {
    "Sinónimos": "Synonym",
    "Palabras clave": "Related Term"
}

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

class CustomException(Exception):
    pass

# ===== DATABASE CONNECTION =====
def get_db_connection():
    """Establece conexión con SQL Server usando pyodbc"""
    conn_str = (
        f"DRIVER={{ODBC Driver 17 for SQL Server}};"
        f"SERVER={DB_CONFIG['server']};"
        f"DATABASE={DB_CONFIG['database']};"
        f"UID={DB_CONFIG['username']};"
        f"PWD={DB_CONFIG['password']}"
    )
    return pyodbc.connect(conn_str)
# ===== DATABASE CONNECTION =====

# ======= DB GET CATALOGS =======
def get_cat_entry_term_type(conn):
    """Recupera diccionario de EntryTermType."""
    with conn.cursor() as cursor:
        cursor.execute("SELECT EntryTermTypeId, TypeName FROM EntryTermType;")
        for row in cursor.fetchall():
            id_value = row[0]
            nombre_value = row[1]
            CAT_ET_TYPE[nombre_value] = id_value

def get_main_medical_attribute(conn):
    """Recupera diccionario para MedicalAttributeGroup."""
    with conn.cursor() as cursor:
        cursor.execute(
            "SELECT AttributeGroupId, AttributeGroupName FROM MedicalAttributeGroup WHERE AttributeGroupName IN ('Descripción', 'Sinónimos', 'Palabras clave');"
        )
        for row in cursor.fetchall():
            id_value = row[0]
            nombre_value = row[1]
            MAIN_MDCL_ATTR_GRP[nombre_value] = id_value
    return True

def get_fltrd_medical_attribute(conn):
    """Recupera diccionario para MedicalAttributeGroup."""
    with conn.cursor() as cursor:
        cursor.execute(
            "SELECT AttributeGroupId, AttributeGroupName FROM MedicalAttributeGroup WHERE AttributeGroupName NOT IN ('Descripción', 'Sinónimos', 'Palabras clave');"
        )
        for row in cursor.fetchall():
            id_value = row[0]
            nombre_value = row[1]
            FLTRD_MDCL_ATTR_GRP[nombre_value] = id_value
    return True
# ======= DB GET CATALOGS =======

def get_medical_attribute_main(label):
    lower_label = label.lower()
    for attr in MAIN_MDCL_ATTR_GRP:
        if attr.lower() in lower_label:
            return MAIN_MDCL_ATTR_GRP[attr]
    return None

def get_medical_attribute_fltrd(label):
    lower_label = label.lower()
    for attr in FLTRD_MDCL_ATTR_GRP:
        if attr.lower() in lower_label:
            return FLTRD_MDCL_ATTR_GRP[attr]
    return None

def get_entryterm_type(label):
    lower_label = label.lower()
    for key in entry_term_type:
        if key.lower() in lower_label:
            return entry_term_type[key]
    return None

def get_entryterm_class(label):
    lower_label = label.lower()
    for key in cat_term_type:
        if key.lower() in lower_label:
            return cat_term_type[key]
    return None

def html_to_text(html_text):
    # Hacer una copia para no modificar el original
    tmp_copy = BeautifulSoup(str(html_text), 'html.parser')
    # Obtener todo el texto
    text = tmp_copy.get_text(separator=' ', strip=True)
    # Limpiar espacios múltiples y saltos de línea
    text = ' '.join(text.split())
    return text

def print_log(message):
    if ENABLE_LOGS:
        print(message)

# ========== EXTRACCION DE DATOS ==========
def extract_data_from_html_local(html_content):
    soup = BeautifulSoup(html_content, "html.parser")

    # ========== ESTRUCTURA PRINCIPAL ==========
    result = {
        "PLMCode": "",
        "EncyclopediaName": "",
        "Description": "",
        "ReadingTime": "",
        "EncyclopediaImage": "",
        "EncyclopediaTypeId": 1,
        "AuthorId": "",
        "Active": 1,
        "MedicalEncyclopediaBodyPartPlane": {},
        "MedicalEncyclopediaAttribute": [],
        "MedicalEntriesTerms": []
    }
    
    # Título
    #title_tag = soup.find(map_encyclopedia_tags.get("EncyclopediaName"))
    #result["EncyclopediaName"] = title_tag.get_text(strip=True) if title_tag else ""
    
    # PLMCode
    fthr_term = ""
    main_tag = soup.find("p", class_="h1")
    if main_tag:
        tag_codigo = map_encyclopedia_tags.get("PLMCode")
        print_log(f"tag_codigo: {tag_codigo}")
        codigo_span = main_tag.find("span", class_=tag_codigo)
        if codigo_span:
            full_text = codigo_span.get_text(strip=True)
            match = re.match(r'(.+?)\s*\[(.+?)\]', full_text)
            if match:
                fthr_term = match.group(1)
                codigo = match.group(2)
                result["PLMCode"] = codigo
                print_log(f"Father Term: {fthr_term}, Código: {codigo}")
            else:
                raise CustomException("No se encontró el patrón esperado en el texto")
        else:
            raise CustomException("No se encontró span con class='Codigo' dentro de <p class='h1'>")
    else:
        raise CustomException("No se encontró elemento <p class='h1'> que contiene el código PLM")
    
    # Set Title from Father Term
    result["EncyclopediaName"] = fthr_term
    
    # Descripción principal como HTML
    tag_master = map_encyclopedia_tags.get("Descripción")
    print_log(f"tag_master: {tag_master}")
    desc_rubro = soup.find("p", class_=tag_master, string=lambda t: t and "Descripción" in t)
    if desc_rubro:
        print_log(f"desc_rubro: {desc_rubro}")
        desc_normal = desc_rubro.find_next_sibling("p", class_="Normal")
        if desc_normal:
            desc_normal = html_to_text(desc_normal)
            result["Description"] = str(desc_normal)
    # ========== MedicalEncyclopediaAttribute ==========
    
    # Process Entries Terms
    added_mdcl = set()
    # Estructura para MedicalTerm [Term, NormalizedTerm]
    medical_term = [{"TermId": 0, "Term": fthr_term, "TypeId": 1, "IsPrimary": 1}]
    added_mdcl.add(fthr_term)
    
    # RubroMaestro detail
    tag_attr = map_encyclopedia_tags.get("Attributes")
    print_log(f"tag_attr: {tag_attr}")
    rub_master = soup.find_all("p", class_=tag_attr)
    print_log(f"rub_master: {len(rub_master)}")
    # Process Attributes
    attributes = []
    for tag in rub_master:
        label = tag.get_text(strip=True).replace(":", "")
        html_content = ''
        pointer = tag.find_next_sibling("p", class_="Normal")
        str_pnter = str(pointer)
        print_log(f"str_pnter: {str_pnter}")
        cntns_synnyms = "</span><span class=\"Sinonimos\">"
        cntns_kywrds = "</span><span class=\"PalabrasClave\">"
        if cntns_synnyms in str_pnter:
            raise CustomException(f"Error en atributo: Sinonimos, se enconraron multiple {cntns_synnyms} en HTML: {str_pnter}")
        elif cntns_kywrds in str_pnter:
            raise CustomException(f"Error en atributo: PalabrasClave, se enconraron multiple {cntns_kywrds} en HTML: {str_pnter}")
        while pointer and not (
            pointer.name == 'p' and pointer.get('class') in [['hr'], [tag_master]]
        ):
            html_content += str(pointer)
            pointer = pointer.find_next_sibling()
        if html_content:
            print_log(f"html_content: {html_content}")
            attribute_id = get_medical_attribute_main(label)
            print_log(f"attribute_id: {attribute_id}, label: {label}")
            if attribute_id:
                attributes.append({
                    "AttributeGroupId": attribute_id,
                    "Content": "",
                    "HTMLContent": html_content.strip()
                })
        print_log(f"get_entryterm_type({label})...")
        et_type_str = get_entryterm_type(label)
        if et_type_str:
            print_log(f"et_type_str: {et_type_str}")
            tbl = str.maketrans("áéíóú", "aeiou")
            str_lbl = label.translate(tbl)
            print_log(f"label: {label}, str_lbl: {str_lbl}")
            str_class = get_entryterm_class(label)
            print_log(f"str_class: {str_class}")
            terms_span = soup.find_all('span', class_=str_class)
            print_log(f"terms_span: {len(terms_span)}")
            #extrctd_txt = trm_span.get_text(strip=True)
            extrctd_txt = ""
            for trm_spn in terms_span:
                extrctd_txt = extrctd_txt + trm_spn.get_text(strip=True)
            labels = [s.strip() for s in extrctd_txt.split('|')]
            print_log(f"labels: {labels}")
            substr_replace = f"</span><span class=\"{str_class}\">"
            for strp_lbl in labels:
                if "</span><span" in strp_lbl:
                    strp_lbl = strp_lbl.replace(substr_replace, '')
                if strp_lbl and strp_lbl not in added_mdcl:
                    print_log(f"et_type_str: {et_type_str}, strp_lbl: {strp_lbl}")
                    # Estructura para MedicalTerm [Term, NormalizedTerm]
                    medical_term.append({
                        "Term": strp_lbl,
                        "TermType": et_type_str,
                        "IsPrimary": 0
                    })
                    added_mdcl.add(strp_lbl)
    
    # Combine all terms
    result["MedicalEntriesTerms"] = medical_term
    
    attributes_not_found = set()
    # More Attributes
    rubroenc = map_encyclopedia_tags.get("HTMLContent")
    print_log(f"rubroenc: {rubroenc}")
    rub_enc = soup.find_all("p", class_=rubroenc)
    print_log(f"rub_enc: {len(rub_enc)}")
    for tag in rub_enc:
        print_log(f"tag: {tag}")
        str_tag = str(tag)
        print_log(f"str_tag: {str_tag}")
        cntns_sbstr = f"</span><span class=\"h2\">"
        if cntns_sbstr in str_tag:
            raise CustomException(f"Error en atributo: {str_tag}, contiene multiples: {cntns_sbstr}")
        span = tag.get_text(strip=True)
        if span:
            label = span.replace(":", "")
            print(f"label: {label}")
            attribute_id = get_medical_attribute_fltrd(label)
            if attribute_id:
                html_content = ''
                pointer = tag.find_next_sibling()
                while pointer and not (
                    pointer.name == 'p' and pointer.get('class') in [['hr'], [rubroenc]]
                ):
                    html_content += str(pointer)
                    pointer = pointer.find_next_sibling()
                attributes.append({
                    "AttributeGroupId": attribute_id,
                    "Content": "",
                    "HTMLContent": html_content.strip()
                })
            else:
                print(f"New label: {label}")
                attributes_not_found.add(label)
    list_not_found = list(attributes_not_found)
    if len(list_not_found) > 0:
        raise CustomException(f"Uno o varios atributos aun no estan dados de alta:{list_not_found}")

    result["MedicalEncyclopediaAttribute"] = list(attributes)
    
    return result
# ========== EXTRACCION DE DATOS ==========

# ========== DB SECELT, INSERT FUNCIONS ==========
def get_encyclopedia_data(cursor, data):
    # Check if exists
    query_select = """
        SELECT EncyclopediaId FROM MedicalEncyclopedia
        WHERE PLMCode = ? AND EncyclopediaName = ?;
    """
    cursor.execute(
        query_select, 
        (data["PLMCode"], data["EncyclopediaName"])
    )
    row = cursor.fetchone()
    if row:
        return row[0]
    else:
        return None

def insert_or_get_encyclopedia_data(cursor, data):
    """Inserta datos en la tabla MedicalEncyclopedia y devuelve el ID generado"""
    # Check if exists
    query_select = """
        SELECT EncyclopediaId FROM MedicalEncyclopedia
        WHERE PLMCode = ? AND EncyclopediaName = ?;
    """
    cursor.execute(
        query_select,
        (data["PLMCode"], data["EncyclopediaName"])
    )
    row = cursor.fetchone()
    if row:
        return row[0]
    else:
        query_insert = """
            INSERT INTO MedicalEncyclopedia (
                PLMCode, EncyclopediaName, Description, ReadingTime, 
                EncyclopediaImage, EncyclopediaTypeId, Active, LastUpdate
            ) 
            OUTPUT INSERTED.EncyclopediaId
            VALUES (?, ?, ?, ?, ?, ?, ?, ?);
        """
        print(f"INSERT INTO MedicalEncyclopedia ()...")
        cursor.execute(query_insert, (
            data["PLMCode"],
            data["EncyclopediaName"],
            data["Description"],
            data["ReadingTime"],
            data["EncyclopediaImage"],
            data["EncyclopediaTypeId"],
            data["Active"],
            datetime.now()
        ))
        encyclopedia_id = cursor.fetchone()[0]
        return encyclopedia_id

def insert_attributes(cursor, encyclopedia_id, attributes):
    """Inserta ó actualiza los atributos relacionados con una enciclopedia"""
    if len(attributes) > 0:
        query = """
            INSERT INTO MedicalEncyclopediaAttribute (
                AttributeGroupId, EncyclopediaId, Content, HTMLContent
            ) VALUES (?, ?, ?, ?);
        """
        # Preparar todos los valores para ejecutarlos en un solo execute_many
        values = [
            (
                attr["AttributeGroupId"],
                encyclopedia_id,
                attr["Content"],
                attr["HTMLContent"]
            )
            for attr in attributes
        ]
        print_log(f"Values: {values}")
        print(f"INSERT INTO MedicalEncyclopediaAttribute ()...")
        cursor.executemany(query, values)

def insert_or_get_term(cursor, data):
    """Inserta un término si no existe o devuelve el ID existente"""
    # Check if term exists
    nrmlzd_trm = str(data["Term"]).lower()
    print_log(f"NormalizedTerm: {nrmlzd_trm}")
    cursor.execute(
        "SELECT TermId FROM MedicalTerm WHERE NormalizedTerm = ?", nrmlzd_trm
    )
    row = cursor.fetchone()
    if row:
        return row[0]
    else:
        query = """
            INSERT INTO MedicalTerm (Term) OUTPUT INSERTED.TermId VALUES (?);
        """
        print_log(f"data: {data}")
        print(f"INSERT INTO MedicalTerm ({data["Term"]})...")
        cursor.execute(query, (data["Term"]))
        term_id = cursor.fetchone()[0]
        return term_id

def insert_medical_entry_term(cursor, encycl_id, et_type_id, data):
    """Inserta un término si no existe o devuelve el ID existente"""
    print_log(f"data: {data}")
    query_met = """
        INSERT INTO MedicalEntryTerm (
            TermId, EntryTermId, EntryTermTypeId, IsPrimary
        ) VALUES (?, ?, ?, ?);
    """
    print(f"INSERT INTO MedicalEntryTerm (...) VALUES({encycl_id}, {data["TermId"]}, {et_type_id}, {data["IsPrimary"]})")
    cursor.execute(
        query_met,
        (encycl_id, data["TermId"], et_type_id, data["IsPrimary"])
    )
    return

def insert_entries_terms_data(cursor, encyclopedia_id, entries_terms):
    """Inserta los términos relacionados con una enciclopedia"""
    if len(entries_terms) > 0:
        # First process MedicalTerm and get all TermIds
        term_ids = set()
        fthr_id = 0
        for count, term in enumerate(entries_terms):
            if len(term) > 2:
                is_prmry = term["IsPrimary"]
                term_id = insert_or_get_term(cursor, term)
                term["TermId"] = term_id
                if is_prmry:
                    fthr_id = term_id
                entries_terms[count] = term
                print_log(f"term_ids.add({term_id})...")
                term_ids.add(term_id)
            else:
                print_log(f"Invalid term: {term_id} !!!")
        
        # Then process MedicalEncyclopediaTerm, the first term is the main term (Father)
        main_term = entries_terms[0]
        main_id = main_term["TermId"]
        print_log(f"term_ids: {term_ids}")
        if main_id in term_ids:
            print(f"INSERT INTO MedicalEncyclopediaTerm ()...")
            cursor.execute(
                "INSERT INTO MedicalEncyclopediaTerm (EncyclopediaId, TermId) VALUES (?, ?)",
                (encyclopedia_id, main_term["TermId"])
            )
        else:
            print(f"Error in insert_or_get_term!!!")
            raise CustomException(f"Error in insert_or_get_term, encyclopedia_id: {encyclopedia_id}")
        # Finally process MedicalEntryTerm
        lst_trm_ids = list(term_ids)
        print_log(f"fthr_id: {fthr_id}, lst_trm_ids: {lst_trm_ids}")
        ent_trms_ids = set()
        for count, et_data in enumerate(entries_terms):
            if count > 0:
                et_type_id = CAT_ET_TYPE.get(et_data["TermType"])
                if et_type_id:
                    at_id = f"{fthr_id}-{et_data["TermId"]}"
                    print_log(f"at_id: {at_id}, ent_trms_ids: {list(ent_trms_ids)}")
                    if at_id not in ent_trms_ids:
                        print_log(f"ent_trms_ids.add({at_id})")
                        ent_trms_ids.add(at_id)
                        insert_medical_entry_term(cursor, fthr_id, et_type_id, et_data)
                    else:
                        print(f"ya existe et_type_id: {et_type_id}")
                else:
                    print(f"Error al recuperar get_entry_term_type_id!!!")
                    raise CustomException(f"Error al recuperar get_entry_term_type_id, encyclopedia_id: {encyclopedia_id}")
# ========== DB SECELT, INSERT FUNCIONS ==========

def process_html_file(conn, html_content):
    """Procesa un archivo HTML y lo inserta en la BD"""
    complete = False
    cursor = conn.cursor()
    try:
        # Extraer datos del HTML
        data = extract_data_from_html_local(html_content)
        
        encyclopedia_id = get_encyclopedia_data(cursor, data)
        if encyclopedia_id:
            print(f"Se encontraron datos existentes para EncyclopediaId: {encyclopedia_id}")
        else:
            # Insert Encyclopedia data and get the generated ID
            encyclopedia_id = insert_or_get_encyclopedia_data(cursor, data)
            print(f"Inserted Encyclopedia with ID: {encyclopedia_id}")

            # Insert Attributes
            if data["MedicalEncyclopediaAttribute"]:
                insert_attributes(cursor, encyclopedia_id, data["MedicalEncyclopediaAttribute"])
                print("Inserted Attributes successfully")
            
            # Insert MedicalTerms and relationships
            if data["MedicalEntriesTerms"]:
                insert_entries_terms_data(cursor, encyclopedia_id, data["MedicalEntriesTerms"])
                print("Inserted MedicalTerms and relationships successfully")
            
            complete = True
            print(f"Datos insertados correctamente. EncyclopediaId: {encyclopedia_id}")
    except Exception as e:
        print(f"conn.rollback()...")
        conn.rollback()
        print(f"Error al procesar archivo: {e}")
    finally:
        return complete

def list_html_files_local(directory):
    """Lista archivos HTML en un directorio local"""
    for filename in os.listdir(directory):
        if filename.endswith(".html"):
            yield os.path.join(directory, filename)

def read_html_file(filepath):
    """Lee el contenido de un archivo HTML local"""
    with open(filepath, 'r', encoding='utf-8') as file:
        return file.read()

def move_file_to_processed(filepath, movepath):
    """Mueve archivo procesado a la carpeta 'processed'"""
    processed_dir = os.path.join(current_dir, movepath)
    if not os.path.exists(processed_dir):
        os.makedirs(processed_dir)
    
    filename = os.path.basename(filepath)
    destination = os.path.join(processed_dir, filename)
    os.rename(filepath, destination)
    print(f"Archivo movido a: {destination}")


def main():
    print(f"get_db_connection()...")
    conn = get_db_connection()
    try:
        # Inicializar Catalogos
        get_main_medical_attribute(conn)
        get_fltrd_medical_attribute(conn)
        get_cat_entry_term_type(conn)
        input_dir = os.path.join(current_dir, path_files)
        print(f"Procesando archivos desde directorio local: {input_dir}")
        for html_file in list_html_files_local(input_dir):
            print(f"Procesando archivo: {html_file}")
            html_content = read_html_file(html_file)
            if process_html_file(conn,html_content):
                print_log(f"* conn.commit()...")
                conn.commit()
                move_file_to_processed(html_file, path_move)
            else:
                move_file_to_processed(html_file, path_error_move)
        print("✅ Ingesta completada correctamente.")
    except Exception as e:
        print(f"❌ Error en el procesamiento de archivos: {e}")
    finally:
        conn.close()

if __name__ == "__main__":
    main()
