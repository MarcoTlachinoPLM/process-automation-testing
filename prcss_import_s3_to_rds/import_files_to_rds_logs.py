import os
import re
import pyodbc
import configparser
from bs4 import BeautifulSoup
from datetime import datetime

# Configuración de la base de datos SQL Server
current_dir = os.path.dirname(os.path.abspath(__file__))
config = configparser.ConfigParser()
config.read(os.path.join(current_dir, '..', 'config.ini'))
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

# Mapeo de grupos de atributos
main_mdcl_attr_grp = {}

fltrd_mdcl_attr_grp = {}

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
def gt_cat_entry_term_type(conn):
    """Recupera diccionario de EntryTermType."""
    cat_et_type = {}
    with conn.cursor() as cursor:
        cursor.execute("SELECT EntryTermTypeId, TypeName FROM EntryTermType;")
        for row in cursor.fetchall():
            id_value = row[0]
            nombre_value = row[1]
            cat_et_type[nombre_value] = id_value
        return cat_et_type

def gt_main_medical_attribute(conn):
    """Recupera diccionario para MedicalAttributeGroup."""
    with conn.cursor() as cursor:
        cursor.execute("SELECT AttributeGroupId, AttributeGroupName FROM MedicalAttributeGroup WHERE AttributeGroupName IN ('Descripción', 'Sinónimos', 'Palabras clave');")
        for row in cursor.fetchall():
            id_value = row[0]
            nombre_value = row[1]
            main_mdcl_attr_grp[nombre_value] = id_value
        return True

def gt_fltrd_medical_attribute(conn):
    """Recupera diccionario para MedicalAttributeGroup."""
    with conn.cursor() as cursor:
        cursor.execute("SELECT AttributeGroupId, AttributeGroupName FROM MedicalAttributeGroup WHERE AttributeGroupName NOT IN ('Descripción', 'Sinónimos', 'Palabras clave');")
        for row in cursor.fetchall():
            id_value = row[0]
            nombre_value = row[1]
            fltrd_mdcl_attr_grp[nombre_value] = id_value
        return True
# ======= DB GET CATALOGS =======

def get_medical_attribute_main(label):
    lower_label = label.lower()
    for attr in main_mdcl_attr_grp:
        if attr.lower() in lower_label:
            return main_mdcl_attr_grp[attr]
    return None

def get_medical_attribute_fltrd(label):
    lower_label = label.lower()
    for attr in fltrd_mdcl_attr_grp:
        if attr.lower() in lower_label:
            return fltrd_mdcl_attr_grp[attr]
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
        tago_codigo = map_encyclopedia_tags.get("PLMCode")
        codigo_span = main_tag.find("span", class_=tago_codigo)
        if codigo_span:
            full_text = codigo_span.get_text(strip=True)
            match = re.match(r'(.+?)\s*\[(.+?)\]', full_text)
            if match:
                fthr_term = match.group(1)
                codigo = match.group(2)
                result["PLMCode"] = codigo
                print(f"Father Term: {fthr_term}, Código: {codigo}")
            else:
                raise CustomException("No se encontró el patrón esperado en el texto")
        else:
            raise CustomException("No se encontró span con class='Codigo' dentro de <p class='h1'>")
    else:
        raise CustomException("No se encontró elemento <p class='h1'> que contiene el código PLM")
    
    # Set Title from Father Term
    result["EncyclopediaName"] = fthr_term
    
    # TODO: Extraer el ID del elemento <body>
    """
    body_tag = soup.find('body')
    if body_tag:
        body_id = body_tag.get('id')
    # Extraer el ID del elemento <div>
    div_tag = soup.find('div')
    if div_tag:
        div_id = div_tag.get('id')
    if body_id and div_id:
        print(f"body_id: {body_id}, div_id: {div_id}")
        result["MedicalEncyclopediaBodyPartPlane"] = {"BodyPartId": body_id, "BodyPlaneId": div_id}
    """
    # Descripción principal como HTML
    tag_master = map_encyclopedia_tags.get("Descripción")
    print(f"tag_master: {tag_master}")
    desc_rubro = soup.find("p", class_=tag_master, string=lambda t: t and "Descripción" in t)
    if desc_rubro:
        print(f"desc_rubro: {desc_rubro}")
        desc_normal = desc_rubro.find_next_sibling("p", class_="Normal")
        if desc_normal:
            desc_normal = html_to_text(desc_normal)
            result["Description"] = str(desc_normal)
    # ========== MedicalEncyclopediaAttribute ==========
    
    # Process Entries Terms
    added_mdcl = set()
    # Estructura para MedicalTerm [Term, NormalizedTerm]
    medical_term = [{"TermId": 0, "Term": fthr_term, "NormalizedTerm": fthr_term.lower(), "TypeId": 1, "IsPrimary": 1}]
    added_mdcl.add(fthr_term)
    
    # RubroMaestro detail
    tag_attr = map_encyclopedia_tags.get("Attributes")
    rub_master = soup.find_all("p", class_=tag_attr)
    # Process Attributes
    attributes = []
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
            print(f"html_content: {html_content}")
            attribute_id = get_medical_attribute_main(label)
            print(f"attribute_id: {attribute_id}, label: {label}")
            if attribute_id:
                attributes.append({
                    "AttributeGroupId": attribute_id,
                    "Content": "",
                    "HTMLContent": html_content.strip()
                })
        print(f"get_entryterm_type()...")
        et_type_str = get_entryterm_type(label)
        if et_type_str:
            print(f"et_type_str: {et_type_str}")
            tbl = str.maketrans("áéíóú", "aeiou")
            str_lbl = label.translate(tbl)
            print(f"label: {label}, str_lbl: {str_lbl}")
            str_class = get_entryterm_class(label)
            print(f"str_class: {str_class}")
            terms_span = soup.find('span', class_=str_class)
            print(f"terms_span: {terms_span}")
            extrctd_txt = terms_span.get_text(strip=True)
            labels = [s.strip() for s in extrctd_txt.split('|')]
            for strp_lbl in labels:
                if strp_lbl not in added_mdcl:
                    print(f"et_type_str: {et_type_str}, strp_lbl: {strp_lbl}")
                    # Estructura para MedicalTerm [Term, NormalizedTerm]
                    medical_term.append({
                        "Term": strp_lbl,
                        "NormalizedTerm": strp_lbl.lower(),
                        "TermType": et_type_str,
                        "IsPrimary": 0
                    })
                    added_mdcl.add(strp_lbl)
    
    # Combine all terms
    result["MedicalEntriesTerms"] = medical_term
    
    attributes_not_found = set()
    # More Attributes
    rubroenc = map_encyclopedia_tags.get("HTMLContent")
    rub_enc = soup.find_all("p", class_=rubroenc)
    for tag in rub_enc:
        span = tag.find("span", class_="h2")
        if span:
            label = span.get_text(strip=True).replace(":", "")
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
                #TODO: Validar si hay que agregarlo, o se debe crear antes en el catalogo...
                print(f"New label: {label}")
                attributes_not_found.add(label)
    list_not_found = list(attributes_not_found)
    if len(list_not_found) > 0:
        raise CustomException(f"Uno o varios atributos aun no estan dados de alta:{list_not_found}")

    result["MedicalEncyclopediaAttribute"] = list(attributes)
    
    return result
# ========== EXTRACCION DE DATOS ==========

# ========== DB SECELT, INSERT FUNCIONS ==========
def get_encyclopedia_data(conn, data):
    with conn.cursor() as cursor:
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

def insert_attributes(conn, encyclopedia_id, attributes):
    """Inserta ó actualiza los atributos relacionados con una enciclopedia"""
    if len(attributes) > 0:
        with conn.cursor() as cursor:
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
            print(f"Values: {values}")
            print(f"INSERT INTO MedicalEncyclopediaAttribute ()...")
            cursor.executemany(query, values)
            conn.commit()

def insert_or_get_encyclopedia_data(conn, data):
    """Inserta datos en la tabla MedicalEncyclopedia y devuelve el ID generado"""
    with conn.cursor() as cursor:
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
            conn.commit()
            return encyclopedia_id

def insert_attributes(conn, encyclopedia_id, attributes):
    """Inserta ó actualiza los atributos relacionados con una enciclopedia"""
    if len(attributes) > 0:
        with conn.cursor() as cursor:
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
            print(f"Values: {values}")
            print(f"INSERT INTO MedicalEncyclopediaAttribute ()...")
            cursor.executemany(query, values)
            conn.commit()

def insert_or_get_term(conn, data):
    """Inserta un término si no existe o devuelve el ID existente"""
    with conn.cursor() as cursor:
        # Check if term exists
        print(f"NormalizedTerm: {data["NormalizedTerm"]}")
        cursor.execute(
            "SELECT TermId FROM MedicalTerm WHERE NormalizedTerm = ?", 
            data["NormalizedTerm"]
        )
        row = cursor.fetchone()
        if row:
            return row[0]
        else:
            query = """
            INSERT INTO MedicalTerm (Term) OUTPUT INSERTED.TermId VALUES (?);
            """
            print(f"data: {data}")
            print(f"INSERT INTO MedicalTerm ({data["Term"]})...")
            cursor.execute(query, (data["Term"]))
            term_id = cursor.fetchone()[0]
            conn.commit()
            return term_id

def insert_medical_entry_term(conn, encycl_id, et_type_id, data):
    """Inserta un término si no existe o devuelve el ID existente"""
    print(f"data: {data}")
    with conn.cursor() as cursor:
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
        conn.commit()
        return

def insert_entries_terms_data(conn, encyclopedia_id, entries_terms):
    """Inserta los términos relacionados con una enciclopedia"""
    if len(entries_terms) > 0:
        with conn.cursor() as cursor:
            # First process MedicalTerm and get all TermIds
            term_ids = set()
            for count, term in enumerate(entries_terms):
                term_id = insert_or_get_term(conn, term)
                term["TermId"] = term_id
                entries_terms[count] = term
                print(f"term_ids.add({term_id})...")
                term_ids.add(term_id)
            
            # Then process MedicalEncyclopediaTerm, the first term is the main term (Father)
            main_term = entries_terms[0]
            main_id = main_term["TermId"]
            print(f"term_ids: {term_ids}")
            if main_id in term_ids:
                print(f"INSERT INTO MedicalEncyclopediaTerm ()...")
                cursor.execute(
                    "INSERT INTO MedicalEncyclopediaTerm (EncyclopediaId, TermId) VALUES (?, ?)",
                    (encyclopedia_id, main_term["TermId"])
                )
                conn.commit()
            else:
                print(f"Error in insert_or_get_term!!!")
            # Finally process MedicalEntryTerm
            ids_list = list(term_ids)
            cat_et_type = gt_cat_entry_term_type(conn)
            ent_trms_ids = set()
            for count, et_data in enumerate(entries_terms):
                if count > 0:
                    et_type_id = cat_et_type.get(et_data["TermType"])
                    if et_type_id:
                        at_id = f"{ids_list[0]}-{et_data["TermId"]}"
                        if at_id not in ent_trms_ids:
                            print(f"ent_trms_ids.add({at_id})")
                            ent_trms_ids.add(at_id)
                            insert_medical_entry_term(conn, ids_list[0], et_type_id, et_data)
                        else:
                            print(f"et_type_id: {et_type_id}")
                    else:
                        print(f"Error al recuperar get_entry_term_type_id!!!")
# ========== DB SECELT, INSERT FUNCIONS ==========

def process_html_file(html_content):
    """Procesa un archivo HTML y lo inserta en la BD"""
    print(f"get_db_connection()...")
    conn = get_db_connection()
    complete = False
    try:
        # Inicializar Catalogos
        gt_main_medical_attribute(conn)
        gt_fltrd_medical_attribute(conn)
        
        # Extraer datos del HTML
        data = extract_data_from_html_local(html_content)
        
        encyclopedia_id = get_encyclopedia_data(conn, data)
        if encyclopedia_id:
            print(f"Se encontraron datos existentes para EncyclopediaId: {encyclopedia_id}")
        else:
            # Insert Encyclopedia data and get the generated ID
            encyclopedia_id = insert_or_get_encyclopedia_data(conn, data)
            print(f"Inserted Encyclopedia with ID: {encyclopedia_id}")

            # Insert Attributes
            if data["MedicalEncyclopediaAttribute"]:
                insert_attributes(conn, encyclopedia_id, data["MedicalEncyclopediaAttribute"])
                print("Inserted Attributes successfully")
            
            # Insert MedicalTerms and relationships
            if data["MedicalEntriesTerms"]:
                insert_entries_terms_data(conn, encyclopedia_id, data["MedicalEntriesTerms"])
                print("Inserted MedicalTerms and relationships successfully")

            complete = True
            print(f"Datos insertados correctamente. EncyclopediaId: {encyclopedia_id}")
    except Exception as e:
        conn.rollback()
        print(f"Error al procesar archivo: {e}")
    finally:
        conn.close()
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
    processed_dir = os.path.join(current_dir, '..', movepath)
    if not os.path.exists(processed_dir):
        os.makedirs(processed_dir)
    
    filename = os.path.basename(filepath)
    destination = os.path.join(processed_dir, filename)
    os.rename(filepath, destination)
    print(f"Archivo movido a: {destination}")

if __name__ == "__main__":
    try:
        input_dir = os.path.join(current_dir, '..', path_files)
        
        print(f"Procesando archivos desde directorio local: {input_dir}")
        for html_file in list_html_files_local(input_dir):
            print(f"Procesando archivo: {html_file}")
            html_content = read_html_file(html_file)
            if process_html_file(html_content):
                move_file_to_processed(html_file, path_move)
            else:
                move_file_to_processed(html_file, path_error_move)
    except Exception as e:
        print(f"Error en el procesamiento de archivos locales: {e}")
