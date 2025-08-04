import os
import re
import boto3
import pyodbc
import configparser
from bs4 import BeautifulSoup
from datetime import datetime

# Configuración de AWS S3
S3_CONFIG = {
    "bucket_name": "opensearch-dev",
    "s3_key": "test_files/"
}

s3_client = boto3.client("s3")

# Configuración de la base de datos SQL Server
current_dir = os.path.dirname(os.path.abspath(__file__))
config = configparser.ConfigParser()
config.read(os.path.join(current_dir, '..', 'config.ini'))

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
rubroenc_map = {}

entry_term_type = {
    "Sinónimos": "Synonym",
    "Palabras clave": "Related Term"
}

cat_term_type = {
    "Sinónimos": "Sinonimos",
    "Palabras clave": "PalabrasClave"
}

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

def gt_cat_medical_attribute(conn):
    """Recupera diccionario para MedicalAttributeGroup."""
    with conn.cursor() as cursor:
        cursor.execute("SELECT AttributeGroupId, AttributeGroupName FROM MedicalAttributeGroup;")
        for row in cursor.fetchall():
            id_value = row[0]
            nombre_value = row[1]
            rubroenc_map[nombre_value] = id_value
        return True
# ======= DB GET CATALOGS =======

def get_medical_attribute(label):
    lower_label = label.lower()
    for key in rubroenc_map:
        if key.lower() in lower_label:
            return rubroenc_map[key]
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
    
    # Extraer el ID del elemento <body>
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
    
    # Descripción principal como HTML
    desc_rubro = soup.find("p", class_="RubroMaestro", string=lambda t: t and "Descripción" in t)
    if desc_rubro:
        desc_normal = desc_rubro.find_next_sibling("p", class_="Normal")
        if desc_normal:
            print(f"desc_normal: {desc_normal}")
            result["Description"] = str(desc_normal)
    
    # ========== MedicalEncyclopediaAttribute ==========
    
    # Process Entries Terms
    added_mdcl = set()
    # Estructura para MedicalTerm [Term, NormalizedTerm]
    medical_term = [{"TermId": 0, "Term": fthr_term, "NormalizedTerm": fthr_term.lower(), "TypeId": 1, "IsPrimary": 1}]
    added_mdcl.add(fthr_term)
    
    # RubroMaestro detail
    rub_master = soup.find_all("p", class_="RubroMaestro")
    for tag in rub_master:
        label = tag.get_text(strip=True).replace(":", "")
        et_type_str = get_entryterm_type(label)
        if et_type_str:
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
    
    # Process Attributes
    attributes = []
    rub_enc = soup.find_all("p", class_="rubroenc")
    for tag in rub_enc:
        span = tag.find("span", class_="h2")
        if span:
            label = span.get_text(strip=True).replace(":", "")
            attribute_id = get_medical_attribute(label)
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
                    "Content": "",
                    "HTMLContent": html_content.strip()
                })
            else:
                #TODO: Validar si hay que agregarlo, o se debe crear antes en el catalogo...
                print(f"New label: {label}")

    result["MedicalEncyclopediaAttribute"] = attributes
    
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
            for count, et_data in enumerate(entries_terms):
                if count > 0:
                    et_type_id = cat_et_type.get(et_data["TermType"])
                    if et_type_id:
                        print(f"et_type_id: {et_type_id}")
                        insert_medical_entry_term(conn, ids_list[0], et_type_id, et_data)
                    else:
                        print(f"Error al recuperar get_entry_term_type_id!!!")
# ========== DB SECELT, INSERT FUNCIONS ==========

def process_html_file(html_content):
    """Procesa un archivo HTML y lo inserta en la BD"""
    print(f"get_db_connection()...")
    conn = get_db_connection()
    try:
        # Inicializar Catalogos
        gt_cat_medical_attribute(conn)
        
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

            print(f"Datos insertados correctamente. EncyclopediaId: {encyclopedia_id}")
    except Exception as e:
        conn.rollback()
        print(f"Error al procesar archivo: {e}")
    finally:
        conn.close()

def list_html_files_from_s3(bucket_name, prefix):
    """Lista archivos HTML desde un prefijo en S3"""
    paginator = s3_client.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket_name, Prefix=prefix):
        for obj in page.get("Contents", []):
            key = obj["Key"]
            if key.endswith(".html"):
                yield key

def read_html_from_s3(bucket_name, key):
    """Lee el contenido de un archivo HTML desde S3"""
    response = s3_client.get_object(Bucket=bucket_name, Key=key)
    return response["Body"].read().decode("utf-8")

def move_file_to_processed(bucket_name, key):
    """Mueve archivo procesado a la carpeta 'processed/'"""
    destination_key = f"processed/{os.path.basename(key)}"
    s3_client.copy_object(
        Bucket=bucket_name,
        CopySource={"Bucket": bucket_name, "Key": key},
        Key=destination_key
    )
    s3_client.delete_object(Bucket=bucket_name, Key=key)
    print(f"Archivo movido a: {destination_key}")

if __name__ == "__main__":
    try:
        print(f"Procesando archivos desde S3: s3://{S3_CONFIG['bucket_name']}/{S3_CONFIG['s3_key']}")
        for html_key in list_html_files_from_s3(S3_CONFIG["bucket_name"], S3_CONFIG["s3_key"]):
            print(f"Procesando archivo: {html_key}")
            html_content = read_html_from_s3(S3_CONFIG["bucket_name"], html_key)
            process_html_file(html_content)
            move_file_to_processed(S3_CONFIG["bucket_name"], html_key)
    except Exception as e:
        print(f"Error en el procesamiento de archivos S3: {e}")