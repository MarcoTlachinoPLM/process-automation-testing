import os
import pyodbc
import re
from bs4 import BeautifulSoup
from datetime import datetime

DB_CONFIG = {
    "server": "plm-rds-desarrollopreproductivo.co6eawhyglix.us-east-1.rds.amazonaws.com",
    "database": "ZMedinet_Pruebas",
    "username": "marco.tlachino",
    "password": "Temporal1234*"
}

# === Rutas de entrada ===
current_dir = os.path.dirname(os.path.abspath(__file__))
input_file = os.path.join(current_dir, '..', 'prcss_data_files', 'test_disease.html')

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
    "Sinónimos": "Synonym",
    "Palabras clave": "Related Term"
}

cat_term_type = {
    "Sinónimos": "Sinonimos",
    "Palabras clave": "PalabrasClave"
}

# === Database Connection ===
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

def gt_cat_entry_term_type(conn):
    """Recupera diccionario de EntryTermType."""
    cat_et_type = {}
    with conn.cursor() as cursor:
        # Check if term exists
        cursor.execute("SELECT EntryTermTypeId, TypeName FROM ZMedinet_Pruebas.dbo.EntryTermType;")
        # Procesa los resultados fila por fila
        for row in cursor.fetchall():
            id_value = row[0]
            nombre_value = row[1]
            cat_et_type[nombre_value] = id_value
        return cat_et_type

def get_attribute_id(label):
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

def extract_data_from_html_local(filepath):
    with open(filepath, "r", encoding="utf-8") as file:
        html_content = file.read()
    
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
    ubro_soup = soup.find_all("p", class_="RubroMaestro")
    for tag in ubro_soup:
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
                    "Content": "",
                    "HTMLContent": html_content.strip()
                })
    
    result["MedicalEncyclopediaAttribute"] = attributes
    
    return result

# === Database Insertion Functions ===
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

def insert_body_part_data(conn, encyclopedia_id, body):
    """Inserta los atributos relacionados con una enciclopedia"""
    with conn.cursor() as cursor:
        query = """
        INSERT INTO MedicalEncyclopediaBodyPartPlane (
            EncyclopediaId, BodyPartId, BodyPlaneId
        ) VALUES (?, ?, ?);
        """
        print(f"INSERT INTO MedicalEncyclopediaBodyPartPlane ()...")
        cursor.execute(query,(encyclopedia_id, body["BodyPartId"], body["BodyPlaneId"]))
        conn.commit()

def update_or_insert_attributes(conn, encyclopedia_id, attributes):
    """Inserta ó actualiza los atributos relacionados con una enciclopedia"""
    if len(attributes) > 0:
        # Define el patrón de búsqueda
        pattern = r'<p class="h5">(.*?)</p>'
        # Process Entries Terms
        attr_new = set()
        attr_updt = set()
        with conn.cursor() as cursor:
            for attrbt in attributes:
                matches = re.findall(pattern, attrbt["HTMLContent"], re.DOTALL)
                if matches:
                    str_html_like = ""
                    for match in matches:
                        if str_html_like:
                            str_html_like = f"{str_html_like}%{match}"
                        else:
                            str_html_like = f"{match}"
                    if str_html_like:
                        # Check if exists
                        query_select = """
                        SELECT AttributeGroupId FROM MedicalEncyclopediaAttribute
                        WHERE EncyclopediaId = ? AND HTMLContent LIKE '%?%';
                        """
                        cursor.execute(
                            query_select, 
                            (encyclopedia_id, str_html_like)
                        )
                        row = cursor.fetchone()
                        if row:
                            attrbt["AttributeGroupId"] = row[0]
                            attr_updt.add(attrbt)
                        else:
                            attr_new.add(attrbt)
            list_updt = list(attr_updt)
            if len(list_updt) > 0:
                for attr_upd in attributes:
                    query_update = """
                    UPDATE MedicalEncyclopediaAttribute
                    SET Content = ?, HTMLContent = ?
                    WHERE EncyclopediaId = ? AND AttributeGroupId = ?;
                    """
                    print(f"UPDATE MedicalEncyclopediaAttribute ()...")
                    cursor.execute(
                            query_update, 
                            (encyclopedia_id, attr_upd["AttributeGroupId"])
                    )
                    conn.commit()
            list_new = list(attr_new)
            if len(list_new) > 0:
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
                        for attr in list_new
                    ]
                    print(f"Values: {values}")
                    print(f"INSERT INTO MedicalEncyclopediaAttribute ()...")
                    cursor.executemany(query, values)
                    conn.commit()

def insert_or_get_term(conn, data):
    """Inserta un término si no existe o devuelve el ID existente"""
    with conn.cursor() as cursor:
        # Check if term exists
        cursor.execute(
            "SELECT TermId FROM MedicalTerm WHERE NormalizedTerm = ?", 
            data["NormalizedTerm"]
        )
        row = cursor.fetchone()
        if row:
            return row[0]
        else:
            query = """
            INSERT INTO MedicalTerm (Term, NormalizedTerm) OUTPUT INSERTED.TermId VALUES (?, ?);
            """
            print(f"data: {data}")
            print(f"INSERT INTO MedicalTerm ()...")
            cursor.execute(query, (data["Term"], data["NormalizedTerm"]))
            term_id = cursor.fetchone()[0]
            conn.commit()
            return term_id

def insert_medical_entry_term(conn, indx, et_type_id, data):
    """Inserta un término si no existe o devuelve el ID existente"""
    print(f"data: {data}")
    with conn.cursor() as cursor:
        query_met = """
        INSERT INTO MedicalEntryTerm (
            TermId, EntryTermId, EntryTermTypeId, IsPrimary
        ) VALUES (?, ?, ?, ?);
        """
        print(f"INSERT INTO MedicalEntryTerm (...) VALUES({data["TermId"]}, {indx}, {et_type_id}, {data["IsPrimary"]})")
        cursor.execute(
            query_met,
            (data["TermId"], indx, et_type_id, data["IsPrimary"])
        )
        conn.commit()
        return

def insert_entries_terms_data(conn, encyclopedia_id, entries_terms):
    """Inserta los términos relacionados con una enciclopedia"""
    if len(entries_terms) > 0:
        with conn.cursor() as cursor:
            # First process MedicalTerm and get all TermIds
            term_ids = set()
            map_term_ids = {}
            for count, term in enumerate(entries_terms):
                term_id = insert_or_get_term(conn, term)
                term["TermId"] = term_id
                entries_terms[count] = term
                print(f"term_ids.add({term_id})...")
                term_ids.add(term_id)
                map_term_ids[term["NormalizedTerm"]] = term_id
            
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
            #ids_list = list(term_ids)
            cat_et_type = gt_cat_entry_term_type(conn)
            for count, et_data in enumerate(entries_terms):
                if count > 0:
                    et_type_id = cat_et_type.get(et_data["TermType"])
                    if et_type_id:
                        print(f"et_type_id: {et_type_id}")
                        insert_medical_entry_term(conn, count+1, et_type_id, et_data)
                    else:
                        print(f"Error al recuperar get_entry_term_type_id!!!")

# === EJECUCIÓN PRINCIPAL ===
if __name__ == "__main__":
    if not os.path.exists(input_file):
        raise FileNotFoundError(f"El archivo {input_file} no existe. Verifica la ruta.")
    
    # Extract data from HTML
    data = extract_data_from_html_local(input_file)
    
    # Connect to database and insert data
    try:
        conn = get_db_connection()
        
        # Insert Encyclopedia data and get the generated ID
        encyclopedia_id = insert_or_get_encyclopedia_data(conn, data)
        print(f"Inserted Encyclopedia with ID: {encyclopedia_id}")
        
        # Insert BodyPart
        #insert_body_part_data(conn, encyclopedia_id, data["MedicalEncyclopediaBodyPartPlane"])
        #print("Inserted BodyPart successfully")

        # Insert Attributes
        if data["MedicalEncyclopediaAttribute"]:
            update_or_insert_attributes(conn, encyclopedia_id, data["MedicalEncyclopediaAttribute"])
            print("Inserted Attributes successfully")
        
        # Insert MedicalTerms and relationships
        if data["MedicalEntriesTerms"]:
            insert_entries_terms_data(conn, encyclopedia_id, data["MedicalEntriesTerms"])
            print("Inserted MedicalTerms and relationships successfully")
        
    except Exception as e:
        print(f"Error: {str(e)}")
        if 'conn' in locals() and conn is not None:
            try:
                conn.rollback()
                print("Se realizó rollback...")
            except Exception as rollback_error:
                print(f"Error al hacer rollback: {rollback_error}")
    finally:
        if 'conn' in locals():
            conn.close()
            print("Se cerró la conexion correctamente.")
    
    print("✅ Proceso completado")
