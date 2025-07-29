# Ejemplo de importacion de Enciclopedias (Python)
import os
import re
from bs4 import BeautifulSoup
import pyodbc
from datetime import datetime

# Configuración de la base de datos SQL Server
DB_CONFIG = {
    "server": "plm-rds-desarrollopreproductivo.co6eawhyglix.us-east-1.rds.amazonaws.com",
    "database": "ZMedinet_Pruebas",
    "username": "marco.tlachino",
    "password": "Temporal1234*"
}

current_dir = os.path.dirname(os.path.abspath(__file__))
input_file = os.path.join(current_dir, '..', 'prcss_data_files', '0_Asma.html')

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

def get_encyclopedia_types(str):
    str_lwr = str.lower()
    for key in encyclopedia_types:
        if str_lwr in key.lower():
            return encyclopedia_types[key]
    return None

def get_attribute_id(label):
    lower_label = label.lower()
    for key in rubro_maestro_map:
        if key.lower() in lower_label:
            return rubro_maestro_map[key]
    return None

def extract_data_from_html(html_content):
    """Extrae datos del HTML y los estructura para la BD"""
    print(f"BeautifulSoup()...")
    soup = BeautifulSoup(html_content, "html.parser")
    print(f"get_encyclopedia_types()...")
    encyclpd_Id = get_encyclopedia_types("Enfermedad")
    print(f"** encyclpd_Id: {encyclpd_Id}")
    result = {
        "PLMCode": "",
        "EncyclopediaName": "",
        "Description": "",
        "ReadingTime": "",
        "EncyclopediaImage": "",
        "EncyclopediaTypeId": encyclpd_Id,
        "AuthorId": None,
        "Active": 1,
        "MedicalEncyclopediaAttribute": []
    }
    # Extraer título
    title_tag = soup.find("title")
    result["EncyclopediaName"] = title_tag.get_text(strip=True) if title_tag else ""
    # Extraer PLMCode
    codigo_tag = soup.find("span", class_="Codigo")
    if codigo_tag:
        match = re.search(r"\[(.*?)\]", codigo_tag.get_text())
        if match:
            result["PLMCode"] = match.group(1)
    # Extraer descripción principal
    desc_rubro = soup.find("p", class_="RubroMaestro", string=lambda t: t and "Descripción" in t)
    if desc_rubro:
        desc_normal = desc_rubro.find_next_sibling("p", class_="Normal")
        if desc_normal:
            result["Description"] = str(desc_normal)
    # Procesar atributos
    attributes = []
    # Rubros principales
    for tag in soup.find_all("p", class_="RubroMaestro"):
        label = tag.get_text(strip=True).replace(":", "")
        attribute_id = get_attribute_id(label)
        if attribute_id:
            html_content = ''
            pointer = tag.find_next_sibling()
            while pointer and not (pointer.name == 'p' and pointer.get('class') == ['hr']):
                html_content += str(pointer)
                pointer = pointer.find_next_sibling()
            attributes.append({
                "AttributeGroupId": attribute_id,
                "Content": "",
                "HTMLContent": html_content.strip()
            })
    # Subrubros
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

def insert_encyclopedia_data(conn, data):
    """Inserta datos en la tabla MedicalEncyclopedia y devuelve el ID generado"""
    with conn.cursor() as cursor:
        # SQL Server usa OUTPUT INSERTED para obtener el ID generado
        query = """
        INSERT INTO MedicalEncyclopedia (
            PLMCode, EncyclopediaName, Description, ReadingTime, 
            EncyclopediaImage, EncyclopediaTypeId, AuthorId, Active, LastUpdate
        ) 
        OUTPUT INSERTED.EncyclopediaId
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?);
        """
        
        cursor.execute(query, (
            data["PLMCode"],
            data["EncyclopediaName"],
            data["Description"],
            data["ReadingTime"],
            data["EncyclopediaImage"],
            data["EncyclopediaTypeId"],
            data["AuthorId"],
            data["Active"],
            datetime.now()
        ))
        
        encyclopedia_id = cursor.fetchone()[0]
        conn.commit()
        return encyclopedia_id

def insert_attributes_data(conn, encyclopedia_id, attributes):
    """Inserta los atributos relacionados con una enciclopedia"""
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
        
        cursor.executemany(query, values)
        conn.commit()

def process_html_file(html_content):
    """Procesa un archivo HTML y lo inserta en la BD"""
    print(f"get_db_connection()...")
    conn = get_db_connection()
    try:
        # Extraer datos del HTML
        print(f"extract_data_from_html()...")
        data = extract_data_from_html(html_content)
        
        # Insertar datos principales
        encyclopedia_id = insert_encyclopedia_data(conn, data)
        
        # Insertar atributos
        if data["MedicalEncyclopediaAttribute"]:
            insert_attributes_data(conn, encyclopedia_id, data["MedicalEncyclopediaAttribute"])
        
        print(f"Datos insertados correctamente. EncyclopediaId: {encyclopedia_id}")
    except Exception as e:
        conn.rollback()
        print(f"Error al procesar archivo: {e}")
    finally:
        conn.close()

if __name__ == "__main__":
    # Procesar todos los archivos HTML
    try:
        if not os.path.exists(input_file):
            raise FileNotFoundError(f"El archivo {input_file} no existe. Verifica la ruta.")
        print(f"Leer y procesar archivo: {input_file}")
        # Leer y procesar archivo
        with open(input_file, "r", encoding="utf-8") as file:
            html_content = file.read()
        print(f"process_html_file()...")
        process_html_file(html_content)
    except Exception as e:
        print(f"Error al procesar archivo {input_file}: {e}")