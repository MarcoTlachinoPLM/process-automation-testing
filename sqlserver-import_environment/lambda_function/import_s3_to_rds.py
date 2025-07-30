import os
import sys
import re
import pyodbc
from datetime import datetime
from bs4 import BeautifulSoup

# === Configuración del entorno para Lambda Layer ===
os.environ['LD_LIBRARY_PATH'] = '/opt/lib:/opt/python/lib/python3.11/site-packages'
os.environ['ODBCINI'] = '/opt/odbc.ini'
os.environ['ODBCSYSINI'] = '/opt'
sys.path.insert(0, '/opt/python/lib/python3.11/site-packages')

# === Configuración de conexión usando variables de entorno ===
DB_CONFIG = {
    "server": os.environ.get("DB_SERVER"),
    "database": os.environ.get("DB_NAME"),
    "username": os.environ.get("DB_USER"),
    "password": os.environ.get("DB_PASSWORD")
}

# === Mapeos estáticos ===
encyclopedia_types = {
    "Enfermedades": 1,
    "Síntomas": 2,
    "Procedimientos Quirurgicos": 3,
    "Procedimientos Diagnosticos": 4
}

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

# === Init Funciones auxiliares ===
def log_environment():
    """Log important environment settings"""
    print("=== Environment Configuration ===")
    print(f"LD_LIBRARY_PATH: {os.getenv('LD_LIBRARY_PATH')}")
    print(f"ODBCINI: {os.getenv('ODBCINI')}")
    print(f"ODBCSYSINI: {os.getenv('ODBCSYSINI')}")
    print("Python sys.path:", sys.path)
    
    # Verificar existencia de archivos críticos
    critical_files = [
        '/opt/python/lib/python3.11/site-packages/pyodbc.so',
        '/opt/microsoft/msodbcsql18/lib64/libmsodbcsql-18.1.so.1.1',
        os.getenv('ODBCINI')
    ]
    
    for file in critical_files:
        exists = "YES" if os.path.exists(file) else "NO!!!"
        print(f"{exists} - {file}")

def get_db_connection():
    conn_str = (
        f"DRIVER={{ODBC Driver 18 for SQL Server}};"
        f"SERVER={DB_CONFIG['server']};"
        f"DATABASE={DB_CONFIG['database']};"
        f"UID={DB_CONFIG['username']};"
        f"PWD={DB_CONFIG['password']};"
        f"Encrypt=yes;TrustServerCertificate=yes;"
    )
    return pyodbc.connect(conn_str)

def get_encyclopedia_types(str_label):
    str_lwr = str_label.lower()
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
# === End Funciones auxiliares ===

# === Init Extracción de datos ===
def extract_data_from_html(html_content):
    soup = BeautifulSoup(html_content, "html.parser")
    encyclpd_Id = get_encyclopedia_types("Enfermedad")
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

    # Título
    title_tag = soup.find("title")
    result["EncyclopediaName"] = title_tag.get_text(strip=True) if title_tag else ""

    # Código PLM
    codigo_tag = soup.find("span", class_="Codigo")
    if codigo_tag:
        match = re.search(r"\[(.*?)\]", codigo_tag.get_text())
        if match:
            result["PLMCode"] = match.group(1)

    # Descripción principal
    desc_rubro = soup.find("p", class_="RubroMaestro", string=lambda t: t and "Descripción" in t)
    if desc_rubro:
        desc_normal = desc_rubro.find_next_sibling("p", class_="Normal")
        if desc_normal:
            result["Description"] = str(desc_normal)

    # Atributos (RubroMaestro y rubroenc)
    attributes = []

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
# === End Extracción de datos ===

# === Init Load datos en RDS ===
def insert_encyclopedia_data(conn, data):
    with conn.cursor() as cursor:
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
    with conn.cursor() as cursor:
        query = """
        INSERT INTO MedicalEncyclopediaAttribute (
            AttributeGroupId, EncyclopediaId, Content, HTMLContent
        ) VALUES (?, ?, ?, ?);
        """
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
# === End Load datos en RDS ===

def process_html_content(html_content):
    conn = get_db_connection()
    try:
        data = extract_data_from_html(html_content)
        encyclopedia_id = insert_encyclopedia_data(conn, data)
        if data["MedicalEncyclopediaAttribute"]:
            insert_attributes_data(conn, encyclopedia_id, data["MedicalEncyclopediaAttribute"])
        return {"status": "success", "encyclopedia_id": encyclopedia_id}
    except Exception as e:
        conn.rollback()
        return {"status": "error", "message": str(e)}
    finally:
        conn.close()

# === Lambda handler principal ===
def lambda_handler(event, context):
    try:
        # 1. Log environment configuration
        log_environment()
        
        # 2. Validar variables de entorno
        required_vars = [
            'RDS_ENDPOINT', 'RDS_DATABASE',
            'RDS_USERNAME', 'RDS_PASSWORD',
            'S3_BUCKET', 'S3_KEY'
        ]
        
        missing_vars = [var for var in required_vars if not os.getenv(var)]
        if missing_vars:
            raise ValueError(f"Missing environment variables: {', '.join(missing_vars)}")
        
        html_content = event.get("html_content")
        if not html_content:
            return {
                "statusCode": 400,
                "body": "html_content no proporcionado en el evento"
            }

        result = process_html_content(html_content)

        return {
            "statusCode": 200 if result["status"] == "success" else 500,
            "body": result
        }

    except Exception as e:
        return {
            "statusCode": 500,
            "body": {
                "status": "error",
                "message": str(e)
            }
        }
