# Ejemplo de importacion de Enciclopedias (Python)
import os
import re
import boto3
from bs4 import BeautifulSoup
import pyodbc
from datetime import datetime

# Configuración de AWS S3
S3_CONFIG = {
    "bucket_name": "opensearch-dev",
    "s3_key": "test_files/"
}

# Configuración de la base de datos SQL Server
DB_CONFIG = {
    "server": "plm-rds-desarrollopreproductivo.co6eawhyglix.us-east-1.rds.amazonaws.com",
    "database": "ZMedinet_Pruebas",
    "username": "marco.tlachino",
    "password": "Temporal1234*"
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

def download_html_from_s3(bucket_name, s3_key, local_path):
    """Descarga un archivo HTML desde S3"""
    s3 = boto3.client('s3')
    try:
        s3.download_file(bucket_name, s3_key, local_path)
        return True
    except Exception as e:
        print(f"Error al descargar archivo desde S3: {e}")
        return False

def extract_data_from_html(html_content):
    """Extrae datos del HTML y los estructura para la BD"""
    soup = BeautifulSoup(html_content, "html.parser")
    
    result = {
        "PLMCode": "",
        "EncyclopediaName": "",
        "Description": "",
        "ReadingTime": "",
        "EncyclopediaImage": "",
        "EncyclopediaTypeId": 1,
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
    
    # Mapeo de grupos de atributos
    rubro_maestro_map = {
        "Descripción": 1,
        "Sinónimos": 2,
        "Palabras clave que guíen a la enciclopedia": 3
    }
    
    rubroenc_map = {
        "Definición y causas": 4,
        "Síntomas y diagnóstico": 5,
        "Tratamiento y bienestar": 6
    }
    
    # Procesar atributos
    attributes = []
    
    # Rubros principales
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
    conn = get_db_connection()
    try:
        # Extraer datos del HTML
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

def process_s3_files():
    """Procesa todos los archivos HTML en el bucket S3 con mejor manejo de errores"""
    try:
        # Configura el cliente S3 con manejo de errores mejorado
        s3 = boto3.client(
            's3',
            config=Config(
                region_name='us-east-1',  # Ajusta a tu región
                retries={'max_attempts': 3, 'mode': 'standard'}
            )
        )
        
        # Verifica primero si puedes acceder al bucket
        try:
            s3.head_bucket(Bucket=S3_CONFIG["bucket_name"])
        except Exception as e:
            print(f"No se puede acceder al bucket: {e}")
            print("Verifica:")
            print("1. Que el bucket existe")
            print("2. Que tus credenciales AWS están configuradas correctamente")
            print("3. Que tu usuario IAM tiene los permisos necesarios")
            return
        
        # Listar archivos en el bucket con paginación
        paginator = s3.get_paginator('list_objects_v2')
        page_iterator = paginator.paginate(
            Bucket=S3_CONFIG["bucket_name"],
            Prefix=S3_CONFIG["s3_key"]
        )
        
        file_count = 0
        
        try:
            for page in page_iterator:
                if 'Contents' not in page:
                    print("No se encontraron archivos en el bucket S3.")
                    continue
                for obj in page['Contents']:
                    if obj['Key'].endswith('.html'):
                        file_count += 1
                        print(f"\nProcesando archivo {file_count}: {obj['Key']}")
                        # Descargar archivo temporalmente
                        local_path = f"/tmp/{os.path.basename(obj['Key'])}"
                        try:
                            s3.download_file(
                                S3_CONFIG["bucket_name"], 
                                obj['Key'], 
                                local_path
                            )
                            # Leer y procesar archivo
                            with open(local_path, "r", encoding="utf-8") as file:
                                html_content = file.read()
                                process_html_file(html_content)
                        except Exception as e:
                            print(f"Error al procesar archivo {obj['Key']}: {e}")
                            continue
                        finally:
                            # Eliminar archivo temporal si existe
                            if os.path.exists(local_path):
                                os.remove(local_path)
            
            if file_count == 0:
                print("No se encontraron archivos HTML en la ubicación especificada.")
            else:
                print(f"\nProceso completado. Total archivos procesados: {file_count}")
                
        except Exception as e:
            print(f"Error inesperado al procesar archivos: {e}")

    except Exception as e:
        print(f"Error de configuración con AWS S3: {e}")
        print("Posibles causas:")
        print("1. Credenciales AWS no configuradas")
        print("2. Región incorrecta")
        print("3. Problemas de red/proxy")

if __name__ == "__main__":
    # Procesar todos los archivos HTML en el bucket S3
    process_s3_files()
