# Ejemplo de extraccion y conversion a JSON (Python)
import boto3
import json
import os
import pyodbc
import configparser

# === CONFIGURACIÓN ===
current_dir = os.path.dirname(os.path.abspath(__file__))
config = configparser.ConfigParser()
config.read(os.path.join(current_dir, '..', 'config.ini'))

RDS_CONFIG = {
    "server": config["database"]["server"],
    "database": config["database"]["database"],
    "username": config["database"]["username"],
    "password": config["database"]["password"]
}

S3_CONFIG = {
    "bucket_name": "plmopensearch-dev",
    "s3_key": "test_files/exported_data.json"
}

SQL_QUERY = """
SELECT TOP 150
    icd.ICDId,
    icd.SpanishDescription
FROM
    Medinet.dbo.ICD11 icd
WHERE
    ChapterNo IS NOT NULL
ORDER BY
    icd.SpanishDescription DESC;
"""

# === CONEXIÓN A SQL SERVER ===
conn_str = (
    f"DRIVER={{ODBC Driver 17 for SQL Server}};"
    f"SERVER={RDS_CONFIG['server']};"
    f"DATABASE={RDS_CONFIG['database']};"
    f"UID={RDS_CONFIG['username']};"
    f"PWD={RDS_CONFIG['password']}"
)

print("[INFO] Conectando a SQL Server...")
conn = pyodbc.connect(conn_str)
cursor = conn.cursor()
cursor.execute(SQL_QUERY)

columns = [col[0] for col in cursor.description]
rows = [dict(zip(columns, row)) for row in cursor.fetchall()]
conn.close()
print("[INFO] Datos obtenidos correctamente.")

# === CONVERTIR A JSON ===
json_data = json.dumps(rows, indent=2, default=str)
current_dir = os.path.dirname(os.path.abspath(__file__))
output_file = os.path.join(current_dir, '..', 'prcss_data_files', 'exported_data.json')
with open(output_file, 'w') as f:
    f.write(json_data)
print(f"[INFO] Archivo JSON guardado en {output_file}")

# === SUBIR A S3 ===
print("[INFO] Subiendo a S3...")
s3 = boto3.client("s3")
s3.upload_file(output_file, S3_CONFIG["bucket_name"], S3_CONFIG["s3_key"])
print(f"[SUCCESS] Archivo subido a s3://{S3_CONFIG['bucket_name']}/{S3_CONFIG['s3_key']}")
