import os
import sys
import json
from datetime import datetime

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

def lambda_handler(event, context):
    # Verificación de paths y librerías
    print("Python path:", sys.path)
    print("LD_LIBRARY_PATH:", os.getenv('LD_LIBRARY_PATH'))
    
    try:
        import pyodbc
        print("pyodbc importado correctamente")
    except Exception as e:
        print(f"Error al importar pyodbc: {str(e)}")
        raise e
    
    # 1. Configuración (mejor usar variables de entorno)
    config = {
        'server': os.getenv('RDS_ENDPOINT'),
        'database': os.getenv('RDS_DATABASE'),
        'username': os.getenv('RDS_USERNAME'),
        'password': os.getenv('RDS_PASSWORD'),
        'bucket_name': os.getenv('S3_BUCKET'),
        's3_key': os.getenv('S3_KEY'),
        'query': os.getenv('SQL_QUERY', SQL_QUERY)
    }
    
    try:
        # 2. Conexión a RDS SQL Server
        conn_str = (
            f"DRIVER={{ODBC Driver 17 for SQL Server}};"
            f"SERVER={config['server']};"
            f"DATABASE={config['database']};"
            f"UID={config['username']};"
            f"PWD={config['password']}"
        )
        
        with pyodbc.connect(conn_str) as conn:
            with conn.cursor() as cursor:
                print(f"Ejecutando query: {config['query']}")
                cursor.execute(config['query'])
                
                # 3. Procesar resultados
                columns = [column[0] for column in cursor.description]
                results = [dict(zip(columns, row)) for row in cursor.fetchall()]
                
                if not results:
                    return {'status': 'success', 'message': 'No data found', 'file_created': False}
                
                # 4. Convertir a JSON
                json_data = json.dumps(results, indent=2, default=str)
                file_size = len(json_data.encode('utf-8'))
                print(f"Datos convertidos a JSON, tamaño: {file_size} bytes")
                
                # 5. Subir a S3
                s3 = boto3.client('s3')
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                s3_key = f"{config['s3_key']}/data-{timestamp}.json"
                
                s3.put_object(
                    Bucket=config['bucket_name'],
                    Key=s3_key,
                    Body=json_data,
                    ContentType='application/json'
                )
                
                print(f"Archivo subido exitosamente a s3://{config['bucket_name']}/{s3_key}")
                
                return {
                    'status': 'success',
                    'file_created': True,
                    'file_location': f"s3://{config['bucket_name']}/{s3_key}",
                    'records_exported': len(results),
                    'file_size_bytes': file_size
                }
    except Exception as e:
        print(f"Error durante la ejecución: {str(e)}")
        return {
            'status': 'error',
            'error_message': str(e)
        }
