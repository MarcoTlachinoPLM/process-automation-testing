
import os
import sys
import json
from datetime import datetime

# Configuración crítica de paths
os.environ['LD_LIBRARY_PATH'] = '/opt/lib:/opt/python/lib/python3.11/site-packages'
os.environ['ODBCINI'] = '/opt/odbc.ini'
os.environ['ODBCSYSINI'] = '/opt'
sys.path.insert(0, '/opt/python/lib/python3.11/site-packages')

# Verificar archivos críticos
critical_files = [
    '/opt/python/lib/python3.11/site-packages/pyodbc.cpython-311-aarch64-linux-gnu.so',
    '/opt/lib/libmsodbcsql-18.1.so.1.1',
    '/opt/odbc.ini'
]

for file in critical_files:
    if not os.path.exists(file):
        print(f"CRITICAL: Missing file {file}")
    else:
        print(f"File exists: {file}")

# Verificación de importación
try:
    import pyodbc
    print("¡pyodbc importado correctamente!")
    print(f"Versión pyodbc: {pyodbc.version}")
    print(f"Drivers disponibles: {pyodbc.drivers()}")
except ImportError as e:
    print(f"Error importando pyodbc: {str(e)}")
    print("sys.path:", sys.path)
    print("LD_LIBRARY_PATH:", os.getenv('LD_LIBRARY_PATH'))
    print("Contenido de /opt/python/lib/python3.11/site-packages:")
    os.system("ls -la /opt/python/lib/python3.11/site-packages")
    raise

try:
    import boto3
    print("boto3 importado correctamente!")
    print(f"Versión boto3: {boto3.__version__}")
except ImportError as e:
    print(f"Error importando boto3: {str(e)}")
    raise

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
    """Create and return a database connection"""
    conn_str = (
        f"DRIVER={{ODBC Driver 18 for SQL Server}};"
        f"SERVER={os.getenv('RDS_ENDPOINT')};"
        f"DATABASE={os.getenv('RDS_DATABASE')};"
        f"UID={os.getenv('RDS_USERNAME')};"
        f"PWD={os.getenv('RDS_PASSWORD')};"
        "Encrypt=yes;TrustServerCertificate=no;Connection Timeout=30;"
    )
    return pyodbc.connect(conn_str)

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
        
        # 3. Query a ejecutar (puede venir del event o usar default)
        query = event.get('query', """
        SELECT TOP 150
            icd.ICDId,
            icd.SpanishDescription
        FROM Medinet.dbo.ICD11 icd
        WHERE ChapterNo IS NOT NULL
        ORDER BY icd.SpanishDescription DESC;
        """)
        
        # 4. Conectar a SQL Server y ejecutar query
        with get_db_connection() as conn:
            with conn.cursor() as cursor:
                print(f"Executing query:\n{query}")
                cursor.execute(query)
                
                # Obtener resultados
                columns = [column[0] for column in cursor.description]
                results = [dict(zip(columns, row)) for row in cursor.fetchall()]
                
                if not results:
                    return {
                        'status': 'success',
                        'message': 'No data found',
                        'data': []
                    }
                
                # 5. Preparar datos para S3
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                s3_key = f"{os.getenv('S3_KEY').rstrip('/')}/export-{timestamp}.json"
                json_data = json.dumps(results, indent=2, default=str)
                
                # 6. Subir a S3
                s3 = boto3.client('s3')
                s3.put_object(
                    Bucket=os.getenv('S3_BUCKET'),
                    Key=s3_key,
                    Body=json_data,
                    ContentType='application/json'
                )
                
                return {
                    'status': 'success',
                    'records_exported': len(results),
                    's3_location': f"s3://{os.getenv('S3_BUCKET')}/{s3_key}",
                    'timestamp': timestamp
                }
    
    except pyodbc.Error as db_err:
        print(f"Database error: {str(db_err)}")
        return {
            'status': 'error',
            'type': 'database',
            'error': str(db_err)
        }
    except Exception as e:
        print(f"Unexpected error: {str(e)}")
        return {
            'status': 'error',
            'type': 'unexpected',
            'error': str(e)
        }
