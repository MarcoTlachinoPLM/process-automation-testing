import configparser
import csv
import os
import pandas as pd
import pyodbc

# Configuración de la base de datos SQL Server
current_dir = os.path.dirname(os.path.abspath(__file__))
config = configparser.ConfigParser()
config.read(os.path.join(current_dir, '..', 'config.ini'))
path_files = 'csv_import_files'
file_name = 'list_of_encyclopedias.csv'

DB_CONFIG = {
    "server": config["database"]["server"],
    "database": config["database"]["database"],
    "username": config["database"]["username"],
    "password": config["database"]["password"]
}

# Habilitar/Dehabilitar LOGs
ENABLE_LOGS = True
MAX_AGE_ID = ""
MAX_GNDR_ID = ""

# Mapa con Tag en archivo CSV para cada parametro
map_tags = {
    "PLMCode": "AlphanumericCode",
    "EncyclopediaName": "FinalTitle|OriginalTopic",
    "AgeRangeIDs": "Age",
    "GenderIDs": "Sex",
    "SystemHumanBodyID": "DevicesAndSystems",
    "BodyPartID": "Body"
}

# === Ruta al archivo CSV ===
csv_file = os.path.join(current_dir, '..', path_files, file_name)

def print_log(message):
    if ENABLE_LOGS:
        print(message)

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
CAT_AGE_RANGE = {}
CAT_GENDER = {}
CAT_HUMAN_BODY = {}
CAT_BODY_PLANE = {}
CAT_BODY_PART = {}

def get_cat_age_range(conn):
    """Recupera diccionario de AgeRange."""
    with conn.cursor() as cursor:
        cursor.execute("SELECT AgeRangeId, AgeRange FROM AgeRange;")
        for row in cursor.fetchall():
            id_value = row[0]
            nombre_value = row[1]
            CAT_AGE_RANGE[id_value] = nombre_value
        return True

def get_cat_gender(conn):
    """Recupera diccionario de Gender."""
    with conn.cursor() as cursor:
        cursor.execute("SELECT GenderId, Gender FROM Gender;")
        for row in cursor.fetchall():
            id_value = row[0]
            nombre_value = row[1]
            CAT_GENDER[id_value] = nombre_value
        return True

def get_cat_human_body(conn):
    """Recupera diccionario para SystemHumanBody."""
    with conn.cursor() as cursor:
        cursor.execute("SELECT SystemHumanBodyId, SystemHumanBody FROM SystemHumanBody;")
        for row in cursor.fetchall():
            id_value = row[0]
            nombre_value = row[1]
            CAT_HUMAN_BODY[id_value] = nombre_value
        return True

def get_cat_body_plane(conn):
    """Recupera diccionario para BodyPlane."""
    with conn.cursor() as cursor:
        cursor.execute("SELECT BodyPlaneId, BodyPlane FROM BodyPlane;")
        for row in cursor.fetchall():
            id_value = row[0]
            nombre_value = row[1]
            CAT_BODY_PLANE[id_value] = nombre_value
        return True

def get_cat_body_part(conn):
    """Recupera diccionario para BodyPart."""
    with conn.cursor() as cursor:
        cursor.execute("SELECT BodyPartId, BodyPart FROM BodyPart;")
        for row in cursor.fetchall():
            id_value = row[0]
            nombre_value = row[1]
            CAT_BODY_PART[id_value] = nombre_value
        return True

age_range_ids = []
gender_ids = []

def get_age_range_ids():
    for age_id in CAT_AGE_RANGE:
        age_range_ids.append(age_id)
    return None

def get_gender_ids():
    for gndr_id in CAT_GENDER:
        gender_ids.append(gndr_id)
    return None
# ======= DB GET CATALOGS =======

def get_encyclopedia_id(cursor, plm_code, encyclopedia_name):
    qry_slct_ency = """
    SELECT EncyclopediaId FROM MedicalEncyclopedia
    WHERE PLMCode = ? AND EncyclopediaName = ?;
    """
    print_log(f"SELECT EncyclopediaId FROM MedicalEncyclopedia...")
    cursor.execute(qry_slct_ency, (plm_code, encyclopedia_name))
    result = cursor.fetchone()
    if result:
        return result[0]
    else:
        return None

def insert_age_range(cursor, encyclopedia_id, age_id):
    # Check if term exists
    cursor.execute(
        "SELECT AgeRangeId FROM MedicalEncyclopediaAgeRange WHERE EncyclopediaId = ? AND AgeRangeId = ?",
        (encyclopedia_id, age_id)
    )
    row = cursor.fetchone()
    if not row:
        qry_insrt_age = """
        INSERT INTO MedicalEncyclopediaAgeRange (EncyclopediaId, AgeRangeId) VALUES (?, ?)
        """
        print(f"INSERT INTO MedicalEncyclopediaAgeRange ({encyclopedia_id}, {age_id})...")
        cursor.execute(qry_insrt_age, (encyclopedia_id, age_id))

def insert_gender(cursor, encyclopedia_id, gndr_id, pregnancy):
    # Check if term exists
    cursor.execute(
        "SELECT GenderId FROM MedicalEncyclopediaGender WHERE EncyclopediaId = ? AND GenderId = ?",
        (encyclopedia_id, gndr_id)
    )
    row = cursor.fetchone()
    if not row:
        qry_insrt_gender = """
        INSERT INTO MedicalEncyclopediaGender (EncyclopediaId, GenderId, IsPregnantCondition) VALUES (?, ?, ?)
        """
        print(f"INSERT INTO MedicalEncyclopediaGender ({encyclopedia_id}, {gndr_id})...")
        cursor.execute(qry_insrt_gender, (encyclopedia_id, gndr_id, pregnancy))

def insert_body(cursor, encyclopedia_id, body_id):
    # Check if term exists
    cursor.execute(
        "SELECT SystemHumanBodyId FROM MedicalEncyclopediaSystemHumanBody WHERE EncyclopediaId = ? AND SystemHumanBodyId = ?",
        (encyclopedia_id, body_id)
    )
    row = cursor.fetchone()
    if not row:
        qry_insrt_body = """
        INSERT INTO MedicalEncyclopediaSystemHumanBody (EncyclopediaId, SystemHumanBodyId) VALUES (?, ?)
        """
        print(f"INSERT INTO MedicalEncyclopediaSystemHumanBody ({encyclopedia_id}, {body_id})...")
        cursor.execute(qry_insrt_body, (encyclopedia_id, body_id))

def insert_body_part(cursor, encyclopedia_id, bdy_prt_id, bdy_pln_id):
    # Check if term exists
    cursor.execute(
        "SELECT BodyPartId FROM MedicalEncyclopediaBodyPartPlane WHERE EncyclopediaId = ? AND BodyPartId = ? AND BodyPlaneId = ?",
        (encyclopedia_id, bdy_prt_id, bdy_pln_id)
    )
    row = cursor.fetchone()
    if not row:
        qry_insrt_bodypart = """
        INSERT INTO MedicalEncyclopediaBodyPartPlane (EncyclopediaId, BodyPartId, BodyPlaneId) VALUES (?, ?, ?)
        """
        print_log(f"INSERT INTO MedicalEncyclopediaBodyPartPlane ({bdy_prt_id}, {bdy_pln_id})...")
        cursor.execute(qry_insrt_bodypart, (encyclopedia_id, bdy_prt_id, bdy_pln_id))

def process_data(conn, row, str_plm_code, arry_names):
    """Procesa una fila de datos y realiza las inserciones en las tablas relacionadas"""
    cursor = conn.cursor()
    try:
        # Obtener EncyclopediaId buscando por PLMCode y EncyclopediaName
        str_code = row[str_plm_code]
        print_log(f"str_code: {str_code}")
        plm_code = str_code.replace("[", "")
        plm_code = plm_code.replace("]", "")
        encyclopedia_name = row[arry_names[0]] if row[arry_names[0]] else row[arry_names[1]]
        # Buscar EncyclopediaId
        print_log(f"plm_code: {plm_code}, encyclopedia_name: {encyclopedia_name}")
        encycl_id = get_encyclopedia_id(cursor, plm_code, encyclopedia_name)
        print_log(f"encycl_id: {encycl_id}")
        if encycl_id:
            encyclopedia_id = int(encycl_id)
            print_log(f"** encyclopedia_id: {encyclopedia_id}")
            try:
                # Procesar MedicalEncyclopediaAgeRange (Age)
                str_age = map_tags.get("AgeRangeIDs")
                if pd.notna(row[str_age]):
                    ages = row[str_age]
                    print_log(f"AGE: {ages}")
                    age_ranges = [x.strip() for x in str(ages).split('|') if x.strip()]
                    print_log(f"age_ranges: {age_ranges}")
                    if len(age_ranges) > 1 or MAX_AGE_ID not in age_ranges:
                        for age_id in age_ranges:
                            insert_age_range(cursor, encyclopedia_id, int(age_id))
                    else:
                        for ageid in age_range_ids:
                            insert_age_range(cursor, encyclopedia_id, int(ageid))
                
                # Procesar MedicalEncyclopediaGender (Sex y Pregnancy)
                str_gndr = map_tags.get("GenderIDs")
                if pd.notna(row[str_gndr]):
                    gender_id = row[str_gndr]
                    str_prgn = map_tags.get("Pregnancy")
                    pregnant = row[str_prgn]
                    is_pregnant = 1 if pregnant and pregnant.strip() == "1" else 0
                    print_log(f"gender_id: {gender_id}, is_pregnant: {is_pregnant}")
                    if MAX_GNDR_ID in str(gender_id):
                        for gndr_id in gender_ids:
                            insert_gender(cursor, encyclopedia_id, int(gndr_id), int(is_pregnant))
                    else:
                        insert_gender(cursor, encyclopedia_id, int(gender_id), int(is_pregnant))
                
                # Procesar MedicalEncyclopediaSystemHumanBody (DevicesAndSystems)
                str_sys_hmn = map_tags.get("SystemHumanBodyID")
                if pd.notna(row[str_sys_hmn]):
                    systems = [x.strip() for x in str(row[str_sys_hmn]).split('|') if x.strip()]
                    print_log(f"systems: {systems}")
                    for system_id in systems:
                        insert_body(cursor, encyclopedia_id, int(system_id))
                
                # Procesar MedicalEncyclopediaBodyPartPlane (Body)
                str_bdy_prt = map_tags.get("BodyPartID")
                if pd.notna(row[str_bdy_prt]):
                    body_parts = [x.strip() for x in str(row[str_bdy_prt]).split('|') if x.strip()]
                    # TODO: Validar como recuperar BodyPlaneId !!!
                    body_plane_id = 1
                    print_log(f"body_parts: {body_parts}, body_plane_id: {body_plane_id}")
                    for body_id in body_parts:
                        insert_body_part(cursor, encyclopedia_id, int(body_id), int(body_plane_id))
            except Exception as e:
                print(f"Error: {e}")
                print(f"conn.rollback()...")
                conn.rollback()
            finally:
                return
        else:
            print(f"No se encontró enciclopedia para PLMCode: {row['AlphanumericCode']}, Title: {row['FinalTitle']}")
            return
    except Exception as e:
        print(f"Error general procesando fila: {e}")
    finally:
        cursor.close()
        return

def main():
    conn = get_db_connection()
    try:
        # Inicializar Catalogos
        print_log(f"Initialize Catalogs...")
        get_cat_age_range(conn)
        get_cat_gender(conn)
        get_cat_human_body(conn)
        get_cat_body_plane(conn)
        get_cat_body_part(conn)
        
        get_age_range_ids()
        print_log(f"age_range_ids: {age_range_ids}")
        age_rngs_int = (int(n) for n in age_range_ids)
        MAX_AGE_ID = str(max(age_rngs_int)+1) if age_rngs_int else "6"
        get_gender_ids()
        print_log(f"gender_ids: {gender_ids}")
        gndrs_int = (int(n) for n in gender_ids)
        MAX_AGE_ID = str(max(gndrs_int)+1) if gndrs_int else "3"
        
        # Leer y procesar cada fila en CSV
        with open(csv_file, newline='', encoding="utf-8") as f:
            reader = csv.DictReader(f)
            prcss_row = set()
            str_plm_code = map_tags.get("PLMCode")
            str_encycl_name = map_tags.get("EncyclopediaName")
            arry_names = str_encycl_name.split('|')
            for index, row in enumerate(reader):
                if pd.notna(row[str_plm_code]) and ((pd.notna(row[arry_names[0]]) or pd.notna(row[arry_names[1]]))):
                    str_row = f"PLMCode-{row['AlphanumericCode']}"
                    if str_row not in prcss_row:
                        prcss_row.add(str_row)
                        print_log(f"* process_data()...")
                        process_data(conn,row,str_plm_code,arry_names)
                        print_log(f"* conn.commit()...")
                        conn.commit()
                else:
                    print(f"Fila {index} omitida - falta AlphanumericCode o FinalTitle")
        """
        df = pd.read_csv(csv_file, encoding='utf-8-sig')

        for index, row in df.iterrows():
            if pd.notna(row['AlphanumericCode']) and ((pd.notna(row['FinalTitle']) or pd.notna(row['OriginalTopic']))):
                print_log(f"* process_data()...")
                process_data(conn, row)
                conn.commit()
            else:
                print(f"Fila {index} omitida - falta AlphanumericCode o FinalTitle")
        """
        print("✅ Ingesta completada correctamente.")
    except Exception as e:
        print(f"❌ Error en el procesamiento: {e}")
    finally:
        conn.close()

if __name__ == "__main__":
    main()
