import configparser
import os
import pyodbc
from datetime import datetime

# Configuración de la base de datos SQL Server
current_dir = os.path.dirname(os.path.abspath(__file__))
config = configparser.ConfigParser()
config.read(os.path.join(current_dir, 'config.ini'))

DB_CONFIG = {
    "server": config["database"]["server"],
    "database": config["database"]["database"],
    "username": config["database"]["username"],
    "password": config["database"]["password"]
}

# Diccionarios y catalogos
CAT_IFO_TYPES = {}
CAT_THRP_LINE = {}

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

# ======= CATALOGS FOR IMPORT =======
# Mapeo de Datos a extraer por pestaña
tab_cntnts_sht = {
  "Type": "InfoDescription",
  "Title": "ElectronicTitle",
  "Description": "ElectronicDescription",
  "InitialDate": "InitialDate",
  "FileName": "FileName",
  "Categories": "Categorías",
  "Synonyms": "Sinónimos",
  "Keywords": "Palabras clave"
}

def get_cat_information_types(conn):
    """Recupera diccionario de EntryTermType."""
    with conn.cursor() as cursor:
        cursor.execute(
            "SELECT InfoTypeId, InfoDescription FROM InformationTypes WHERE Active = 1 ORDER BY InfoDescription ASC;"
        )
        for row in cursor.fetchall():
            id_value = row[0]
            name_value = row[1]
            CAT_IFO_TYPES[name_value] = id_value
        return True

def get_cat_therapeutic_line(conn):
    """Recupera diccionario de EntryTermType."""
    with conn.cursor() as cursor:
        cursor.execute(
            "SELECT Description, TherapeuticLineId FROM TherapeuticLine ORDER BY Description ASC;"
        )
        for row in cursor.fetchall():
            id_value = row[1]
            name_value = row[0]
            CAT_THRP_LINE[id_value] = name_value
        return True

def get_catalogs_import(conn=None):
    """Recupera todos los catálogos de una vez."""
    close_conn = False
    if conn is None:
        conn = get_db_connection()
        close_conn = True
    try:
        get_cat_information_types(conn)
        get_cat_therapeutic_line(conn)
        return True
    finally:
        if close_conn:
            conn.close()

def clear_catalogs_csv():
    """Limpia todos los diccionarios de catálogos."""
    CAT_IFO_TYPES.clear()
    CAT_THRP_LINE.clear()
# ======= DB GET CATALOGS FOR IMPORT =======

# ======= FUNCTIONS FOR CATALOGS =======
def get_information_type(str_type):
    lower_str = str_type.lower()
    for typ in CAT_IFO_TYPES:
        if typ.lower() in lower_str:
            return CAT_IFO_TYPES[typ]
    return None

def get_therap_line_by_id(id):
    for tl_id in CAT_THRP_LINE:
        if tl_id in id:
            return CAT_THRP_LINE[tl_id]
    return None
# ======= FUNCTIONS FOR CATALOGS =======

# ======= FUNCTIONS FOR DATA INGESTION =======
def get_electronic_info(cursor, data):
    # Check if exists
    query_select = """
        SELECT ElectronicId FROM PLMClients.dbo.ElectronicInformation
        WHERE Active = 1 AND InfoTypeId = ? AND HTMLFileName = 'MEX' AND ElectronicTitle = ?
        AND LastUpdate IS NOT NULL AND IsNew IS NOT NULL;
    """
    cursor.execute(
        query_select, 
        (data["TypeId"], data["Title"])
    )
    row = cursor.fetchone()
    if row:
        return row[0]
    else:
        return None

def insert_update_electronic_info(cursor, data):
    """Inserta datos en la tabla ElectronicInformation y devuelve el ID generado"""
    # Check if exists
    query_select = """
        SELECT ElectronicId FROM PLMClients.dbo.ElectronicInformation
        WHERE Active = 1 AND InfoTypeId = ? AND HTMLFileName = 'MEX' AND ElectronicTitle = ?;
    """
    cursor.execute(
        query_select, 
        (data["TypeId"], data["Title"])
    )
    row = cursor.fetchone()
    elctrnc_id = None
    if row:
        elctrnc_id = row[0]
    if elctrnc_id:
        query_update = """
            UPDATE ElectronicInformation
            SET 
                CompanyClientId = ?,
                ElectronicDescription = ?,
                Link = ?,
                HTMLFileName = ?,
                FileUrl = ?,
                TargetUrl = ?,
                ImageUrl = ?,
                ThumbnailUrl = ?,
                StartDate = CONVERT(DATETIME, ?, 120),
                EndDate = CONVERT(DATETIME, ?, 120),
                LastUpdate = ?,
                IsNew = 0
            WHERE ElectronicId = ?;
        """
        print(f"UPDATE ElectronicInformation ...")
        cursor.execute(query_update, (
            data["Company"],
            data["Description"],
            data["Link"],
            data["HTMLFileName"],
            data["FileUrl"],
            data["TargetUrl"],
            data["ImageUrl"],
            data["ThumbnailUrl"],
            data["StartDate"],
            data["EndDate"],
            datetime.now(),
            elctrnc_id
        ))
    else:
        query_insert = """
            INSERT INTO ElectronicInformation (
                InfoTypeId, CompanyClientId, ElectronicTitle, ElectronicDescription, PublishedDate, FileName, Link, Active,
                HTMLFileName, FileUrl, TargetUrl, ImageUrl, ThumbnailUrl, StartDate, EndDate, IsNew
            )
            OUTPUT INSERTED.ElectronicId
            VALUES (
                ?, ?, ?, ?, ?, ?, ?, ?, 1, 'MEX', ?, ?, ?, ?, ?, CONVERT(DATETIME, ?, 120), CONVERT(DATETIME, ?, 120), ?, ?, ?
            );
        """
        print(f"INSERT INTO MedicalEncyclopedia ()...")
        cursor.execute(query_insert, (
            data["TypeId"],
            data["Company"],
            data["Title"],
            data["Description"],
            datetime.now(),
            data["FileName"],
            data["Link"],
            1,
            data["HTMLFileName"],
            data["FileUrl"],
            data["TargetUrl"],
            data["ImageUrl"],
            data["ThumbnailUrl"],
            data["StartDate"],
            data["EndDate"],
            1
        ))
        elctrnc_id = cursor.fetchone()[0]
        return elctrnc_id

def insert_update_country_app_tools(cursor, elctrnc_id, data):
    """Inserta datos en la tabla CountryApplicationTools y devuelve el ID generado"""
    # Check if exists
    query_select = """
        SELECT * FROM CountryApplicationTools
        WHERE ElectronicId=? AND InfoTypeId=? AND CountryId=? AND PrefixId=? AND TargetId=?;
    """
    cursor.execute(
        query_select, 
        (elctrnc_id, data["TypeId"], "MEX", "PREFIX", 2)
    )
    row = cursor.fetchone()
    elctrnc_id = None
    if row:
        elctrnc_id = row[0]
    if not elctrnc_id:
        query_insert = """
            INSERT INTO CountryApplicationTools (
                ElectronicId, InfoTypeId, CountryId, PrefixId, TargetId
            )
            OUTPUT INSERTED.ElectronicId
            VALUES (?, ?, ?, ?, ?, ?);
        """
        print(f"INSERT INTO MedicalEncyclopedia ()...")
        cursor.execute(query_insert, (
            elctrnc_id,
            data["TypeId"],
            "MEX",
            "PREFIX",
            2
        ))
        elctrnc_id = cursor.fetchone()[0]
        return elctrnc_id

def insert_update_country_app_tools(cursor, elctrnc_id, term_id):
    """Inserta datos en la tabla ElectronicInfoMedicalTerm y devuelve el ID generado"""
    # Check if exists
    query_select = """
        SELECT * FROM ElectronicInfoMedicalTerm
        WHERE ElectronicId=? AND TermId=?;
    """
    cursor.execute(
        query_select, 
        (elctrnc_id, term_id)
    )
    row = cursor.fetchone()
    elctrnc_id = None
    if row:
        elctrnc_id = row[0]
    if not elctrnc_id:
        query_insert = """
            INSERT INTO ElectronicInfoMedicalTerm (ElectronicId, TermId)
            OUTPUT INSERTED.ElectronicId
            VALUES (?, ?);
        """
        print(f"INSERT INTO MedicalEncyclopedia ()...")
        cursor.execute(query_insert, (elctrnc_id, term_id))
        elctrnc_id = cursor.fetchone()[0]
        return elctrnc_id
# ======= FUNCTIONS FOR DATA INGESTION =======

# Ejemplo de uso
if __name__ == "__main__":
    # Cargar todos los catálogos para import process
    get_catalogs_import()
    
    print("* Import catalogs loaded:")
    print(f"InformationTypes: {len(CAT_IFO_TYPES)}")
    print(f"Items: {CAT_IFO_TYPES}")
