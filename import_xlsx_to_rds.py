import os
import os
import json
import pandas as pd
# Importar funciones generales
from utils_tools import (
    print_log,
    sheets,
    init_catalogs,
    get_path_by_info_type,
    get_supplie_by_name
)
# Importar funciones para DB
from database_utils import (
    get_db_connection,
    get_catalogs_import,
    tab_cntnts_sht,
    get_information_type
)

# Configuración de la base de datos SQL Server
current_dir = os.path.dirname(os.path.abspath(__file__))
path_files = 'data_import_files'
path_move = 'data_processed_files'
path_error_move = 'data_error_files'
catalog_name = 'catalog_encyclopedias.json'
path_catalogs = 'catalogs'

# Rutas base del proyecto
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_IMPORT_DIR = os.path.join(BASE_DIR, "data_import_xlsx")
OUTPUT_DIR = os.path.join(BASE_DIR, "data_processed_files")
os.makedirs(OUTPUT_DIR, exist_ok=True)

# Guardar archivo JSON
SAVE_JSON = True

def read_excel_sheets(file_path: str):
    """Lee las pestañas requeridas del Excel."""
    data = {}
    for sheet in sheets:
        try:
            df = pd.read_excel(file_path, sheet_name=sheet)
            data[sheet] = df.fillna("").to_dict(orient="records")
        except Exception as e:
            print(f"⚠️ Error leyendo hoja '{sheet}' en '{os.path.basename(file_path)}': {e}")
            data[sheet] = []
    return data

def process_contents_sheet(rows):
    """Procesa filas de 'Hoja Contenidos' para ElectronicInformation y TherapLineElecInfo."""
    set_electronic_info = []
    for row in rows:
        title = row.get(tab_cntnts_sht.get("Title"), "")
        type = row.get(tab_cntnts_sht.get("Type"), "")
        print_log(f"type: {type}")
        if not type:
            continue
        print_log(f"get_path_by_info_type({type})...")
        file_path = get_path_by_info_type(type)
        file_name = row.get(tab_cntnts_sht.get("FileName"), "")
        supplie = None
        if not file_name:
            print_log(f"get_supplie_by_name({title})...")
            supplie = get_supplie_by_name(title)
            if supplie:
                file_name = f"{supplie["FileName"]}.webp"
        print_log(f"file_path: {file_path}, file_name: {file_name}")
        electronic_info = {
            "TypeId": int(get_information_type(type)),
            "Company": 50,
            "Title": title,
            "Description": row.get(tab_cntnts_sht.get("Description"), ""),
            "InitDate": row.get(tab_cntnts_sht.get("InitDate"), ""),
            "FileName": "",
            "Link": "",
            "HTMLFileName": "",
            "FileUrl": file_path,
            "TargetUrl": file_name,
            "ImageUrl": file_name,
            "ThumbnailUrl": "/miniaturas",
            "StartDate": "",
            "EndDate": "",
            "TherapLines": [],
            "Synonyms": [],
            "Keywords": []
        }
        therap_lines = []
        ctgrs = row.get(tab_cntnts_sht.get("Categories"), "")
        ct_arry = ctgrs.split('|')
        for ctg in ct_arry:
            if ctg.strip():
                therap_lines.append({"TherapLineId": int(ctg.strip())})
        print_log(f"therap_lines: {therap_lines}")
        if len(therap_lines):
            electronic_info["TherapLines"] = therap_lines
        synonyms = []
        synnyms = row.get(tab_cntnts_sht.get("Synonyms"), "")
        syn_arry = synnyms.split('|')
        for syn in syn_arry:
            if syn.strip():
                synonyms.append({"Synonym": syn.strip()})
        print_log(f"synonyms: {synonyms}")
        if len(synonyms):
            electronic_info["Synonyms"] = synonyms
        keywords = []
        kywrds = row.get(tab_cntnts_sht.get("Keywords"), "")
        kw_arry = kywrds.split('|')
        for wrd in kw_arry:
            if wrd.strip():
                keywords.append({"Word": wrd.strip()})
        print_log(f"keywords: {keywords}")
        if len(keywords):
            electronic_info["Keywords"] = keywords
        set_electronic_info.append(electronic_info)
    return set_electronic_info

def process_original_content(rows):
    """Procesa filas de 'ORI CONTENIDO' para CountryApplicationTools."""
    original_content = []
    #TODO: Validar...
    return original_content

def process_excel_file(conn, file_name: str):
    """Procesa un archivo XLSX y devuelve la estructura JSON final."""
    complete = True
    set_information = None
    cursor = conn.cursor()
    try:
        file_path = os.path.join(DATA_IMPORT_DIR, file_name)
        print(f"* Procesando archivo: {file_path}")
        data = read_excel_sheets(file_path)
        
        # Procesar "Hoja Contenidos"
        print_log("process_contents_sheet()...")
        set_information = process_contents_sheet(data.get("Hoja Contenidos", []))
        """
        fllw_prcss = True
        encyclopedia_id = get_encyclopedia_data(cursor, data)
        process = [True, True, True]
        if encyclopedia_id:
            print(f"Se encontraron datos existentes para EncyclopediaId: {encyclopedia_id}")
            fllw_prcss = validate_attributes(cursor, encyclopedia_id)
            process[0] = fllw_prcss
            if not fllw_prcss:
                fllw_prcss = validate_entries_terms_data(cursor, encyclopedia_id)
                process[1] = fllw_prcss
            if not fllw_prcss:
                fllw_prcss = validate_medical_entry_term(cursor, encyclopedia_id)
                process[2] = fllw_prcss
        if fllw_prcss:
            print(f"* encyclopedia_id: {encyclopedia_id}, process: {process}")
            # Insert Encyclopedia data and get the generated ID
            encyclopedia_id = encyclopedia_id if encyclopedia_id else insert_or_get_encyclopedia_data(cursor, data)
            print(f"Encyclopedia ID: {encyclopedia_id}")

            # Insert Attributes
            if data["MedicalEncyclopediaAttribute"] and process[0]:
                insert_attributes(cursor, encyclopedia_id, data["MedicalEncyclopediaAttribute"])
                print("Inserted Attributes successfully")
            
            # Insert MedicalTerms and relationships
            if data["MedicalEntriesTerms"] and (process[1] or process[2]):
                insert_entries_terms_data(cursor, encyclopedia_id, data["MedicalEntriesTerms"])
                print("Inserted MedicalTerms and relationships successfully")
            
            print(f"Datos insertados correctamente. EncyclopediaId: {encyclopedia_id}")
        """
    except Exception as e:
        complete = False
        print(f"conn.rollback()...")
        conn.rollback()
        print(f"Error al procesar archivo: {e}")
    finally:
        return {
            "Complete": complete, "SetInformation": set_information,
        }

def main():
    print(f"get_db_connection()...")
    conn = get_db_connection()
    try:
        # Inicializar Catalogos
        print_log(f"get_catalogs_import()...")
        get_catalogs_import(conn)
        init_catalogs()
        xlsx_files = [f for f in os.listdir(DATA_IMPORT_DIR) if f.endswith(".xlsx")]
        if not xlsx_files:
            print("⚠️ No se encontraron archivos .XLSX en", DATA_IMPORT_DIR)
            return
        print_log(f"No archivos: {len(xlsx_files)}")
        for file_name in xlsx_files:
            print_log(f"process_excel_file({file_name})...")
            result = process_excel_file(conn, file_name)
            if result["Complete"]:
                if SAVE_JSON:
                    final_data = result["SetInformation"]
                    # Nombre de salida .json igual que el archivo Excel
                    base_name = os.path.splitext(file_name)[0]
                    output_file = os.path.join(OUTPUT_DIR, f"{base_name}.json")
                    print_log(f"output_file: {output_file}")
                    with open(output_file, "w", encoding="utf-8") as f:
                        json.dump(final_data, f, ensure_ascii=False, indent=4)
                    print(f"✅ Archivo procesado y guardado en {output_file}")
            else:
                print(f"⚠️ No se culmino el proceso para {output_file}")
    except Exception as e:
        print(f"❌ Error en el procesamiento de archivos: {e}")
    finally:
        conn.close()

if __name__ == "__main__":
    main()
