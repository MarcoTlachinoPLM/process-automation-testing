import pandas as pd
import json
import os

# Rutas base del proyecto
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
PATH = os.path.join(BASE_DIR, "catalogs")
file_name = 'estatus_insumos.csv'
catalog_name = 'catalog_supplies.json'

# === Ruta al archivo CSV ===
csv_file = os.path.join(PATH, file_name)
json_file = os.path.join(PATH, catalog_name)

def csv_to_json():
    """Convierte un archivo CSV a JSON"""
    # Leer CSV
    df = pd.read_csv(csv_file)
    # Seleccionar solo las columnas necesarias
    df_selected = df[["TIPO", "NOMBRE DEL INSUMO", "NOMBRE DEL ARCHIVO", "MINIATURA"]]
    # Reemplazar NaN por cadenas vacías
    df_selected = df_selected.fillna("")
    # Transformar al formato deseado
    json_data = []
    #data_json = df_selected.to_dict(orient="records")
    for _, row in df_selected.iterrows():
        if row["TIPO"] and row["NOMBRE DEL INSUMO"] and row["NOMBRE DEL ARCHIVO"]:
            json_data.append({
                "Type": row["TIPO"],
                "NameSupplie": row["NOMBRE DEL INSUMO"],
                "FileName": row["NOMBRE DEL ARCHIVO"],
                "Thumbnail": 1 if str(row["MINIATURA"]).strip().upper() == "VERDADERO" else 0
            })
    # Guardar en archivo JSON con indentación y soporte UTF-8
    with open(json_file, "w", encoding="utf-8") as f:
        json.dump(json_data, f, indent=2, ensure_ascii=False)
    print(f"✅ Archivo JSON generado en: {os.path.abspath(json_file)}")
if __name__ == "__main__":
    csv_to_json()
