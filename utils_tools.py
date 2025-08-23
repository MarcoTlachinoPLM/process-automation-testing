import json
import os
import unicodedata
from bs4 import BeautifulSoup

# Directorio donde están los archivos con errores
current_dir = os.path.dirname(os.path.abspath(__file__))
cat_image_path = 'cat_image_path.json'
cat_supplies = 'catalog_supplies.json'
path_catalogs = 'catalogs'

# Habilitar/Dehabilitar LOGs
ENABLE_LOGS = True
#ENABLE_LOGS = False

def print_log(message):
    if ENABLE_LOGS:
        print(message)

# Pestañas para extraccion de datos en el Excel
sheets = ["Hoja Contenidos", "ORI CONTENIDO"]

# Tags adicionales, identificados para RubroMaestro
tags_cln = ["NegroNormal", "BNegroNormal", "Codigok"]

def html_to_text(html_text) -> str:
    # Hacer una copia para no modificar el original
    tmp_copy = BeautifulSoup(str(html_text), 'html.parser')
    # Obtener todo el texto
    text = tmp_copy.get_text(separator=' ', strip=True)
    # Limpiar espacios múltiples y saltos de línea
    text = ' '.join(text.split())
    return text

def getBeautifulSoup(html_content):
    return BeautifulSoup(html_content, "html.parser")

def normalize_string(s: str) -> str:
    """
    Normalizar cadena para comparación (Elimina/Convierte): Espacios inicio/final, Minúsculas, Acentos/Diacríticos
    """
    # Quitar espacios al inicio y final
    s = s.strip().lower()
    # Normalizar y eliminar acentos
    s = ''.join(
        c for c in unicodedata.normalize('NFD', s)
        if unicodedata.category(c) != 'Mn'
    )
    return s

def compare_strings(s1: str, s2: str) -> bool:
    return normalize_string(s1) == normalize_string(s2)

# ======= LOAD CATALOGS =======
CAT_PATH_IMG = []
CAT_SUPPLIES = []

def init_cat_image_path() -> bool:
    print(f"Load data for Catalog of InformationTypes...")
    cat_image = os.path.join(current_dir, path_catalogs, cat_image_path)
    print(f"* cat_image: {cat_image}")
    with open(cat_image, 'r', encoding='utf-8') as file:
        cat_data = json.load(file)
    if len(cat_data) > 0:
        for item in cat_data:
            CAT_PATH_IMG.append(item)
    print(f"CAT_PATH_IMG: {len(CAT_PATH_IMG)}")
    return True

def init_cat_supplies() -> bool:
    print(f"Load data for Catalog of Insumos...")
    cat_sppls = os.path.join(current_dir, path_catalogs, cat_supplies)
    print(f"* cat_sppls: {cat_sppls}")
    with open(cat_sppls, 'r', encoding='utf-8') as file:
        cat_data = json.load(file)
    if len(cat_data) > 0:
        for item in cat_data:
            CAT_SUPPLIES.append(item)
    print(f"CAT_SUPPLIES: {len(CAT_SUPPLIES)}")
    return True

def get_path_by_info_type(info_type) -> str:
    """Obtiene Path basado en el InfoType."""
    try:
        print(f"* cat-len: {len(CAT_PATH_IMG)}, info_type: {info_type}")
        for item in CAT_PATH_IMG:
            str_type = item["InfoDescription"]
            if compare_strings(str_type, info_type):
                return item["Path"]
        raise None
    except KeyError as e:
        raise ValueError(f"Estructura de datos inválida: {str(e)}") from e

def get_supplie_by_name(str_name) -> str:
    """Obtiene FileName basado en el NameSupplie."""
    try:
        print(f"* cat-len: {len(CAT_SUPPLIES)}, str_name: {str_name}")
        for item in CAT_SUPPLIES:
            supplie = normalize_string(item["NameSupplie"])
            if compare_strings(supplie, str_name):
                return item
        return None
    except KeyError as e:
        raise ValueError(f"Estructura de datos inválida: {str(e)}") from e

def init_catalogs():
    """Inicializa todos los catálogos"""
    init_cat_image_path()
    init_cat_supplies()
# ======= LOAD CATALOGS =======
