import os
import pandas as pd

# =========================
# 1) RUTAS DEL PROYECTO
# =========================
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

RAW_DIR = os.path.join(BASE_DIR, "data", "raw")
CLEAN_DIR = os.path.join(BASE_DIR, "data", "clean")
OUTPUTS_DIR = os.path.join(BASE_DIR, "outputs")
NOTEBOOKS_DIR = os.path.join(BASE_DIR, "notebooks")

os.makedirs(CLEAN_DIR, exist_ok=True)
os.makedirs(OUTPUTS_DIR, exist_ok=True)

path_dic = os.path.join(RAW_DIR, "diccionario25.xlsx")
df_dic = pd.read_excel(path_dic, header=1)

# =========================
# 2) DICCIONARIO (SÍ TIENE HEADERS)
# =========================
def load_diccionario_ine(path, header_row=1):
    """
    Carga diccionario25.xlsx que sí tiene headers reales.
    header_row=1 significa: fila 2 en Excel (pandas cuenta desde 0).
    """
    df = pd.read_excel(path, header=header_row)

    # Quitar columnas Unnamed
    df = df.loc[:, ~df.columns.astype(str).str.contains("^Unnamed", case=False)]

    # Quitar filas completamente vacías
    df = df.dropna(how="all").reset_index(drop=True)

    # Normalizar nombres de columnas
    df.columns = (
        df.columns.astype(str)
        .str.strip()
        .str.lower()
        .str.replace(" ", "_")
    )
    return df

# =========================
# 5) VALIDACIONES BÁSICAS (PRINCIPIANTE)
# =========================
print("\n--- SHAPES ---")
print("Diccionario:", df_dic.shape)
print("\n--- COLUMNAS DICCIONARIO ---")
print(df_dic.columns.tolist())
print(df_dic.head(5))
#Convertir en minúscula data y headers
df_dic.columns = (
    df_dic.columns
    .str.strip()
    .str.lower()
    .str.replace(" ", "_")
)
# Normalizar formatos
df_dic["cpro"] = df_dic["cpro"].astype(str).str.zfill(2)
df_dic["cmun"] = df_dic["cmun"].astype(str).str.zfill(3)

# Crear ID único nacional
df_dic["id_municipio"] = df_dic["cpro"] + df_dic["cmun"]
print(df_dic.head())
#Importar nuevo dataset a cvs
os.makedirs(RAW_DIR, exist_ok=True)
output_path = os.path.join(RAW_DIR, "df_dic.csv")
df_dic.to_csv(
    output_path,
    index=False,
    encoding="utf-8-sig"
)
print("Documento Descargado y Guardado")