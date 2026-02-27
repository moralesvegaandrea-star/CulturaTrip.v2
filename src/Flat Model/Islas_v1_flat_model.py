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

path_islas = os.path.join(RAW_DIR, "25codislas.xlsx")

def parse_islas_sheet(raw_sheet: pd.DataFrame, sheet_name: str) -> pd.DataFrame:
    """
    raw_sheet: DataFrame leído con header=None (hoja completa).
    sheet_name: '07', '35', '38'...
    Devuelve la tabla limpia + columna provincia tomada de la fila 2.
    """

    # 1) Provincia está en A2 (fila index 1, col 0)
    provincia = str(raw_sheet.iloc[1, 0]).strip()

    # 2) Headers reales están en la fila 3 (index 2)
    header_row = 2
    headers = raw_sheet.iloc[header_row].astype(str).str.strip().str.lower().tolist()

    # 3) Datos empiezan en la fila 4 (index 3)
    df = raw_sheet.iloc[header_row + 1:].copy()
    df.columns = headers

    # 4) Quitar filas vacías
    df = df.dropna(how="all").reset_index(drop=True)

    # 5) Asegurar códigos como string con ceros
    df["cpro"] = df["cpro"].astype(str).str.zfill(2)
    df["cmun"] = df["cmun"].astype(str).str.zfill(3)
    df["cisla"] = df["cisla"].astype(str).str.zfill(3)

    # 6) Agregar columna provincia (del header)
    df["provincia"] = provincia

    # (opcional) agregar nombre de hoja, por trazabilidad
    df["sheet"] = str(sheet_name)

    return df

# 1) Leer todas las hojas como diccionario
sheets = pd.read_excel(path_islas, sheet_name=None, header=None)

# 2) Parsear y unir
dfs = []
for sheet_name, raw_sheet in sheets.items():
    if not str(sheet_name).isdigit():
        continue

    df_sheet = parse_islas_sheet(raw_sheet, sheet_name)
    dfs.append(df_sheet)

df_islas = pd.concat(dfs, ignore_index=True)

# Crear ID único nacional
df_islas["id_municipio"] = df_islas["cpro"] + df_islas["cmun"]
print(df_islas.head())

print("Islas unificadas:", df_islas.shape)
print(df_islas.head(10))
print("Provincias:", df_islas["provincia"].unique())
print(df_islas.tail(10))

#Importar nuevo dataset a cvs
os.makedirs(RAW_DIR, exist_ok=True)
output_path = os.path.join(RAW_DIR, "df_islas.csv")
df_islas.to_csv(
    output_path,
    index=False,
    encoding="utf-8-sig"
)
print("Documento Descargado y Guardado")