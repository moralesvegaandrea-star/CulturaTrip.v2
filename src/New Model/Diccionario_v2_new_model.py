import os
import pandas as pd
from pathlib import Path
# =========================
# 1) RUTAS DEL PROYECTO
# =========================
BASE_DIR = Path(__file__).resolve().parents[2]

RAW_DIR = BASE_DIR / "data" / "raw"
INTERIM_DIR = BASE_DIR / "data" / "interim"   # ✅ NUEVO
CLEAN_DIR = BASE_DIR / "data" / "clean"
OUTPUTS_DIR = BASE_DIR / "outputs"

RAW_DIR.mkdir(parents=True, exist_ok=True)
INTERIM_DIR.mkdir(parents=True, exist_ok=True)  # ✅ NUEVO
CLEAN_DIR.mkdir(parents=True, exist_ok=True)
OUTPUTS_DIR.mkdir(parents=True, exist_ok=True)

# =========================
# 2) CARGAR DATASET
# =========================
path_dic = os.path.join(RAW_DIR, "diccionario25.xlsx")

# El header real está en la fila 2 del Excel → header=1
df_dic = pd.read_excel(path_dic, header=1)

# =========================
# 3) LIMPIEZA INICIAL
# =========================

# Quitar columnas "Unnamed"
df_dic = df_dic.loc[:, ~df_dic.columns.astype(str).str.contains("^Unnamed")]

# Quitar filas completamente vacías
df_dic = df_dic.dropna(how="all").reset_index(drop=True)

# Normalizar nombres de columnas
df_dic.columns = (
    df_dic.columns
    .str.strip()
    .str.lower()
    .str.replace(" ", "_")
)

print("Columnas detectadas:")
print(df_dic.columns.tolist())

# =========================
# 4) NORMALIZAR CÓDIGOS (MUY IMPORTANTE)
# =========================

# Convertir a string y aplicar padding correcto
df_dic["codauto"] = df_dic["codauto"].astype(str).str.zfill(2)
df_dic["cpro"] = df_dic["cpro"].astype(str).str.zfill(2)
df_dic["cmun"] = df_dic["cmun"].astype(str).str.zfill(3)
df_dic["dc"] = df_dic["dc"].astype(str).str.zfill(1)

# =========================
# 5) CREAR CLAVES (PK / FK)
# =========================

# ID MUNICIPIO (PK principal nacional)
df_dic["id_municipio"] = (
    df_dic["codauto"] +
    df_dic["cpro"] +
    df_dic["cmun"] +
    df_dic["dc"]
)

# id_municipio_parcial = CPRO + CMUN + DC

df_dic["id_municipio_parcial"] = (
    df_dic["cpro"] +
    df_dic["cmun"] +
    df_dic["dc"]
)

# ID PROVINCIA (FK)
df_dic["id_provincia"] = df_dic["cpro"]

# ID CCAA (FK)
df_dic["id_ccaa"] = df_dic["codauto"]

# =========================
# 6) DEJAR SOLO COLUMNAS NECESARIAS
# =========================

df_dim_municipio = df_dic[[
    "id_municipio",
    "id_municipio_parcial",
    "id_provincia",
    "id_ccaa",
    "nombre"
]].copy()

# Quitar duplicados por seguridad
df_dim_municipio = df_dim_municipio.drop_duplicates()

# =========================
# 7) VALIDACIONES BÁSICAS
# =========================

print("\nShape final:", df_dim_municipio.shape)
print(df_dim_municipio.head())
print(df_dim_municipio.dtypes)
# =========================
# 8) GUARDAR DATASET LIMPIO
# =========================

output_path = os.path.join(INTERIM_DIR, "dim_municipio_base.csv")

df_dim_municipio.to_csv(
    output_path,
    index=False,
    encoding="utf-8-sig"
)

print("\n✅ dim_municipio_base.csv guardado correctamente en /clean")
