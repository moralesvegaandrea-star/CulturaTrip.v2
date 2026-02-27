import os
import pandas as pd
from pathlib import Path
# =========================
# 1) RUTAS
# =========================
BASE_DIR = Path(__file__).resolve().parents[2]
RAW_DIR = BASE_DIR / "data" / "raw"
CLEAN_DIR = BASE_DIR / "data" / "clean"
OUTPUTS_DIR = BASE_DIR / "outputs"

os.makedirs(CLEAN_DIR, exist_ok=True)

path_ccaa = os.path.join(RAW_DIR, "codccaa.xls")

# =========================
# 2) LEER ARCHIVO (HEADER REAL EN FILA 2)
# =========================
df_ccaa = pd.read_excel(path_ccaa, header=1, engine="xlrd")

print("Columnas detectadas:", df_ccaa.columns.tolist())
print(df_ccaa.head())

# =========================
# 3) NORMALIZAR COLUMNAS
# =========================
df_ccaa.columns = (
    df_ccaa.columns
    .astype(str)
    .str.strip()
    .str.lower()
)

# Renombrar correctamente
df_ccaa = df_ccaa.rename(columns={
    "codigo": "id_ccaa",
    "literal": "ccaa_nombre"
})

# =========================
# 4) LIMPIEZA + FORMATO
# =========================
df_ccaa = df_ccaa[["id_ccaa", "ccaa_nombre"]].copy()

df_ccaa["id_ccaa"] = df_ccaa["id_ccaa"].astype(str).str.strip().str.zfill(2)
df_ccaa["ccaa_nombre"] = df_ccaa["ccaa_nombre"].astype(str).str.strip()

# Quitar vacíos y duplicados
df_ccaa = df_ccaa[df_ccaa["id_ccaa"] != ""].drop_duplicates().reset_index(drop=True)

# =========================
# 5) AGREGAR id_pais (ESCALABILIDAD)
# =========================
df_ccaa.insert(0, "id_pais", "ES")

# (Opcional) id global
df_ccaa["gid_ccaa"] = df_ccaa["id_pais"] + "-" + df_ccaa["id_ccaa"]

print("\nShape final:", df_ccaa.shape)
print(df_ccaa)

print(df_ccaa.dtypes)

# =========================
# 6) GUARDAR
# =========================
output_path = os.path.join(CLEAN_DIR, "dim_ccaa_base.csv")
df_ccaa.to_csv(output_path, index=False, encoding="utf-8-sig")

print("\n✅ dim_ccaa_base.csv actualizado con id_pais (y gid_ccaa)")
