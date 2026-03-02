import pandas as pd
import os
import numpy as np
import unicodedata
import re
import matplotlib.pyplot as plt
from pathlib import Path

# 0) Helpers
# =========================
def normaliza(s: str) -> str:
    """Normaliza strings: lower, trim, sin acentos, guiones/espacios consistentes."""
    if pd.isna(s):
        return s
    s = str(s).strip().lower()
    s = unicodedata.normalize("NFKD", s).encode("ascii", "ignore").decode("utf-8")
    s = re.sub(r"\s*-\s*", "-", s)   # " - " -> "-"
    s = re.sub(r"\s+", " ", s)       # espacios múltiples -> 1
    return s

# =========================
# 1) RUTAS DEL PROYECTO
# =========================
BASE_DIR = Path(__file__).resolve().parents[2]

RAW_DIR = BASE_DIR / "data" / "raw"
INTERIM_DIR = BASE_DIR / "data" / "interim"   # ✅ NUEVO
CLEAN_DIR = BASE_DIR / "data" / "clean"
OUTPUTS_DIR = BASE_DIR / "outputs"
EXPERIMENTAL_DIR = BASE_DIR / "data" / "Experimental"

RAW_DIR.mkdir(parents=True, exist_ok=True)
INTERIM_DIR.mkdir(parents=True, exist_ok=True)  # ✅ NUEVO
CLEAN_DIR.mkdir(parents=True, exist_ok=True)
OUTPUTS_DIR.mkdir(parents=True, exist_ok=True)
EXPERIMENTAL_DIR.mkdir(parents=True, exist_ok=True)

path_dic = os.path.join(RAW_DIR, "transporte_terrestre.xlsx")
df_terrestre = pd.read_excel(path_dic, header=0)

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
print("Diccionario:", df_terrestre.shape)
print("\n--- COLUMNAS DICCIONARIO ---")
print(df_terrestre.columns.tolist())
print(df_terrestre.head(5))
#Convertir en minúscula data y headers
df_terrestre.columns = (
    df_terrestre.columns
    .str.strip()
    .str.lower()
    .str.replace(" ", "_")
)
# Revision Dataset
df_terrestre.head(5)
print("Columnas",df_terrestre.head(5))
df_terrestre.info()
print("Tipo Datos", df_terrestre.info())
df_terrestre.isna().sum()
print("Nulos",df_terrestre.isna().sum())
df_terrestre.duplicated().sum()
print("Duplicados", df_terrestre.duplicated().sum())

# Lowercase + strip + normaliza provincia
df_terrestre = df_terrestre.apply(
    lambda x: x.str.lower() if x.dtype == "object" else x
)
df_terrestre.columns = df_terrestre.columns.str.strip().str.lower()
if "tipo_transporte" in df_terrestre.columns:
    df_terrestre["tipo_transporte"] = df_terrestre["tipo_transporte"].apply(normaliza)

#Tipo de transprote
freq_tipo_transporte = df_terrestre["tipo_transporte"].value_counts().reset_index()
freq_tipo_transporte.columns = ["tipo_transporte","frecuencia"]
print(freq_tipo_transporte)

# Crear modo_transporte y distancia_tipo
df_terrestre["modo_transporte"] = df_terrestre["tipo_transporte"].str.contains(
    "ferrocarril", case=False
).map({True: "ferrocarril", False: "autobus"})

df_terrestre["distancia_tipo"] = df_terrestre["tipo_transporte"].str.contains(
    "larga", case=False
).map({True: "larga", False: "media"})

df_terrestre.head(5)
print("Columnas",df_terrestre.head(5))

df_terrestre = df_terrestre.drop(columns=["tipo_transporte"])
print(df_terrestre.head())
print(df_terrestre.isna().sum())
print("Duplicados:", df_terrestre.duplicated().sum())

# =========================
# 9) Guardar dataset final
# =========================
output_path = os.path.join(
    EXPERIMENTAL_DIR,
    "transporte_terrestre_clean.csv"
)

df_terrestre.to_csv(output_path, index=False)

print(f"Dataset guardado en: {output_path}")

