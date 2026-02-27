import os
import time
from pathlib import Path
import requests
import pandas as pd
import unicodedata
import re

# =========================
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

def load_csv(path):
    try:
        return pd.read_csv(
            path,
            encoding="utf-8",
            sep=None,  # 🔑 detección automática
            engine="python"  # 🔑 necesario para sep=None
        )
    except UnicodeDecodeError:
        return pd.read_csv(
            path,
            encoding="latin1",
            sep=None,
            engine="python"
        )

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
RAW_DIR = os.path.join(BASE_DIR, "data", "raw")
CLEAN_DIR = os.path.join(BASE_DIR, "data", "clean")
OUTPUTS_DIR = os.path.join(BASE_DIR, "outputs")
NOTEBOOKS_DIR = os.path.join(BASE_DIR, "notebooks")

df_espana_ccaa = pd.read_csv("../data/clean/df_espana_ccaa.csv")
df_diccionario = pd.read_csv("../data/raw/df_dic.csv")
df_proj_alojamientos = pd.read_csv("../../outputs/df_alojamientos.csv")

#Objetivo incluir la columna ccaa en el dataset de df_espana_ccaa
# 1) Crear mapa provincia -> ccaa (único)
provincia_ccaa_map = (
    df_proj_alojamientos[["provincia", "ccaa"]]
    .dropna()
    .drop_duplicates(subset=["provincia"])  # garantiza 1 fila por provincia
)
# 2) Asegurar normalización (por si acaso)
df_espana_ccaa["provincia"] = df_espana_ccaa["provincia"].apply(normaliza)
provincia_ccaa_map["provincia"] = provincia_ccaa_map["provincia"].apply(normaliza)

# 3) Merge: muchas filas (df_espana_ccaa) -> una (mapa)
df_espana_ccaa_enriched = df_espana_ccaa.merge(
    provincia_ccaa_map,
    on="provincia",
    how="left",
    validate="m:1"
)
# 4) Validar que no queden provincias sin ccaa
faltantes = df_espana_ccaa_enriched["ccaa"].isna().sum()
print("Filas sin CCAA luego del merge:", faltantes)

# 5) Identificar nulos/faltates

faltantes_df = df_espana_ccaa_enriched[
    df_espana_ccaa_enriched["ccaa"].isna()
]

print(faltantes_df)
# Ceuta y Melilla son Ciudades Autónomas
ccaa_fix = {
    "ciudad autonoma de ceuta": "ceuta",
    "ciudad autonoma de melilla": "melilla"
}
df_espana_ccaa_enriched["ccaa"] = (
    df_espana_ccaa_enriched["ccaa"]
    .fillna(df_espana_ccaa_enriched["provincia"].map(ccaa_fix))
)
print(df_espana_ccaa_enriched["ccaa"].isna().sum())
df_espana_ccaa_enriched.rename(columns={
      "ccaa": "comunidad autonoma",
}, inplace=True)

# Traer codigo de comunidad autonoma
print(df_espana_ccaa_enriched.columns.tolist())
print(df_diccionario.columns.tolist())
# Limpiar nombres de columnas
df_espana_ccaa_enriched.columns = df_espana_ccaa_enriched.columns.str.lower().str.strip()
df_diccionario.columns = df_diccionario.columns.str.lower().str.strip()
# Asegurarnos que sean comparables
df_espana_ccaa_enriched["cpro"] = (
    pd.to_numeric(df_espana_ccaa_enriched["cpro"], errors="coerce")
    .astype("Int64")
    .astype(str)
    .str.zfill(2)
)

df_diccionario["cpro"] = (
    pd.to_numeric(df_diccionario["cpro"], errors="coerce")
    .astype("Int64")
    .astype(str)
    .str.zfill(2)
)

mapa_cpro_codauto = (
    df_diccionario[["cpro", "codauto"]]
    .dropna()
    .drop_duplicates()
)

#Hacer el merge
df_espana_ccaa_enriched = df_espana_ccaa_enriched.merge(
    mapa_cpro_codauto,
    on="cpro",
    how="left"
)
df_espana_ccaa_enriched.rename(columns={"codauto": "id_ccaa"}, inplace=True)
# Revision
print("Nulos en id_ccaa:", df_espana_ccaa_enriched["id_ccaa"].isna().sum())
df_espana_ccaa_enriched[["provincia", "cpro", "id_ccaa"]].head(10)

#Por ultimo organizar columnas del dataset
def ordenar_columnas(df_espana_ccaa_enriched, orden_preferido):
    # columnas que sí existen en el df
    orden_existente = [col for col in orden_preferido if col in df_espana_ccaa_enriched.columns]
    # columnas que no fueron especificadas
    resto = [col for col in df_espana_ccaa_enriched.columns if col not in orden_existente]
    return df_espana_ccaa_enriched[orden_existente + resto]
#Lista del orden de columnas
orden_dim_geografia = [
    "pais",
    "country_code",
    "id_ccaa",
    "comunidad autonoma",
    "provincia",
     "cpro",
     "nombre",
    "id_municipio",
     "cmun",
    "cisla",
    "isla",
    "dc"
]
df_espana_ccaa_enriched = ordenar_columnas(df_espana_ccaa_enriched, orden_dim_geografia)
#Validacion de orden
print(df_espana_ccaa_enriched.columns.tolist())
print(df_espana_ccaa_enriched.head(5))

#Importar nuevo dataset a cvs
os.makedirs(CLEAN_DIR, exist_ok=True)
output_path = os.path.join(CLEAN_DIR, "ciudades_espana.csv")
df_espana_ccaa_enriched.to_csv(
    output_path,
    index=False,
    encoding="utf-8-sig"
)
print("ciudades de espana guardado")