import pandas as pd
import os
import numpy as np
import unicodedata
import re
import matplotlib.pyplot as plt
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

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
RAW_DIR = os.path.join(BASE_DIR, "data", "raw")
CLEAN_DIR = os.path.join(BASE_DIR, "data", "clean")
OUTPUTS_DIR = os.path.join(BASE_DIR, "outputs")
NOTEBOOKS_DIR = os.path.join(BASE_DIR, "notebooks")

df_atracciones = load_csv(os.path.join(RAW_DIR, "atracciones_recursos_turisticos.csv"))
# Revision Dataset
df_atracciones.head(5)
print("Columnas",df_atracciones.head(5))
df_atracciones.info()
print("Tipo Datos", df_atracciones.info())
df_atracciones.isna().sum()
print("Nulos",df_atracciones.isna().sum())
df_atracciones.duplicated().sum()
print("Duplicados", df_atracciones.duplicated().sum())
# Lowercase + strip + normaliza provincia
df_atracciones = df_atracciones.apply(
    lambda x: x.str.lower() if x.dtype == "object" else x
)
df_atracciones.columns = df_atracciones.columns.str.strip().str.lower()
if "provincia" in df_atracciones.columns:
    df_atracciones["provincia"] = df_atracciones["provincia"].apply(normaliza)

#Convertir titulos a español
df_atracciones.rename(columns={
      "ccaa": "comunidad autonoma"
}, inplace=True)

print(df_atracciones.columns.tolist())

#Convertir titulos a español
df_atracciones.rename(columns={
      "ccaa": "comunidad autonoma"
}, inplace=True)

df_atracciones[[
    "valoracion_por_categoria",
    "valoracion_general",
]].head(10)

# Cambio de formato
cols_to_numeric = [
    "valoracion_por_categoria",
    "valoracion_general",
    "total_opiniones_categoria"
]

for col in cols_to_numeric:
    df_atracciones[col] = (
        df_atracciones[col]
        .astype(str)
        .str.replace(".", "", regex=False)  # elimina separador de miles
        .str.replace(",", ".", regex=False)  # coma decimal → punto
        .astype(float)
    )

df_atracciones[cols_to_numeric] = df_atracciones[cols_to_numeric].apply(
    pd.to_numeric, errors="coerce"
)
#Validacion de Nulos
print("Nulos",df_atracciones[cols_to_numeric].info())

# =========================
# Cargar dataset referencia (df_espana_ccaa) y comparar provincias
# =========================
# Ajusta esta ruta si tu archivo está en otro lugar.
# Si lo tienes en tu proyecto: "../data/clean/df_espana_ccaa.csv"
# En tu entorno actual (subida a este chat) lo vi como: "/mnt/data/df_espana_ccaa.csv"
df_espana_ccaa = pd.read_csv("../data/clean/ciudades_espana.csv")

# Lowercase + strip + normaliza provincia
df_espana_ccaa = df_espana_ccaa.apply(
    lambda x: x.str.lower() if x.dtype == "object" else x
)
df_espana_ccaa.columns = df_espana_ccaa.columns.str.strip().str.lower()

if "provincia" in df_espana_ccaa.columns:
    df_espana_ccaa["provincia"] = df_espana_ccaa["provincia"].apply(normaliza)

# --- Mapping típico Ceuta/Melilla (evita no-match falsos)
map_prov = {
    "ciudad autonoma de ceuta": "ceuta",
    "ciudad autónoma de ceuta": "ceuta",
    "ciudad autonoma de melilla": "melilla",
    "ciudad autónoma de melilla": "melilla",
}
df_atracciones["provincia"] = df_atracciones["provincia"].replace(map_prov)
df_espana_ccaa["provincia"] = df_espana_ccaa["provincia"].replace(map_prov)

# --- Comparación
arov_ref = set(df_espana_ccaa["provincia"].dropna().unique())
arov_aloj = set(df_atracciones["provincia"].dropna().unique())
arov_no_match = sorted(arov_aloj - arov_ref)

print("Provincias en df_espana_ccaa que NO están en la referencia:", arov_no_match)

# Categoria
freq_atracciones = df_atracciones["categoria"].value_counts().reset_index()
freq_atracciones.columns = ["categoria","frecuencia"]
print(freq_atracciones)

#Remplazo de valores en categoria
df_atracciones["categoria"] = df_atracciones["categoria"].replace("desconocido", "otros")
df_atracciones["categoria"] = df_atracciones["categoria"].replace("activo naturaleza", "paisaje naturaleza")
df_atracciones["categoria"] = df_atracciones["categoria"].replace("activo urbano", "paisaje urbano")
df_atracciones["categoria"] = df_atracciones["categoria"].replace("museos", "paisaje urbano")

print("duplicados",df_atracciones.duplicated(
    subset=["categoria","comunidad autonoma", "provincia"]
).sum())
#Tratamiento de duplicados
df_atracciones_agg = (
    df_atracciones
    .groupby(
        ["categoria","comunidad autonoma", "provincia"],
        as_index=False
    )
    .agg(
        valoracion_por_categoria_promedio=("valoracion_por_categoria", "mean"),
        valoracion_general_promedio=("valoracion_general", "mean"),
        total_opiniones_categoria_promedio=("total_opiniones_categoria", "mean")
    )
)
#Validacion de cambios
print(df_atracciones_agg.shape)
print(df_atracciones_agg.head())

print("duplicados",df_atracciones_agg.duplicated(
    subset=["categoria","comunidad autonoma", "provincia"]
).sum())

#Redondear valores
df_atracciones_agg["valoracion_por_categoria_promedio"] = df_atracciones_agg["valoracion_por_categoria_promedio"].round(2)
df_atracciones_agg["valoracion_general_promedio"] = df_atracciones_agg["valoracion_general_promedio"].round(2)


#Importar nuevo dataset a cvs
os.makedirs(CLEAN_DIR, exist_ok=True)
output_path = os.path.join(CLEAN_DIR, "recursos_turisticos.csv")
df_atracciones_agg.to_csv(
    output_path,
    index=False,
    encoding="utf-8-sig"
)
