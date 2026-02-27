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

df_hvaloracion = load_csv(os.path.join(RAW_DIR, "ind_satisfaccion_menciones_hotel.csv"))

# Revision Dataset
df_hvaloracion.head(5)
print("Columnas",df_hvaloracion.head(5))
df_hvaloracion.info()
print("Tipo Datos", df_hvaloracion.info())
df_hvaloracion.isna().sum()
print("Nulos",df_hvaloracion.isna().sum())
df_hvaloracion.duplicated().sum()
print("Duplicados", df_hvaloracion.duplicated().sum())
#Columnas necesarias
columnas_finales = [
    "MES",
    "CATEGORIA_ALOJAMIENTO",
    "CCAA",
    "PROVINCIA",
    "INDICE_SATISFACCION_HOTELERA"
]
df_hvaloracion = df_hvaloracion[columnas_finales]
# Lowercase + strip + normaliza provincia
df_hvaloracion = df_hvaloracion.apply(
    lambda x: x.str.lower() if x.dtype == "object" else x
)
df_hvaloracion.columns = df_hvaloracion.columns.str.strip().str.lower()
if "ccaa" in df_hvaloracion.columns:
    df_hvaloracion["ccaa"] = df_hvaloracion["ccaa"].apply(normaliza)
df_hvaloracion.columns = df_hvaloracion.columns.str.strip().str.lower()
if "provincia" in df_hvaloracion.columns:
    df_hvaloracion["provincia"] = df_hvaloracion["provincia"].apply(normaliza)
# Renombrar titulos
df_hvaloracion.rename(columns={
        "indice_satisfaccion_hotelera": "valoraciones"
    }, inplace=True)

df_hvaloracion["valoraciones"] = (
    df_hvaloracion["valoraciones"]
    .astype(str)
    .str.replace(",", ".", regex=False)   # coma decimal -> punto
    .str.replace("%", "", regex=False)    # por si viene con %
    .str.strip()
    .replace(["nan", "none", "null", "-", "—", ""], np.nan)
)

df_hvaloracion["valoraciones"] = pd.to_numeric(df_hvaloracion["valoraciones"], errors="coerce")

# Cambio de formato
df_hvaloracion["valoraciones"] = pd.to_numeric(
    df_hvaloracion["valoraciones"],
    errors="coerce"
)
print(df_hvaloracion.info())

#  Imputación de nulos en valoraciones
# 1) Consolidar a nivel PK (primary key)
pk = ["mes", "ccaa", "provincia", "categoria_alojamiento"]

df_hvaloracion = (
    df_hvaloracion
    .groupby(pk, as_index=False)
    .agg(valoraciones=("valoraciones", "mean"))
)

# 2) Imputación (si aún hay nulos)
df_hvaloracion["valoraciones"] = df_hvaloracion["valoraciones"].fillna(
    df_hvaloracion.groupby(["categoria_alojamiento", "ccaa", "mes"])["valoraciones"].transform("mean")
)
df_hvaloracion["valoraciones"] = df_hvaloracion["valoraciones"].fillna(
    df_hvaloracion.groupby(["categoria_alojamiento", "mes"])["valoraciones"].transform("mean")
)
df_hvaloracion["valoraciones"] = df_hvaloracion["valoraciones"].fillna(df_hvaloracion["valoraciones"].mean())

print("Duplicados por PK:", df_hvaloracion.duplicated(subset=pk).sum())
print("Nulos valoraciones:", df_hvaloracion["valoraciones"].isna().sum())
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
df_hvaloracion["provincia"] = df_hvaloracion["provincia"].replace(map_prov)
df_espana_ccaa["provincia"] = df_espana_ccaa["provincia"].replace(map_prov)

# --- Comparación
prov_ref = set(df_espana_ccaa["provincia"].dropna().unique())
prov_aloj = set(df_hvaloracion["provincia"].dropna().unique())
prov_no_match = sorted(prov_aloj - prov_ref)

print("Provincias en df_hvaloracion que NO están en la referencia:", prov_no_match)

# =========================
#  Guardar dataset limpio
# =========================
#Importar nuevo dataset a cvs
os.makedirs(CLEAN_DIR, exist_ok=True)
output_path = os.path.join(CLEAN_DIR, "df_hvaloracion.csv")
df_hvaloracion.to_csv(
    output_path,
    index=False,
    encoding="utf-8-sig"
)
print("Documento Descargado y Guardado")



