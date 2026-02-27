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

path_dic = os.path.join(RAW_DIR, "actividades_ocio.xlsx")
df_ocio = pd.read_excel(path_dic, header=0)
# Revision Dataset
df_ocio.head(5)
print("Columnas",df_ocio.head(5))
df_ocio.info()
print("Tipo Datos", df_ocio.info())
df_ocio.isna().sum()
print("Nulos",df_ocio.isna().sum())
df_ocio .duplicated().sum()
print("Duplicados", df_ocio.duplicated().sum())
# Visualizar duplicados
duplicados = df_ocio[df_ocio.duplicated(keep=False)]
print(duplicados)
# Lowercase + strip + normaliza provincia
df_ocio = df_ocio.apply(
    lambda x: x.str.lower() if x.dtype == "object" else x
)
df_ocio.columns = df_ocio.columns.str.strip().str.lower()
if "provincia" in df_ocio.columns:
    df_ocio["provincia"] = df_ocio["provincia"].apply(normaliza)
# Convertir titulos a español
df_ocio.rename(columns={
        "ccaa": "comunidad autonoma",
        "categoria": "subcategoria",
    }, inplace=True)
# Cambio de formato
cols_to_numeric = [
    "gasto_total",
    "precio_medio_entrada"
]
for col in cols_to_numeric:
    df_ocio[col] = (
        df_ocio[col]
        .astype(str)
        .str.replace(".", "", regex=False)  # elimina separador de miles
        .str.replace(",", ".", regex=False)  # coma decimal → punto
        .astype(float)
    )

df_ocio[cols_to_numeric] = df_ocio[cols_to_numeric].apply(
    pd.to_numeric, errors="coerce"
)
#Validacion de Nulos
print("Nulos",df_ocio[cols_to_numeric].info())
#Columnas necesarias
columnas_finales = [
    "mes",
    "producto",
    "subcategoria",
    "comunidad autonoma",
    "provincia",
    "gasto_total",
    "precio_medio_entrada"
]
df_ocio = df_ocio[columnas_finales]
#Verificacion de columnas
print(df_ocio.info())
print(df_ocio.head())
#Para limpiar duplicados agrupar por mes, producto, subcategoria, comunidad autonoma y provincia
#Columnas necesarias
df_ocio_agg = (
    df_ocio
    .groupby(
        ["mes", "producto", "subcategoria", "comunidad autonoma", "provincia"],
        as_index=False
    )
    .agg(
        gasto_total_promedio=("gasto_total", "mean"),
        precio_medio_entrada_promedio=("precio_medio_entrada", "mean")
    )
)
#Validacion de cambios
print(df_ocio_agg.shape)
print(df_ocio_agg.head())
print("duplicados",df_ocio_agg.duplicated(
    subset=["mes", "producto", "subcategoria", "comunidad autonoma", "provincia"]
).sum())

# Ciudades de España
freq_provincia = df_ocio_agg["provincia"].value_counts().reset_index()
freq_provincia.columns = ["provincia","frecuencia"]
print(freq_provincia)

#Tratamiento de Provincia
## Provincia = "total nacional"
#No pertenece a ninguna CCAA#
#Duplica información que ya existe desagregada#
#Rompe análisis territoriales si no se trata bie#

df_ocio_agg = df_ocio_agg[
    df_ocio_agg["provincia"].str.lower().str.strip() != "total nacional"
]
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
df_ocio_agg["provincia"] = df_ocio_agg["provincia"].replace(map_prov)
df_espana_ccaa["provincia"] = df_espana_ccaa["provincia"].replace(map_prov)

# --- Comparación
rov_esp = set(df_espana_ccaa["provincia"].dropna().unique())
rov_ocio = set(df_ocio_agg["provincia"].dropna().unique())
ocio_rov_no_match = sorted(rov_ocio - rov_esp)

print("Provincias en df_ocio_agg que NO están en la referencia:", ocio_rov_no_match)

# Producto
freq_proctucto = df_ocio_agg["producto"].value_counts().reset_index()
freq_proctucto.columns = ["procto","frecuencia"]
print(freq_proctucto)

# Subcategoria
freq_subcategoria = df_ocio_agg["subcategoria"].value_counts().reset_index()
freq_subcategoria.columns = ["subcategoria","frecuencia"]
print(freq_subcategoria)

#Se va a crear una columna nueva llamada Categoria
#Creacion de Lista
df_ocio_agg["subcategoria"] = df_ocio_agg["subcategoria"].apply(normaliza)
map_categoria = {
    "deportes y aventuras": "paisaje naturaleza",
    "actividades infantiles": "paisaje naturaleza",
    "parques tematicos": "paisaje naturaleza",
    "museo y exposiciones": "paisaje urbano",
    "cultura, teatro y danza": "paisaje urbano",
    "ferias": "compras",
    "cursos": "servicios",
    "conferencias": "otros",
    "gastronomia": "comida y bebida",
    "musica": "vida nocturna",
    "musicales": "vida nocturna",
    "cine": "vida nocturna"
}
#Crear columna
df_ocio_agg["categoria"] = df_ocio_agg["subcategoria"].map(map_categoria)
#Validacion
print("Nulos",df_ocio_agg["categoria"].isna().value_counts())
print("Tipo Datos", df_ocio_agg.info())

#Por ultimo organizar columnas del dataset
def ordenar_columnas(df_ocio_agg, orden_preferido):
    # columnas que sí existen en el df
    orden_existente = [col for col in orden_preferido if col in df_ocio_agg.columns]
    # columnas que no fueron especificadas
    resto = [col for col in df_ocio_agg.columns if col not in orden_existente]
    return df_ocio_agg[orden_existente + resto]
#Lista del orden de columnas
orden_dim_ocio = [
    "mes",
    "categoria",
    "producto",
    "subcategoria",
    "comunidad autonoma",
    "provincia",
    "gasto_total_promedio",
    "precio_medio_entrada_promedio"

]
df_ocio_agg= ordenar_columnas(df_ocio_agg,orden_dim_ocio)
#Validacion de orden
print(df_ocio_agg.columns.tolist())
print(df_ocio_agg.head(5))

#Redondear valores
df_ocio_agg["gasto_total_promedio"] = df_ocio_agg["gasto_total_promedio"].round(2)
df_ocio_agg["precio_medio_entrada_promedio"] = df_ocio_agg["precio_medio_entrada_promedio"].round(2)
print("Duplicados", df_ocio_agg.duplicated().sum())
#Importar nuevo dataset a cvs
os.makedirs(CLEAN_DIR, exist_ok=True)
output_path = os.path.join(CLEAN_DIR, "df_ocio_agg.csv")
df_ocio_agg.to_csv(
    output_path,
    index=False,
    encoding="utf-8-sig"
)