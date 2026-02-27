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

df_vivienda = load_csv(os.path.join(RAW_DIR, "vut_fuente_privada_ccaa.csv"))

# Revision Dataset
df_vivienda.head(5)
print("Columnas",df_vivienda.head(5))
df_vivienda.info()
print("Tipo Datos", df_vivienda.info())
df_vivienda.isna().sum()
print("Nulos",df_vivienda.isna().sum())
df_vivienda.duplicated().sum()
print("Duplicados", df_vivienda.duplicated().sum())

# Lowercase + strip + normaliza provincia
df_vivienda = df_vivienda.apply(
    lambda x: x.str.lower() if x.dtype == "object" else x
)
df_vivienda.columns = df_vivienda.columns.str.strip().str.lower()
if "ccaa" in df_vivienda.columns:
    df_vivienda["ccaa"] = df_vivienda["ccaa"].apply(normaliza)

# Renombrar titulos
df_vivienda.rename(columns={
        "tipo_vivienda": "categoria_alojamiento",
    }, inplace=True)
# Cambio de formato
cols_to_numeric = [
    "valoraciones",
    "plazas_por_vivienda_turistica",
    "porcentaje_viviendas_turisticas",
    "precio"
]
for col in cols_to_numeric:
    df_vivienda[col] = (
        df_vivienda[col]
        .astype(str)
        .str.replace(".", "", regex=False)  # elimina separador de miles
        .str.replace(",", ".", regex=False)  # coma decimal → punto
        .astype(float)
    )

df_vivienda[cols_to_numeric] = df_vivienda[cols_to_numeric].apply(
    pd.to_numeric, errors="coerce"
)
print("Tipo Datos", df_vivienda.info())
#Columnas necesarias
columnas_finales = [
    "mes",
    "ccaa",
    "categoria_alojamiento",
    "valoraciones",
    "precio"
]
df_vivienda = df_vivienda[columnas_finales]
#Verificacion de columnas
print(df_vivienda.info())

# alojamietos
freq_vivienda = df_vivienda["categoria_alojamiento"].value_counts().reset_index()
freq_vivienda.columns = ["categoria_alojamiento","frecuencia"]
print(freq_vivienda)
#CCAA
freq_vivienda = df_vivienda["ccaa"].value_counts().reset_index()
freq_vivienda.columns = ["ccaa","frecuencia"]
print(freq_vivienda)

#Tratamiento de ccaa
## ccaa = "total nacional"
#No pertenece a ninguna CCAA#
#Duplica información que ya existe desagregada#
#Rompe análisis territoriales si no se trata bie#

df_vivienda = df_vivienda[
    df_vivienda["ccaa"].str.lower().str.strip() != "total nacional"
]

# Identificar cantidad de nulos de las variables no aplica y otros
df_otras = df_vivienda[df_vivienda["categoria_alojamiento"].isin(["otros", "no aplica"])].copy()
vars_a_evaluar = ["valoraciones", "precio"]
nulos_otras = df_otras[vars_a_evaluar].isna().sum()
print("Nulos en 'otros' y 'no aplica' por variable:\n", nulos_otras)
#Tratamiento de Nulos
df_vivienda = df_vivienda[
    ~df_vivienda["categoria_alojamiento"]
    .str.lower()
    .str.strip()
    .isin(["otros", "no aplica"])
].copy()

freq_viviendas = df_vivienda ["categoria_alojamiento"].value_counts().reset_index()
freq_viviendas.columns = ["categoria_alojamiento","frecuencia"]
print(freq_viviendas)
#Para limpiar duplicados agrupar por mes, producto, subcategoria, comunidad autonoma y provincia
#Columnas necesarias
df_vivienda_agg = (
    df_vivienda
    .groupby(
        ["mes", "ccaa", "categoria_alojamiento"],
        as_index=False
    )
    .agg(
        valoraciones_promedio=("valoraciones", "mean"),
        precio_promedio=("precio", "mean")
    )
)

#Validacion de cambios
print(df_vivienda_agg.shape)
print(df_vivienda_agg.head())
print("duplicados",df_vivienda_agg.duplicated(
    subset=["mes", "ccaa", "categoria_alojamiento"]
).sum())
print("Nulos",df_vivienda_agg.isna().sum())
#Analizar filas con nulos
filas_con_nulos = df_vivienda_agg[df_vivienda_agg.isna().any(axis=1)]
print(filas_con_nulos)
g = filas_con_nulos.iloc[0]
mask = (
    (df_vivienda["mes"] == g["mes"]) &
    (df_vivienda["ccaa"] == g["ccaa"]) &
    (df_vivienda["categoria_alojamiento"] == g["categoria_alojamiento"])
)

print(df_vivienda.loc[mask, ["valoraciones", "precio"]].isna().sum())
print("Total filas del grupo:", mask.sum())
df_vivienda_agg = df_vivienda_agg.dropna(subset=["precio_promedio"]).copy()
print("Nulos",df_vivienda_agg.isna().sum())

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

if "ccaa" in df_espana_ccaa.columns:
    df_espana_ccaa["comunidad autonoma"] = df_espana_ccaa["comunidad autonoma"].apply(normaliza)

# --- Mapping típico Ceuta/Melilla (evita no-match falsos)
map_prov = {
    "ciudad autonoma de ceuta": "ceuta",
    "ciudad autónoma de ceuta": "ceuta",
    "ciudad autonoma de melilla": "melilla",
    "ciudad autónoma de melilla": "melilla",
}
df_vivienda_agg["ccaa"] = df_vivienda_agg["ccaa"].replace(map_prov)
df_espana_ccaa["comunidad autonoma"] = df_espana_ccaa["comunidad autonoma"].replace(map_prov)

# --- Comparación
prov_ref = set(df_espana_ccaa["comunidad autonoma"].dropna().unique())
prov_aloj = set(df_vivienda_agg["ccaa"].dropna().unique())
prov_no_match = sorted(prov_aloj - prov_ref)

print("Provincias en df_alojamientos que NO están en la referencia:", prov_no_match)

print("df_vivienda_agg cols:", df_vivienda_agg.columns.tolist())
print("df_espana_ccaa cols:", df_espana_ccaa.columns.tolist())
# 1) Normaliza headers
df_espana_ccaa.columns = df_espana_ccaa.columns.str.strip().str.lower()

# 2) Renombrar a una llave común
df_espana_ccaa = df_espana_ccaa.rename(columns={"comunidad autonoma": "ccaa"})

# 3) Normalizar textos
df_vivienda_agg["ccaa"] = df_vivienda_agg["ccaa"].apply(normaliza)
df_espana_ccaa["ccaa"] = df_espana_ccaa["ccaa"].apply(normaliza)

#Merge
prov_por_ccaa = df_espana_ccaa[["ccaa", "provincia"]].dropna().drop_duplicates()
df_vivienda_con_prov = df_vivienda_agg.merge(
    prov_por_ccaa,
    on="ccaa",
    how="left"
)

print(df_vivienda_con_prov.shape)
print(df_vivienda_con_prov[["mes","ccaa","categoria_alojamiento","provincia"]].head())
print("Nulos provincia:", df_vivienda_con_prov["provincia"].isna().sum())

#Objetivo hacer append con el dataset alojamientos.
# Paso incorporar pecio_checkin_entre_semana y precio_check
#Cambiar nombre de precio_promedio precio_checkin_entre_semana
#Duplicar valor de precio en precio_checkin_fin_semana
#Con el proposito de duplicar el valor no introduce sesgo, solo compatibilidad estructural
df_vivienda_con_prov = df_vivienda_con_prov.rename(
    columns={"precio_promedio": "precio_checkin_entre_semana"}
)
#Duplicar
df_vivienda_con_prov["precio_checkin_fin_semana"] = (
    df_vivienda_con_prov["precio_checkin_entre_semana"]
)
#Validar Cambios
print(df_vivienda_con_prov[[
    "precio_checkin_entre_semana",
    "precio_checkin_fin_semana"
]].head())

print("Nulos:\n", df_vivienda_con_prov[[
    "precio_checkin_entre_semana",
    "precio_checkin_fin_semana"
]].isna().sum())

# =========================
# Crear periodo_antelacion (simulación basada en estacionalidad)
# =========================

def antelacion_por_mes(mes):
    """
    Regla simple:
    - Alta demanda: junio, julio, agosto, diciembre -> "2-3 meses"
    - Resto -> "1 mes"
    """
    try:
        mes = int(mes)
    except (TypeError, ValueError):
        return pd.NA  # por si mes viene raro

    if mes in [6, 7, 8, 12]:
        return "2-3 meses"
    else:
        return "1 mes"

# Asegurar que mes sea numérico (si viene como string)
df_vivienda_con_prov["mes"] = pd.to_numeric(df_vivienda_con_prov["mes"], errors="coerce")

# Crear la columna
df_vivienda_con_prov["periodo_antelacion"] = df_vivienda_con_prov["mes"].apply(antelacion_por_mes)

# Validación rápida
print(df_vivienda_con_prov[["mes", "ccaa", "categoria_alojamiento", "periodo_antelacion"]].head(10))
print("Nulos en periodo_antelacion:", df_vivienda_con_prov["periodo_antelacion"].isna().sum())
print(df_vivienda_con_prov["periodo_antelacion"].value_counts(dropna=False))

#Redondear valores
df_vivienda_con_prov["precio_checkin_entre_semana"] = df_vivienda_con_prov["precio_checkin_entre_semana"].round(2)
df_vivienda_con_prov["precio_checkin_entre_semana"] = df_vivienda_con_prov["precio_checkin_entre_semana"].round(2)
df_vivienda_con_prov["valoraciones_promedio"] = df_vivienda_con_prov["valoraciones_promedio"].round(2)

# Renombrar titulos
df_vivienda_con_prov.rename(columns={
        "valoraciones_promedio": "valoraciones",
    }, inplace=True)

print("Duplicados", df_vivienda_con_prov.duplicated().sum())
# =========================
#  Guardar dataset limpio
# =========================
#Importar nuevo dataset a cvs
os.makedirs(CLEAN_DIR, exist_ok=True)
output_path = os.path.join(CLEAN_DIR, "df_vivienda_con_prov.csv")
df_vivienda_con_prov.to_csv(
    output_path,
    index=False,
    encoding="utf-8-sig"
)
print("Documento Descargado y Guardado")
