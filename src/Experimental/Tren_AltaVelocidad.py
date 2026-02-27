import pandas as pd
import os
import numpy as np
import unicodedata
import re
import requests
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

def load_csv(path):
    try:
        return pd.read_csv(
            path,
            encoding="utf-8",
            sep=None,
            engine="python",
            keep_default_na=False
        )
    except UnicodeDecodeError:
        return pd.read_csv(
            path,
            encoding="latin1",
            sep=None,
            engine="python",
            keep_default_na=False
        )



df_tren= load_csv(os.path.join(RAW_DIR, "Tren alta velocidad.csv",))

# Revision Dataset
df_tren.head(5)
print("Columnas",df_tren.head(5))
df_tren.info()
print("Tipo Datos", df_tren.info())
df_tren.isna().sum()
print("Nulos",df_tren.isna().sum())
df_tren.duplicated().sum()
print("Duplicados", df_tren.duplicated().sum())

# Lowercase + strip + normaliza provincia
df_tren = df_tren.apply(
    lambda x: x.str.lower() if x.dtype == "object" else x
)
df_tren["empresa"] = df_tren["empresa"].apply(normaliza)
df_tren["tipo_de_servicio"] = df_tren["tipo_de_servicio"].apply(normaliza)
df_tren["tipo_de_producto"] = df_tren["tipo_de_producto"].apply(normaliza)
df_tren["trayecto"] = df_tren["trayecto"].apply(normaliza)


freq_empresa = df_tren["empresa"].value_counts().reset_index()
freq_empresa.columns = ["empresa","frecuencia"]
print(freq_empresa)

df_tren["empresa"] = df_tren["empresa"].astype(str).str.strip()
df_tren = df_tren[df_tren["empresa"].str.lower() != "total"]
print("Quedan empresas:", df_tren["empresa"].nunique())
print(df_tren["empresa"].value_counts().head(10))

#Convertir el precio a formato numerico

df_tren["precio"] = (
    df_tren["precio"].astype(str)
    .str.replace("€", "", regex=False)
    .str.replace(".", "", regex=False)   # miles
    .str.replace(",", ".", regex=False)  # decimal
    .str.strip()
)

df_tren["precio"] = pd.to_numeric(df_tren["precio"], errors="coerce")

print(df_tren["precio"].describe())
print("Nulos en precio:", df_tren["precio"].isna().sum())

# Normalizar trayecto y extraer origen y destino

df_tren["trayecto"] = (
    df_tren["trayecto"].astype(str)
    .str.strip()
    .str.replace("–", "-", regex=False)  # por si hay guion largo
)

# Separar origen y destino (ej: Madrid-Barcelona)
split_tray = df_tren["trayecto"].str.split("-", n=1, expand=True)
df_tren["origen"] = split_tray[0].str.strip()
df_tren["destino"] = split_tray[1].str.strip()
# (opcional) normalizar a minúsculas para merges futuros
df_tren["origen_norm"] = df_tren["origen"].str.lower()
df_tren["destino_norm"] = df_tren["destino"].str.lower()

print(df_tren[["trayecto", "origen", "destino"]].head(5))

#Seleccionar columnas necesarias
cols_keep = [
    "mes",
    "empresa",
    "tipo_de_servicio",
    "tipo_de_producto",
    "origen",
    "destino",
    "precio"
]
df_tren = df_tren[cols_keep].copy()
print(df_tren.shape)
df_tren.head()

# Renombrar titulos
df_tren.rename(columns={
        "Name": "destino"
    }, inplace=True)
df_tren.loc[df_tren["destino"] == "alicante", "destino"] = "alicante/alacant"
df_tren.loc[df_tren["destino"] == "valencia", "destino"] = "valencia/valencia"


# Corregir escala del precio (4573 → 45.73)
df_tren["precio"] = df_tren["precio"].astype(float)

df_tren.loc[df_tren["precio"] > 1000, "precio"] = (
    df_tren.loc[df_tren["precio"] > 1000, "precio"] / 100
)

# Validación rápida
print(df_tren["precio"].describe())




df_geo = pd.read_csv(
    os.path.join(CLEAN_DIR, "dim_geografia_es_latlon_final.csv")
)
print(df_geo.shape)
print(df_geo.head(5))
print("Tipo Datos", df_geo.info())

df_tren["destino"] = df_tren["destino"].apply(normaliza)
df_geo["comunidad autonoma"] = df_geo["comunidad autonoma"].apply(normaliza)
df_geo["provincia"] = df_geo["provincia"].apply(normaliza)

df_tren_ref = set(df_tren["destino"].dropna().unique())
df_geo_ref = set(df_geo["provincia"].dropna().unique())

print("Destino en tren:", len(df_tren_ref))
print("Provincia en Geo:", len(df_geo_ref))

geo_ok = df_tren_ref.intersection(df_geo_ref)
print("Coinciden:", len(geo_ok))

geo_faltantes = df_tren_ref - df_geo_ref
print("NO encontrados en paises.csv:")
for p in sorted(geo_faltantes):
    print("-", p)
# =========================
# 9) Guardar dataset final
# =========================
output_path = os.path.join(
    CLEAN_DIR,
    "tren_alta_velocidad_clean.csv"
)

df_tren.to_csv(output_path, index=False)

print(f"Dataset guardado en: {output_path}")