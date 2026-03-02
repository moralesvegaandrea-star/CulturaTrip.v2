# ============================================================
# Tren alta velocidad | Limpieza + validación geográfica (sin dim_geografia_es_latlon_final)
# - Limpia dataset de tren (precio, trayecto, normalización)
# - Reconstruye "df_geo" (provincia + lat/lng) usando:
#     1) dim_municipio_final.csv (admin: id_municipio, id_provincia, provincia_nombre)
#     2) dim_geografia_municipio_osm.csv (geo: id_municipio, lat, lon)
# - Valida coincidencias: destino (tren) vs provincia (geo)
# - Guarda output en data/Experimental/
# ============================================================

import pandas as pd
import os
import numpy as np
import unicodedata
import re
import requests
import matplotlib.pyplot as plt
from pathlib import Path


# =========================
# Helpers
# =========================
def load_csv(path):
    try:
        return pd.read_csv(
            path,
            encoding="utf-8",
            sep=None,  # detección automática
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
# Rutas del proyecto
# =========================
BASE_DIR = Path(__file__).resolve().parents[2]

RAW_DIR = BASE_DIR / "data" / "raw"
INTERIM_DIR = BASE_DIR / "data" / "interim"
CLEAN_DIR = BASE_DIR / "data" / "clean"
EXPERIMENTAL_DIR = BASE_DIR / "data" / "Experimental"

RAW_DIR.mkdir(parents=True, exist_ok=True)
INTERIM_DIR.mkdir(parents=True, exist_ok=True)
CLEAN_DIR.mkdir(parents=True, exist_ok=True)
EXPERIMENTAL_DIR.mkdir(parents=True, exist_ok=True)


# =========================
# 1) Cargar dataset de tren
# =========================
df_tren = load_csv(os.path.join(RAW_DIR, "Tren alta velocidad.csv"))

print("Shape tren:", df_tren.shape)
print("Columnas tren:", df_tren.columns.tolist())
print(df_tren.head(5))
print(df_tren.info())

print("Nulos tren:\n", df_tren.isna().sum())
print("Duplicados tren:", df_tren.duplicated().sum())

# Lowercase + normaliza columnas clave
df_tren = df_tren.apply(lambda x: x.str.lower() if x.dtype == "object" else x)

for c in ["empresa", "tipo_de_servicio", "tipo_de_producto", "trayecto"]:
    if c in df_tren.columns:
        df_tren[c] = df_tren[c].apply(normaliza)

# Quitar filas "total" en empresa
if "empresa" in df_tren.columns:
    df_tren["empresa"] = df_tren["empresa"].astype(str).str.strip()
    df_tren = df_tren[df_tren["empresa"].str.lower() != "total"]
    print("Quedan empresas:", df_tren["empresa"].nunique())
    print(df_tren["empresa"].value_counts().head(10))

# =========================
# 2) Convertir precio a numérico
# =========================
if "precio" in df_tren.columns:
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

    # Corregir escala del precio (4573 → 45.73)
    df_tren["precio"] = pd.to_numeric(df_tren["precio"], errors="coerce").astype(float)
    df_tren["precio"] = np.where(df_tren["precio"] > 1000,
                                 df_tren["precio"] / 100,
                                 df_tren["precio"])

    print("Precio corregido:\n", df_tren["precio"].describe())

# =========================
# 3) Separar trayecto (origen-destino)
# =========================
if "trayecto" in df_tren.columns:
    df_tren["trayecto"] = (
        df_tren["trayecto"].astype(str)
        .str.strip()
        .str.replace("–", "-", regex=False)  # guion largo
    )

    split_tray = df_tren["trayecto"].str.split("-", n=1, expand=True)
    df_tren["origen"] = split_tray[0].str.strip()
    df_tren["destino"] = split_tray[1].str.strip()

    df_tren["origen_norm"] = df_tren["origen"].astype(str).str.lower().apply(normaliza)
    df_tren["destino_norm"] = df_tren["destino"].astype(str).str.lower().apply(normaliza)

    print(df_tren[["trayecto", "origen", "destino"]].head(5))


# =========================
# 4) Seleccionar columnas necesarias
# =========================
cols_keep = ["mes", "empresa", "tipo_de_servicio", "tipo_de_producto", "origen", "destino", "precio"]
cols_keep = [c for c in cols_keep if c in df_tren.columns]
df_tren = df_tren[cols_keep].copy()

print("Shape tren (cols_keep):", df_tren.shape)
print(df_tren.head())

# Correcciones específicas de nombres (si aplican)
df_tren["destino"] = df_tren["destino"].astype(str).apply(normaliza)
df_tren["origen"] = df_tren["origen"].astype(str).apply(normaliza)

df_tren.loc[df_tren["destino"] == "alicante", "destino"] = "alicante/alacant"
df_tren.loc[df_tren["destino"] == "valencia", "destino"] = "valencia/valencia"


# ============================================================
# 5) Reconstruir df_geo (provincia + lat/lng) desde los 2 archivos adjuntos
# ============================================================
# Rutas (asegúrate de que estos archivos estén en data/clean/)
muni_path = os.path.join(CLEAN_DIR, "dim_municipio_final.csv")
osm_path = os.path.join(CLEAN_DIR, "dim_geografia_municipio_osm.csv")

df_muni = load_csv(muni_path)
df_osm = load_csv(osm_path)

# Limpieza robusta headers (BOM/espacios)
df_muni.columns = df_muni.columns.astype(str).str.replace("\ufeff", "", regex=False).str.strip()
df_osm.columns  = df_osm.columns.astype(str).str.replace("\ufeff", "", regex=False).str.strip()

# Validación columnas mínimas según tus capturas
req_muni = {"id_municipio", "id_provincia", "provincia_nombre"}
req_osm  = {"id_municipio", "lat"}  # lon o lng se valida abajo

missing_muni = req_muni - set(df_muni.columns)
if missing_muni:
    raise ValueError(f"❌ Faltan columnas en dim_municipio_final.csv: {missing_muni}")

if "id_municipio" not in df_osm.columns:
    raise ValueError(f"❌ Faltan id_municipio en dim_geografia_municipio_osm.csv. Columnas: {list(df_osm.columns)}")

if "lat" not in df_osm.columns:
    raise ValueError(f"❌ Faltan lat en dim_geografia_municipio_osm.csv. Columnas: {list(df_osm.columns)}")

# Normalizar lon -> lng
if "lon" in df_osm.columns and "lng" not in df_osm.columns:
    df_osm = df_osm.rename(columns={"lon": "lng"})

if "lng" not in df_osm.columns:
    raise ValueError(f"❌ Falta lon/lng en dim_geografia_municipio_osm.csv. Columnas: {list(df_osm.columns)}")

# Tipos
df_muni["id_municipio"] = df_muni["id_municipio"].astype(str)
df_osm["id_municipio"]  = df_osm["id_municipio"].astype(str)

# Merge admin + geo a nivel municipio
df_geo_muni = df_muni.merge(
    df_osm[["id_municipio", "lat", "lng"]],
    on="id_municipio",
    how="left"
)

# Construir df_geo a nivel provincia (lo que tu script usa para validar)
df_geo = (
    df_geo_muni.groupby(["id_provincia", "provincia_nombre"], as_index=False)
              .agg(lat=("lat", "mean"),
                   lng=("lng", "mean"))
              .rename(columns={"id_provincia": "cpro", "provincia_nombre": "provincia"})
)

# Normalizar provincia para match
df_geo["provincia"] = df_geo["provincia"].astype(str).apply(normaliza)
df_geo["cpro"] = df_geo["cpro"].astype(str).str.zfill(2)
df_geo["lat"] = pd.to_numeric(df_geo["lat"], errors="coerce")
df_geo["lng"] = pd.to_numeric(df_geo["lng"], errors="coerce")

df_geo = df_geo.dropna(subset=["provincia"]).copy()

print("✅ df_geo reconstruido:", df_geo.shape)
print(df_geo.head(5))


# =========================
# 6) Validación destino (tren) vs provincia (geo)
# =========================
df_tren_ref = set(df_tren["destino"].dropna().unique())
df_geo_ref = set(df_geo["provincia"].dropna().unique())

print("Destino en tren:", len(df_tren_ref))
print("Provincia en Geo:", len(df_geo_ref))

geo_ok = df_tren_ref.intersection(df_geo_ref)
print("Coinciden:", len(geo_ok))

geo_faltantes = df_tren_ref - df_geo_ref
print("NO encontrados en df_geo (provincia):")
for p in sorted(geo_faltantes):
    print("-", p)


# =========================
# 7) Guardar dataset final
# =========================
output_path = os.path.join(EXPERIMENTAL_DIR, "tren_alta_velocidad_clean.csv")
df_tren.to_csv(output_path, index=False)
print(f"✅ Dataset guardado en: {output_path}")