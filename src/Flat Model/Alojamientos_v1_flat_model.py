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


def load_csv(path: str) -> pd.DataFrame:
    """Carga CSV con autodetección de separador y fallback de encoding."""
    try:
        return pd.read_csv(path, encoding="utf-8", sep=None, engine="python")
    except UnicodeDecodeError:
        return pd.read_csv(path, encoding="latin1", sep=None, engine="python")


# =========================
# 1) URL del API
# =========================
BASE_URL = (
    "https://dataestur.azure-api.net/API-SEGITTUR-v1/"
    "PRECIOS_ALOJAMIENTOS_HOTELEROS_DL?"
    "desde%20%28a%C3%B1o%29=2023&desde%20%28mes%29=01&"
    "hasta%20%28a%C3%B1o%29=2026&hasta%20%28mes%29=12&"
    "CCAA=Todos&Provincia=Todos"
)

# =========================
# 2) Rutas del proyecto
# =========================
BASE_DIR = Path(__file__).resolve().parents[1]
RAW_DIR = BASE_DIR / "data" / "raw"
CLEAN_DIR = BASE_DIR / "data" / "clean"
OUTPUTS_DIR = os.path.join(BASE_DIR, "outputs")

RAW_DIR.mkdir(parents=True, exist_ok=True)
CLEAN_DIR.mkdir(parents=True, exist_ok=True)

RAW_CSV_PATH = RAW_DIR / "precio_alojamientos_raw.csv"
OUT_PATH = CLEAN_DIR / "df_proj_alojamientos.csv"  # salida final limpia

headers = {"accept": "application/octet-stream"}
session = requests.Session()

# =========================
# 3) Descargar CSV del API (con reintentos)
# =========================
if not RAW_CSV_PATH.exists():
    max_reintentos = 3
    for intento in range(1, max_reintentos + 1):
        response = session.get(BASE_URL, headers=headers, timeout=60)
        print("Content-Type:", response.headers.get("Content-Type"))

        if response.status_code == 200:
            RAW_CSV_PATH.write_bytes(response.content)
            print("Guardado:", RAW_CSV_PATH)
            break

        elif response.status_code == 429:
            espera = 30 * intento
            print(f"⚠️ 429 Too Many Requests. Esperando {espera}s... (intento {intento}/{max_reintentos})")
            time.sleep(espera)
            continue

        else:
            print("Error:", response.status_code)
            print("Respuesta (primeros 300 chars):", response.text[:300])
            raise SystemExit
else:
    print("Usando archivo ya descargado:", RAW_CSV_PATH)

# =========================
# 4) Cargar y limpieza básica (hotel)
# =========================
df_aloj = pd.read_csv(RAW_CSV_PATH, sep=";", encoding="latin-1")
print(df_aloj.head(3))
print(df_aloj.info())

# Lowercase para texto y headers
df_aloj = df_aloj.apply(lambda x: x.str.lower() if x.dtype == "object" else x)
df_aloj.columns = df_aloj.columns.str.strip().str.lower()

# Renombrar columnas precios con guiones (si vienen así)
df_aloj = df_aloj.rename(columns={
    "precio_check-in_entre_semana": "precio_checkin_entre_semana",
    "precio_check-in_fin_semana": "precio_checkin_fin_semana",
})

# Convertir precios a numérico (miles/decimales)
for col in ["precio_checkin_entre_semana", "precio_checkin_fin_semana"]:
    if col in df_aloj.columns:
        df_aloj[col] = (
            df_aloj[col]
            .astype(str)
            .str.replace(".", "", regex=False)   # separador de miles
            .str.replace(",", ".", regex=False)  # coma decimal -> punto
            .str.strip()
        )
        df_aloj[col] = pd.to_numeric(df_aloj[col], errors="coerce")

# Chequeo duplicados/nulos (fila completa)
print("Duplicados (fila completa):", df_aloj.duplicated().sum())
print("Nulos por columna:\n", df_aloj.isna().sum())

# Normalizar llaves
for c in ["categoria_alojamiento", "ccaa", "provincia"]:
    if c in df_aloj.columns:
        df_aloj[c] = df_aloj[c].apply(normaliza)

# Mapping Ceuta/Melilla (por si aparece)
map_prov = {
    "ciudad autonoma de ceuta": "ceuta",
    "ciudad autónoma de ceuta": "ceuta",
    "ciudad autonoma de melilla": "melilla",
    "ciudad autónoma de melilla": "melilla",
}
if "provincia" in df_aloj.columns:
    df_aloj["provincia"] = df_aloj["provincia"].replace(map_prov)
# Imputación de nulos en precios (fallback)
cprecio = ["precio_checkin_entre_semana", "precio_checkin_fin_semana"]
for col in cprecio:
    if col not in df_aloj.columns:
        df_aloj[col] = pd.NA
df_aloj[cprecio] = df_aloj.groupby(
    ["categoria_alojamiento", "ccaa", "provincia", "mes"]
)[cprecio].transform(lambda x: x.fillna(x.mean()))

df_aloj[cprecio] = df_aloj.groupby(
    ["categoria_alojamiento", "ccaa", "mes"]
)[cprecio].transform(lambda x: x.fillna(x.mean()))

df_aloj[cprecio] = df_aloj.groupby(
    ["categoria_alojamiento", "mes"]
)[cprecio].transform(lambda x: x.fillna(x.mean()))

for col in cprecio:
    df_aloj[col] = df_aloj[col].fillna(df_aloj[col].mean())

print("Nulos en precios tras imputación:\n", df_aloj[cprecio].isna().sum())
# =========================
# 5) Consolidar a PK (sin año) + periodo_antelacion
# =========================
pk = ["mes", "ccaa", "provincia", "categoria_alojamiento"]

# Si no existe periodo_antelacion en el dataset de hotel, créalo como NA
if "periodo_antelacion" not in df_aloj.columns:
    df_aloj["periodo_antelacion"] = pd.NA

# Quitar año para consolidar 2023-2026
df_proj_aloj = df_aloj.drop(columns=["año"], errors="ignore").copy()

# Agregación final por PK
df_proj_avg = (
    df_proj_aloj
    .groupby(pk, as_index=False)
    .agg(
        periodo_antelacion=("periodo_antelacion",
                            lambda s: s.dropna().mode().iloc[0] if not s.dropna().mode().empty else pd.NA),
        precio_checkin_entre_semana=("precio_checkin_entre_semana", "mean"),
        precio_checkin_fin_semana=("precio_checkin_fin_semana", "mean"),
    )
)

# Redondeo
df_proj_avg["precio_checkin_entre_semana"] = df_proj_avg["precio_checkin_entre_semana"].round(2)
df_proj_avg["precio_checkin_fin_semana"] = df_proj_avg["precio_checkin_fin_semana"].round(2)

print("Duplicados PK (post-agg):", df_proj_avg.duplicated(subset=pk).sum())

# =========================
# 6) MERGE: Valoraciones hoteleras (df_hvaloracion)
# =========================
df_hvaloracion = load_csv(str(CLEAN_DIR / "df_hvaloracion.csv"))
df_hvaloracion.columns = (
    df_hvaloracion.columns
    .str.replace("\ufeff", "", regex=False)  # quita BOM
    .str.strip()
    .str.lower()
)
# Normalizar llaves
for c in ["categoria_alojamiento", "ccaa", "provincia"]:
    df_hvaloracion[c] = df_hvaloracion[c].apply(normaliza)
df_hvaloracion["mes"] = pd.to_numeric(df_hvaloracion["mes"], errors="coerce")
print(df_hvaloracion.columns.tolist())
# Asegurar 1 fila por PK
df_hvaloracion = (
    df_hvaloracion
    .groupby(pk, as_index=False)
    .agg(valoraciones=("valoraciones", "mean"))
)

# Merge 1:1
df_proj_avg = df_proj_avg.merge(
    df_hvaloracion[pk + ["valoraciones"]],
    on=pk,
    how="left",
    validate="1:1"
)

print("Nulos en valoraciones (hotel):", df_proj_avg["valoraciones"].isna().sum())

# =========================
# 7) APPEND: Vivienda turística privada con provincia (df_vivienda_con_prov)
# =========================
df_vivienda_con_prov = load_csv(str(CLEAN_DIR / "df_vivienda_con_prov.csv"))
df_vivienda_con_prov.columns = (
    df_vivienda_con_prov.columns
    .str.replace("\ufeff", "", regex=False)  # quita BOM
    .str.strip()
    .str.lower()
)
# Normalizar llaves
for c in ["categoria_alojamiento", "ccaa", "provincia"]:
    df_vivienda_con_prov[c] = df_vivienda_con_prov[c].apply(normaliza)
df_vivienda_con_prov["mes"] = pd.to_numeric(df_vivienda_con_prov["mes"], errors="coerce")
print(df_vivienda_con_prov.columns.tolist())

# Asegurar esquema común
cols_final = [
    "mes",
    "ccaa",
    "provincia",
    "categoria_alojamiento",
    "periodo_antelacion",
    "precio_checkin_entre_semana",
    "precio_checkin_fin_semana",
    "valoraciones",
]

for col in cols_final:
    if col not in df_proj_avg.columns:
        df_proj_avg[col] = pd.NA
    if col not in df_vivienda_con_prov.columns:
        df_vivienda_con_prov[col] = pd.NA

df_proj_avg = df_proj_avg[cols_final].copy()
df_vivienda_con_prov = df_vivienda_con_prov[cols_final].copy()
# Tipos (evita warnings futuros)
for col in ["precio_checkin_entre_semana", "precio_checkin_fin_semana", "valoraciones", "mes"]:
    df_proj_avg[col] = pd.to_numeric(df_proj_avg[col], errors="coerce")
    df_vivienda_con_prov[col] = pd.to_numeric(df_vivienda_con_prov[col], errors="coerce")

for col in ["ccaa", "provincia", "categoria_alojamiento", "periodo_antelacion"]:
    df_proj_avg[col] = df_proj_avg[col].astype("string")
    df_vivienda_con_prov[col] = df_vivienda_con_prov[col].astype("string")

# (Opcional) marcar fuente para trazabilidad
df_proj_avg["fuente"] = "hotel_dataestur"
df_vivienda_con_prov["fuente"] = "vut_privada"

# Concat final
df_final = pd.concat([df_proj_avg, df_vivienda_con_prov], ignore_index=True)
# Validaciones
print("Shape final:", df_final.shape)
print("Duplicados PK final:", df_final.duplicated(subset=pk).sum())
print("Nulos por columna:\n", df_final.isna().sum())

# Columna boolean (True/False) de disponibilidad
df_final["tiene_valoraciones"] = df_final["valoraciones"].notna()

# =========================
# 8) LAT/LON: usar dim_geografia_es_latlon_final (centroide por provincia+ccaa)
# =========================
# Nota: el archivo está en clean, si lo tienes en otro folder ajusta el path.
df_geo = load_csv(
    os.path.join(OUTPUTS_DIR, "division_politica_y_geoespacial.csv")
)


df_geo = df_geo.apply(lambda x: x.str.lower() if x.dtype == "object" else x)
df_geo.columns = df_geo.columns.str.strip().str.lower()

# CCAA == comunidad autonoma
if "comunidad autonoma" in df_geo.columns and "ccaa" not in df_geo.columns:
    df_geo["ccaa"] = df_geo["comunidad autonoma"].apply(normaliza)
else:
    df_geo["ccaa"] = df_geo["ccaa"].apply(normaliza)

df_geo["provincia"] = df_geo["provincia"].apply(normaliza)

# Mapping Ceuta/Melilla por si acaso
df_geo["provincia"] = df_geo["provincia"].replace(map_prov)

map_prov = {
    "ciudad autonoma de ceuta": "ceuta",
    "ciudad autónoma de ceuta": "ceuta",
    "ciudad autonoma de melilla": "melilla",
    "ciudad autónoma de melilla": "melilla",
}

df_final["provincia"] = df_final["provincia"].replace(map_prov)
df_geo["provincia"] = df_geo["provincia"].replace(map_prov)

# Centroide provincial (promedio de lat/lng de municipios)
# En tu archivo las columnas se ven como lat y lng
dim_provincia_latlon = (
    df_geo
    .sort_values(["ccaa", "provincia", "id_ccaa"])  # orden estable
    .groupby(["ccaa", "provincia"], as_index=False)
    .agg(
        cpro=("cpro", "first"),
        id_ccaa=("id_ccaa", "first"),
        latitud=("lat", "mean"),
        longitud=("lng", "mean")
    )
)
#Revision
keys = ["ccaa", "provincia"]

# Asegurar que ambas llaves estén normalizadas (por si acaso)
for c in keys:
    df_final[c] = df_final[c].apply(normaliza)
    dim_provincia_latlon[c] = dim_provincia_latlon[c].apply(normaliza)

# Ver qué filas NO encuentran match
check = df_final.merge(
    dim_provincia_latlon[keys].drop_duplicates(),
    on=keys,
    how="left",
    indicator=True
)

no_match = (
    check[check["_merge"] == "left_only"][keys]
    .drop_duplicates()
    .sort_values(keys)
)

print("Combinaciones (ccaa, provincia) sin match:", len(no_match))
print(no_match.head(50))




# Merge con validate m:1 (muchas filas alojamiento a una fila provincia)
df_final = df_final.merge(
    dim_provincia_latlon,
    on=["ccaa", "provincia"],
    how="left",
    validate="m:1"
)

print("Nulos latitud:", df_final["latitud"].isna().sum())
print("Nulos longitud:", df_final["longitud"].isna().sum())
# Establecer FK:Provincia
df_final = df_final.rename(
    columns={"cpro": "id_provincia"}
)

# Creacion de PK por medio de consecutivo
df_alojamientos = df_final.reset_index(drop=True)
df_alojamientos["id_alojamiento"] = (
    df_alojamientos.index + 1
)

orden_columnas = [
    'id_alojamiento',
    'id_provincia',
    "id_ccaa",
    'mes',
    'ccaa',
    'provincia',
     'categoria_alojamiento',
     'periodo_antelacion',
      'precio_checkin_entre_semana',
      'precio_checkin_fin_semana',
      'valoraciones',
      'tiene_valoraciones',
      'latitud',
      'longitud',
      'fuente'
]

df_alojamientos = df_alojamientos[orden_columnas]
#Revision
print(df_alojamientos.columns.tolist())
print("Shape df_alojamientos:", df_alojamientos.shape)
print("Nulos lat_provincia:", df_alojamientos["latitud"].isna().sum())
print("Duplicados", df_alojamientos.duplicated().sum())

# Validación final de integridad geoespacial
assert df_alojamientos["latitud"].notna().all()
assert df_alojamientos["longitud"].notna().all()
assert df_alojamientos.duplicated(
    subset=["id_provincia", "mes", "categoria_alojamiento"]
).sum() == 0


# =========================
# 8) Guardar outputs finales
# =========================
os.makedirs(OUTPUTS_DIR, exist_ok=True)
out_alojamientos = os.path.join(
    OUTPUTS_DIR,
    "df_alojamientos.csv"
)
df_alojamientos.to_csv(
    out_alojamientos,
    index=False,
    encoding="utf-8-sig"
)
print("📁 Dataset final guardado en outputs:")
print(" -", out_alojamientos)