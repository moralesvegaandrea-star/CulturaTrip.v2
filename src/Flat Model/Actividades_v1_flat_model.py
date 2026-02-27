import os
import pandas as pd
import numpy as np
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

def normaliza_df_texto(df: pd.DataFrame) -> pd.DataFrame:
    """Normaliza nombres de columnas + strings (solo columnas tipo object)."""
    df = df.copy()
    df.columns = (
        df.columns.astype(str)
        .str.strip()
        .str.lower()
    )
    for c in df.columns:
        if df[c].dtype == "object":
            df[c] = df[c].apply(normaliza)
    return df

def pick_col(cols, candidates):
    """Busca una columna por match exacto o por 'contiene'."""
    # match exacto
    for c in candidates:
        if c in cols:
            return c
    # match por contiene
    for col in cols:
        for cand in candidates:
            if cand in col:
                return col
    return None

# =========================
# 1) Rutas de proyecto
# =========================
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
CLEAN_DIR = os.path.join(BASE_DIR, "data", "clean")
OUTPUTS_DIR = os.path.join(BASE_DIR, "outputs")


OCIO_PATH = os.path.join(CLEAN_DIR, "df_ocio_agg.csv")
RECURSOS_PATH = os.path.join(CLEAN_DIR, "recursos_turisticos.csv")

# ✅ Ajusta si tu archivo se llama distinto
DIM_GEO_PATH = os.path.join(OUTPUTS_DIR, "division_politica_y_geoespacial.csv")

# =========================
# 2) Cargar datasets
# =========================
df_ocio_agg = pd.read_csv(OCIO_PATH, encoding="utf-8-sig")
df_atracciones_agg = pd.read_csv(RECURSOS_PATH, encoding="utf-8-sig")
df_geo = pd.read_csv(DIM_GEO_PATH, encoding="utf-8-sig")

# =========================
# 3) Normalizar texto / columnas
# =========================
df_ocio_agg = normaliza_df_texto(df_ocio_agg)
df_atracciones_agg = normaliza_df_texto(df_atracciones_agg)
df_geo = normaliza_df_texto(df_geo)

# Asegurar consistencia del nombre de CCAA
if "ccaa" in df_ocio_agg.columns and "comunidad autonoma" not in df_ocio_agg.columns:
    df_ocio_agg = df_ocio_agg.rename(columns={"ccaa": "comunidad autonoma"})

if "ccaa" in df_atracciones_agg.columns and "comunidad autonoma" not in df_atracciones_agg.columns:
    df_atracciones_agg = df_atracciones_agg.rename(columns={"ccaa": "comunidad autonoma"})

if "ccaa" in df_geo.columns and "comunidad autonoma" not in df_geo.columns:
    df_geo = df_geo.rename(columns={"ccaa": "comunidad autonoma"})

# =========================
# 3.1) Detectar/renombrar lat/lng en df_geo (CLAVE)
# =========================
cols_geo = df_geo.columns.tolist()

lat_candidates = ["lat", "latitude", "latitud"]
lng_candidates = ["lng", "lon", "long", "longitude", "longitud"]

lat_col = pick_col(cols_geo, lat_candidates)
lng_col = pick_col(cols_geo, lng_candidates)

print("🔎 Columnas df_geo:", cols_geo)
print("✅ Lat detectada:", lat_col)
print("✅ Lon/Lng detectada:", lng_col)

if lat_col is None or lng_col is None:
    raise KeyError(
        "No se detectaron columnas de latitud/longitud en dim_geografia_es_latlon_final.\n"
        f"Columnas disponibles: {cols_geo}"
    )

# Renombrar a estándar: lat / lng
df_geo = df_geo.rename(columns={lat_col: "lat", lng_col: "lng"})

# =========================
# 4) Validaciones mínimas de columnas
# =========================
required_ocio = {"categoria", "provincia", "comunidad autonoma"}
required_attr = {"categoria", "provincia"}
required_geo = {"provincia", "comunidad autonoma", "lat", "lng"}

missing_ocio = required_ocio - set(df_ocio_agg.columns)
missing_attr = required_attr - set(df_atracciones_agg.columns)
missing_geo = required_geo - set(df_geo.columns)

if missing_ocio:
    raise KeyError(f"Faltan columnas en df_ocio_agg: {missing_ocio}")
if missing_attr:
    raise KeyError(f"Faltan columnas en df_atracciones_agg: {missing_attr}")
if missing_geo:
    raise KeyError(
        f"Faltan columnas en dim_geografia_es_latlon_final: {missing_geo}\n"
        f"Columnas disponibles: {df_geo.columns.tolist()}"
    )


# =========================
# 5) Merge: ocio + valoraciones/opiniones
# =========================
cols_attr = [
    "categoria",
    "provincia",
    "valoracion_por_categoria_promedio",
    "valoracion_general_promedio",
    "total_opiniones_categoria_promedio"
]

missing_cols_attr = set(cols_attr) - set(df_atracciones_agg.columns)
if missing_cols_attr:
    raise KeyError(
        "Faltan columnas de valoraciones en df_atracciones_agg: "
        f"{missing_cols_attr}\n"
        f"Columnas disponibles: {df_atracciones_agg.columns.tolist()}"
    )

df_actividades = df_ocio_agg.merge(
    df_atracciones_agg[cols_attr],
    on=["categoria", "provincia"],
    how="left"
)

df_actividades["hay_valoracion"] = df_actividades["valoracion_general_promedio"].notna()

print("✅ Merge ocio + valoraciones listo")
print("Shape df_actividades:", df_actividades.shape)
print("Nulos (valoracion_general_promedio):", df_actividades["valoracion_general_promedio"].isna().sum())
print(df_actividades[df_actividades["provincia"] == "ceuta"][["provincia", "comunidad autonoma"]].drop_duplicates())
print(df_geo[df_geo["comunidad autonoma"] == "ceuta"][["provincia", "cpro", "lat", "lng"]].head(10))

# =========================
# 3.2) Fix nombres de provincias especiales (Ceuta/Melilla)
# =========================
map_prov_geo = {
    "ciudad autonoma de ceuta": "ceuta",
    "ciudad autónoma de ceuta": "ceuta",
    "ciudad autonoma de melilla": "melilla",
    "ciudad autónoma de melilla": "melilla",
}

# Normaliza 'provincia' en df_geo para que matchee con df_actividades
df_geo["provincia"] = df_geo["provincia"].replace(map_prov_geo)

# (Opcional, pero recomendable) también en df_actividades por seguridad
df_actividades["provincia"] = df_actividades["provincia"].replace(map_prov_geo)




# =========================
# 6) Centroides provinciales (ciudad -> provincia)
# =========================
df_geo["lat"] = pd.to_numeric(df_geo["lat"], errors="coerce")
df_geo["lng"] = pd.to_numeric(df_geo["lng"], errors="coerce")

print("Columnas en df_geo:")
print(df_geo.columns.tolist())

df_centroides_prov = (
    df_geo
    .dropna(subset=["lat", "lng", "provincia", "comunidad autonoma", "cpro","id_ccaa"])
    .groupby(["provincia", "comunidad autonoma", "cpro","id_ccaa"], as_index=False)
    .agg(
        lat_provincia=("lat", "mean"),
        lng_provincia=("lng", "mean")
    )
)

df_centroides_prov["lat_provincia"] = df_centroides_prov["lat_provincia"].round(6)
df_centroides_prov["lng_provincia"] = df_centroides_prov["lng_provincia"].round(6)

print("✅ Centroides provinciales listos")
print("Shape centroides:", df_centroides_prov.shape)

# =========================
# 7) Merge final: actividades + centroides
# =========================
df_actividades_geo = df_actividades.merge(
    df_centroides_prov[
        [
            "cpro",
            "id_ccaa",
            "provincia",
            "comunidad autonoma",
            "lat_provincia",
            "lng_provincia"
        ]
    ],
    on=["provincia", "comunidad autonoma"],
    how="left"
)
# Establecer FK:Provincia
df_actividades_geo = df_actividades_geo.rename(
    columns={"cpro": "id_provincia"}
)
# Establecer FK:Provincia
df_actividades_geo = df_actividades_geo.rename(
    columns={"cmun": "id_cmun"}
)
# Creacion de PK por medio de consecutivo
df_actividades_geo = df_actividades_geo.reset_index(drop=True)
df_actividades_geo["id_actividad"] = (
    df_actividades_geo.index + 1
)
print("✅ Merge final con lat/lng listo")
print("Shape df_actividades_geo:", df_actividades_geo.shape)
print("Nulos lat_provincia:", df_actividades_geo["lat_provincia"].isna().sum())
print("Columnas finales df_actividades_geo:")
print(df_actividades_geo.columns.tolist())

orden_columnas = [
    'id_actividad',
    'id_provincia',
      "id_ccaa",
    'comunidad autonoma',
    'provincia',
    'mes',
    'categoria',
    'producto',
    'subcategoria',
    'gasto_total_promedio',
    'precio_medio_entrada_promedio',
    'valoracion_por_categoria_promedio',
    'valoracion_general_promedio',
    'total_opiniones_categoria_promedio',
    'hay_valoracion',
    'lat_provincia',
    'lng_provincia',
]

df_actividades_geo = df_actividades_geo[orden_columnas]

print(df_actividades_geo.columns.tolist())
print("Shape df_actividades_geo:", df_actividades_geo.shape)
print("Nulos lat_provincia:", df_actividades_geo["lat_provincia"].isna().sum())
print("Duplicados", df_actividades_geo.duplicated().sum())
#Revision de nulos
nulos_lat = df_actividades_geo[df_actividades_geo["lat_provincia"].isna()]
print("Total nulos lat_provincia:", nulos_lat.shape[0])
print(nulos_lat.head())
#revision de combinaciones
sin_match_geo = (
    nulos_lat[["provincia", "comunidad autonoma", "id_provincia"]]
    .drop_duplicates()
    .sort_values(["comunidad autonoma", "provincia"])
)

print("Combinaciones únicas sin lat/lng:", sin_match_geo.shape[0])
sin_match_geo

# Provincias+CCAA esperadas en actividades
keys_act = df_actividades_geo[["provincia", "comunidad autonoma"]].drop_duplicates()

# Provincias+CCAA disponibles en centroides
keys_geo = df_centroides_prov[["provincia", "comunidad autonoma"]].drop_duplicates()

# Qué llaves están en actividades pero no en centroides
keys_no_match = keys_act.merge(keys_geo, on=["provincia", "comunidad autonoma"], how="left", indicator=True)
keys_no_match = keys_no_match[keys_no_match["_merge"] == "left_only"].drop(columns="_merge")

print("Llaves sin match:", keys_no_match.shape[0])
keys_no_match.sort_values(["comunidad autonoma", "provincia"]).head(50)

# Ver valores sospechosos (muestra con repr para ver espacios)
for _, r in sin_match_geo.head(30).iterrows():
    print(repr(r["comunidad autonoma"]), "|", repr(r["provincia"]))
# Ver en df_geo si existen esas combinaciones y si tienen lat/lng
check_geo = sin_match_geo.merge(
    df_geo[["provincia", "comunidad autonoma", "lat", "lng"]],
    on=["provincia", "comunidad autonoma"],
    how="left"
)

print(check_geo.isna().sum())
check_geo.head(30)




# =========================
# 8) Guardar outputs finales
# =========================
os.makedirs(OUTPUTS_DIR, exist_ok=True)
out_actividades_geo = os.path.join(
    OUTPUTS_DIR,
    "df_actividades_geo.csv"
)
df_actividades_geo.to_csv(
    out_actividades_geo,
    index=False,
    encoding="utf-8-sig"
)
print("📁 Dataset final guardado en outputs:")
print(" -", out_actividades_geo)

