import pandas as pd
from pathlib import Path

# =========================
# 1) RUTAS DEL PROYECTO
# =========================
BASE_DIR = Path(__file__).resolve().parents[2]

RAW_DIR = BASE_DIR / "data" / "raw"
INTERIM_DIR = BASE_DIR / "data" / "interim"
CLEAN_DIR = BASE_DIR / "data" / "clean"
OUTPUTS_DIR = BASE_DIR / "outputs"

RAW_DIR.mkdir(parents=True, exist_ok=True)
INTERIM_DIR.mkdir(parents=True, exist_ok=True)
CLEAN_DIR.mkdir(parents=True, exist_ok=True)
OUTPUTS_DIR.mkdir(parents=True, exist_ok=True)

# Inputs (ya existentes en el proyecto)
INPUT_MUNICIPIO = CLEAN_DIR / "dim_municipio_final.csv"
INPUT_GEO_OSM = CLEAN_DIR / "dim_geografia_municipio_osm.csv"

# Output
OUTPUT_PATH = CLEAN_DIR / "dim_provincia_coordenadas_osm.csv"

# =========================
# 2) CARGAR INPUTS
# =========================
df_mun = pd.read_csv(INPUT_MUNICIPIO, dtype=str)
df_geo = pd.read_csv(INPUT_GEO_OSM, dtype=str)

print("dim_municipio_final:", df_mun.shape)
print("dim_geografia_municipio_osm:", df_geo.shape)

# =========================
# 3) PREPARAR Y UNIR POR id_municipio
# =========================
# Forzar tipos consistentes en la clave
df_mun["id_municipio"] = df_mun["id_municipio"].astype(str)
df_geo["id_municipio"] = df_geo["id_municipio"].astype(str)

# Convertir lat/lon a numérico (vienen como string del CSV)
df_geo["lat"] = pd.to_numeric(df_geo["lat"], errors="coerce")
df_geo["lon"] = pd.to_numeric(df_geo["lon"], errors="coerce")

# Merge: traemos lat/lon a la tabla de municipios
df = df_mun.merge(
    df_geo[["id_municipio", "lat", "lon"]],
    on="id_municipio",
    how="left"
)

print("\nMerge municipio + geo:", df.shape)
print("Municipios sin lat/lon:", df[df["lat"].isna() | df["lon"].isna()].shape[0])

# =========================
# 4) CALCULAR CENTROIDE POR PROVINCIA
# =========================
# Nos quedamos solo con filas con coordenadas válidas
df_validos = df.dropna(subset=["lat", "lon"]).copy()

# Agrupamos por provincia y promediamos lat/lon = centroide geográfico
df_provincias = (
    df_validos
    .groupby(["id_provincia", "provincia_nombre", "id_ccaa", "ccaa_nombre"], as_index=False)
    .agg(
        lat_provincia=("lat", "mean"),
        lon_provincia=("lon", "mean"),
        n_municipios=("id_municipio", "count")
    )
)

# Redondeamos coordenadas a 6 decimales (precisión ~0.11 m, más que suficiente)
df_provincias["lat_provincia"] = df_provincias["lat_provincia"].round(6)
df_provincias["lon_provincia"] = df_provincias["lon_provincia"].round(6)

# Orden de columnas final
cols_final = [
    "id_provincia",
    "provincia_nombre",
    "id_ccaa",
    "ccaa_nombre",
    "lat_provincia",
    "lon_provincia",
    "n_municipios"
]
df_provincias = df_provincias[cols_final].sort_values("id_provincia").reset_index(drop=True)

# =========================
# 5) GUARDAR OUTPUT
# =========================
df_provincias.to_csv(OUTPUT_PATH, index=False, encoding="utf-8-sig")
print("\n✅ Dim provincia coordenadas guardada:", OUTPUT_PATH)

# =========================
# 6) VALIDACIONES (QA)
# =========================
total = df_provincias.shape[0]
con_coords = df_provincias[df_provincias["lat_provincia"].notna() & df_provincias["lon_provincia"].notna()].shape[0]

print("\n--- COBERTURA PROVINCIAS ---")
print(f"Total provincias: {total}")
print(f"Con coordenadas: {con_coords}")
print(f"Cobertura (%): {round(con_coords / total * 100, 2)}")

# QA: coordenadas dentro del rango de España (incluye Canarias)
fuera_rango = df_provincias[
    (df_provincias["lat_provincia"] < 26) | (df_provincias["lat_provincia"] > 45) |
    (df_provincias["lon_provincia"] < -20) | (df_provincias["lon_provincia"] > 6)
]
print("Provincias fuera de rango España (QA):", fuera_rango.shape[0])

print("\nEjemplo:")
print(df_provincias.head(10))