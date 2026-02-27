import pandas as pd
from pathlib import Path

BASE_PATH = Path("../../data/clean")

df_geo = pd.read_csv(BASE_PATH / "dim_geografia_es_latlon.csv", dtype=str)
df_osm = pd.read_csv(BASE_PATH / "nominatim_cache.csv", dtype=str)
df_geo["geo_source"] = "geonames"
df_geo = df_geo.merge(
    df_osm[["id_municipio", "osm_query"]],
    on="id_municipio",
    how="left"
)
mask_osm = df_geo["lat"].isna() & df_geo["osm_query"].notna()

df_geo.loc[mask_osm, "geo_source"] = "osm"
df_geo = df_geo.drop(columns=["osm_query"], errors="ignore")
OUTPUT_FINAL = BASE_PATH / "dim_geografia_es_latlon_final.csv"
df_geo.to_csv(OUTPUT_FINAL, index=False, encoding="utf-8")
print("Tipo Datos", df_geo.info())
cols_keep = [
    "country_code",
    "pais",
    "id_municipio",
    "cpro",
    "provincia",
    "cmun",
    "id_ccaa",
    "comunidad autonoma",
    "cisla",
    "isla",
    "dc",
    "lat",
    "lng",
    "geo_source"
]
df_geo = df_geo[cols_keep].copy()
print(df_geo.shape)
df_geo.head()
print(df_geo[df_geo["provincia"].str.contains("ceuta", na=False)][["provincia", "lat", "lng"]])
print(
    df_geo[df_geo["provincia"].str.contains("ceuta", na=False)]
    [["pais", "comunidad autonoma", "id_ccaa", "provincia"]]
    .drop_duplicates()
)
print(
    df_geo[df_geo["provincia"].str.contains("ceuta", na=False)]
    [[ "cpro", "cmun", "id_municipio","lat", "lng"]]
    .drop_duplicates()
)


OUTPUTS_PATH = Path("../../outputs")
OUTPUTS_PATH.mkdir(exist_ok=True)

OUTPUT_FINAL = OUTPUTS_PATH / "division_politica_y_geoespacial.csv"

df_geo.to_csv(
    OUTPUT_FINAL,
    index=False,
    encoding="utf-8"
)

print("Dataset final guardado en:", OUTPUT_FINAL)


df_geo["geo_source"].value_counts(normalize=True) * 100