import time
import re
import unicodedata
import requests
import pandas as pd
from pathlib import Path

# =========================
# 1) RUTAS
# =========================
BASE_DIR = Path(__file__).resolve().parents[1]
CLEAN_DIR = BASE_DIR / "data" / "clean"

INPUT_PATH = CLEAN_DIR / "ciudades_espana.csv"
CACHE_PATH = CLEAN_DIR / "geonames_cache.csv"
OUTPUT_PATH = CLEAN_DIR / "dim_geografia_es_latlon.csv"

# =========================
# 2) CONFIG GEONAMES (HTTP para evitar SSL)
# =========================
GEONAMES_URL = "http://api.geonames.org/searchJSON"
USERNAME = "andreamoralesvega"

# =========================
# 3) UTILIDADES DE TEXTO (LIMPIEZA + SIN TILDES)
# =========================
def limpiar(s: str) -> str:
    s = str(s).strip()
    s = re.sub(r"\s+", " ", s)   # colapsa espacios
    return s.rstrip(",")         # quita coma final

def sin_tildes(s: str) -> str:
    s = unicodedata.normalize("NFKD", s)
    return "".join(c for c in s if not unicodedata.combining(c))

# =========================
# 4) FUNCIÓN GEONAMES (MULTI-RESULTADOS + MEJOR CANDIDATO)
# =========================
def buscar_geonames_multi(q: str, country: str = "ES", maxRows: int = 5):
    params = {
        "q": q,
        "country": country,
        "featureClass": "P",
        "maxRows": maxRows,
        "orderby": "relevance",
        "username": USERNAME
    }

    r = requests.get(GEONAMES_URL, params=params, timeout=30)
    r.raise_for_status()
    data = r.json()

    # Si devuelve error en JSON (ej: límite)
    if "status" in data:
        return None, None, None

    results = data.get("geonames", [])
    if not results:
        return None, None, None

    # Elegimos "mejor" por población (si existe)
    best = sorted(results, key=lambda x: int(x.get("population") or 0), reverse=True)[0]
    return best.get("lat"), best.get("lng"), str(best.get("geonameId"))

# =========================
# 5) GENERADOR DE QUERIES (OPCIÓN 1)
#    Usa tu dataset: nombre, provincia, country_code
#    (Sin usar CCAA porque en tu screenshot no aparece columna de nombre CCAA)
# =========================
def generar_queries(row) -> list[str]:
    nombre = limpiar(row["nombre"])
    prov = limpiar(row["provincia"])
    country = row.get("country_code", "ES")

    base = [
        f"{nombre}, {prov}",
        f"{nombre}, {prov}, Spain",
        f"{nombre}, Spain",
        nombre
    ]

    queries = []
    for q in base:
        q1 = limpiar(q)
        q2 = limpiar(sin_tildes(q))
        queries.append(q1)
        queries.append(q2)

    # Quitar duplicados manteniendo orden
    seen = set()
    final = []
    for q in queries:
        if q and q not in seen:
            final.append(q)
            seen.add(q)

    return final

# =========================
# 6) RESOLVER MUNICIPIO (CASCADE)
# =========================
def resolver_municipio(row, sleep_secs: float = 2):
    country = row.get("country_code", "ES")

    for q in generar_queries(row):
        lat, lng, gid = buscar_geonames_multi(q, country=country, maxRows=5)
        if lat and lng:
            return q, lat, lng, gid
        time.sleep(sleep_secs)

    return None, None, None, None

# =========================
# 7) LEER INPUT Y LIMPIAR DUPLICADOS
# =========================
df = pd.read_csv(INPUT_PATH, dtype=str)
df["id_ccaa"] = df["id_ccaa"].astype(str).str.strip()

# Priorizar registros con isla válida (distinto de "No aplica")
df["__isla_valida__"] = (df["isla"].fillna("").str.strip().str.lower() != "no aplica").astype(int)

# Ordena para que por cada id_municipio quede primero el que tiene isla
df = df.sort_values(["id_municipio", "__isla_valida__"], ascending=[True, False])

# Ahora sí: deja 1 fila por id_municipio (la “mejor”)
df = df.drop_duplicates(subset=["id_municipio"], keep="first").reset_index(drop=True)

df = df.drop(columns=["__isla_valida__"])

# Crear q_geonames base (tu estrategia original)
df["q_geonames"] = (df["nombre"].astype(str) + ", " + df["provincia"].astype(str)).map(limpiar)

# Lookup único
df_lookup = df[["id_municipio", "q_geonames", "country_code", "nombre", "provincia","id_ccaa"]].drop_duplicates()

print("DIM (unica por id_municipio):", df.shape)
print("Lookup (unique):", df_lookup.shape)

# =========================
# 8) LEER CACHE Y LIMPIAR DUPLICADOS
# =========================
if CACHE_PATH.exists():
    cache = pd.read_csv(CACHE_PATH, dtype=str)
else:
    cache = pd.DataFrame(columns=["id_municipio", "q_geonames", "lat", "lng", "geonameId", "geo_query_usada", "geo_pass"])

# =========================
# FIX: asegurar columnas nuevas en cache existente
# =========================
for col in ["geo_query_usada", "geo_pass"]:
    if col not in cache.columns:
        cache[col] = pd.NA

# limpiar formato q_geonames en cache también
if "q_geonames" in cache.columns:
    cache["q_geonames"] = cache["q_geonames"].astype(str).map(limpiar)

# dejar 1 fila por id_municipio (nos quedamos con el último)
if not cache.empty:
    cache = cache.drop_duplicates(subset=["id_municipio"], keep="last").reset_index(drop=True)

print("Cache (deduplicado):", cache.shape)

# =========================
# 9) PRIMERA PASADA: PENDIENTES (SIN REPETIR)
# =========================
cache_ids = set(cache["id_municipio"].astype(str)) if not cache.empty else set()
pendientes = df_lookup[~df_lookup["id_municipio"].astype(str).isin(cache_ids)].copy()

print("Pendientes totales (1ra pasada):", len(pendientes))

# LÍMITE POR EJECUCIÓN (BATCH)
MAX_REQUESTS = 300
pendientes = pendientes.head(MAX_REQUESTS)
print("Pendientes en este batch (1ra pasada):", len(pendientes))

nuevos_1 = []
for _, row in pendientes.iterrows():
    # 1ra pasada: usa el q_geonames "base"
    lat, lng, gid = buscar_geonames_multi(row["q_geonames"], country=row["country_code"], maxRows=1)

    nuevos_1.append({
        "id_municipio": str(row["id_municipio"]),
        "q_geonames": row["q_geonames"],
        "lat": lat,
        "lng": lng,
        "geonameId": gid,
        "geo_query_usada": row["q_geonames"],
        "geo_pass": "pass_1"
    })

    time.sleep(1)  # evita 429

# Actualizar cache con 1ra pasada
if nuevos_1:
    cache = pd.concat([cache, pd.DataFrame(nuevos_1)], ignore_index=True)

cache = cache.drop_duplicates(subset=["id_municipio"], keep="last").reset_index(drop=True)
cache.to_csv(CACHE_PATH, index=False, encoding="utf-8")
print("Cache guardado tras pass_1:", CACHE_PATH)

# =========================
# 10) MERGE INTERMEDIO PARA VER NULOS
# =========================
df_inter = df.merge(
    cache[["id_municipio", "lat", "lng", "geonameId", "geo_query_usada", "geo_pass"]],
    on="id_municipio",
    how="left"
)
df_inter = df_inter.drop_duplicates(subset=["id_municipio"], keep="first").reset_index(drop=True)

nulos_antes = df_inter["lat"].isna().sum()
print("Nulos lat tras pass_1:", nulos_antes)

# =========================
# 11) SEGUNDA PASADA (OPCIÓN 1): SOLO LOS QUE SIGUEN NULOS
# =========================
fallidos = df_inter[df_inter["lat"].isna() | df_inter["lng"].isna()].copy()
print("Municipios a reintentar (pass_2):", fallidos.shape[0])

# Puedes limitar también esta segunda pasada si quieres
MAX_PASS2 = 150
fallidos = fallidos.head(MAX_PASS2)
print("Municipios en este batch (pass_2):", fallidos.shape[0])

nuevos_2 = []
for _, row in fallidos.iterrows():
    q_ok, lat, lng, gid = resolver_municipio(row, sleep_secs=1.5)

    nuevos_2.append({
        "id_municipio": str(row["id_municipio"]),
        "q_geonames": row.get("q_geonames", ""),
        "lat": lat,
        "lng": lng,
        "geonameId": gid,
        "geo_query_usada": q_ok,
        "geo_pass": "pass_2"
    })

# Actualizar cache con 2da pasada
if nuevos_2:
    cache = pd.concat([cache, pd.DataFrame(nuevos_2)], ignore_index=True)

cache = cache.drop_duplicates(subset=["id_municipio"], keep="last").reset_index(drop=True)
cache.to_csv(CACHE_PATH, index=False, encoding="utf-8")
print("Cache guardado tras pass_2:", CACHE_PATH)

# =========================
# 12) MERGE FINAL Y GUARDAR DIM CON LAT/LON
# =========================
df_final = df.merge(
    cache[["id_municipio", "lat", "lng", "geonameId", "geo_query_usada", "geo_pass"]],
    on="id_municipio",
    how="left"
)

df_final = df_final.drop_duplicates(subset=["id_municipio"], keep="first").reset_index(drop=True)
df_final.to_csv(OUTPUT_PATH, index=False, encoding="utf-8")
print("DIM final guardada:", OUTPUT_PATH)

# =========================
# 13) VALIDACIONES
# =========================
print("\n--- VALIDACIONES ---")
print("Filas base:", df.shape[0])
print("Filas final:", df_final.shape[0])
print("Duplicados id_municipio:", df_final["id_municipio"].duplicated().sum())

print("Lat nulos:", df_final["lat"].isna().sum())
print("Lng nulos:", df_final["lng"].isna().sum())

print(df_final[["id_municipio", "nombre", "provincia", "lat", "lng", "geo_pass"]].head(10))

# Cobertura geoespacial
total = df_final.shape[0]
con_latlng = df_final[df_final["lat"].notna() & df_final["lng"].notna()].shape[0]
sin_latlng = total - con_latlng
cobertura_pct = round((con_latlng / total) * 100, 2)

print("\n--- COBERTURA GEOESPACIAL ---")
print(f"Total municipios: {total}")
print(f"Con lat/lng: {con_latlng}")
print(f"Sin lat/lng: {sin_latlng}")
print(f"Cobertura (%): {cobertura_pct}")

print("\n--- NULOS POR COLUMNA ---")
print(df_final.isna().sum())


