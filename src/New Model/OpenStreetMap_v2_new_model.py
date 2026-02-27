import time
import re
import unicodedata
import requests
import pandas as pd
from pathlib import Path

# =========================
# 1) RUTAS
# =========================
BASE_DIR = Path(__file__).resolve().parents[2]

RAW_DIR = BASE_DIR / "data" / "raw"
INTERIM_DIR = BASE_DIR / "data" / "interim"   # ✅ NUEVO
CLEAN_DIR = BASE_DIR / "data" / "clean"
OUTPUTS_DIR = BASE_DIR / "outputs"

RAW_DIR.mkdir(parents=True, exist_ok=True)
INTERIM_DIR.mkdir(parents=True, exist_ok=True)  # ✅ NUEVO
CLEAN_DIR.mkdir(parents=True, exist_ok=True)
OUTPUTS_DIR.mkdir(parents=True, exist_ok=True)

INPUT_PATH = CLEAN_DIR / "dim_municipio_final.csv"               # ✅ final
CACHE_PATH = OUTPUTS_DIR / "osm_cache_municipios.csv"            # ✅ outputs (cache)
OUTPUT_PATH = CLEAN_DIR / "dim_geografia_municipio_osm.csv"      # ✅ final

# =========================
# 2) CONFIG NOMINATIM (OPENSTREETMAP)
# =========================
NOMINATIM_URL = "https://nominatim.openstreetmap.org/search"
HEADERS = {
    # Pon algo identificable (email real si quieres)
    "User-Agent": "CulturaTrip_TFM/1.0 (contact: andrea@culturatrip.local)"
}

# =========================
# 3) UTILIDADES TEXTO
# =========================
def limpiar(s: str) -> str:
    s = str(s).strip()
    s = re.sub(r"\s+", " ", s)
    return s.rstrip(",")

def sin_tildes(s: str) -> str:
    s = unicodedata.normalize("NFKD", str(s))
    return "".join(c for c in s if not unicodedata.combining(c))

def generar_queries(row) -> list[str]:
    """
    Genera queries de mayor a menor precisión.
    Usa municipio + provincia + ccaa + España.
    """
    nombre = limpiar(row.get("nombre", ""))
    prov = limpiar(row.get("provincia_nombre", row.get("provincia", "")))
    ccaa = limpiar(row.get("ccaa_nombre", ""))

    base = [
        f"{nombre}, {prov}, {ccaa}, España",
        f"{nombre}, {prov}, España",
        f"{nombre}, {ccaa}, España",
        f"{nombre}, España",
        nombre
    ]

    # También probamos sin tildes
    queries = []
    for q in base:
        q1 = limpiar(q)
        q2 = limpiar(sin_tildes(q))
        queries.append(q1)
        queries.append(q2)

    # Deduplicar manteniendo orden
    seen = set()
    out = []
    for q in queries:
        if q and q not in seen:
            out.append(q)
            seen.add(q)
    return out

# =========================
# 4) BUSCAR EN NOMINATIM
# =========================
def buscar_nominatim(q: str, countrycodes: str = "es"):
    params = {
        "q": q,
        "format": "json",
        "limit": 5,
        "addressdetails": 1,
        "countrycodes": countrycodes,  # 'es' para España
    }
    r = requests.get(NOMINATIM_URL, params=params, headers=HEADERS, timeout=30)
    r.raise_for_status()
    data = r.json()

    if not data:
        return None, None, None, None

    # Heurística simple: preferimos tipo city/town/village y con mayor "importance"
    def score(item):
        importance = float(item.get("importance") or 0)
        t = (item.get("type") or "").lower()
        place_rank = int(item.get("place_rank") or 0)

        bonus = 0
        if t in ["city", "town", "village", "hamlet", "municipality"]:
            bonus += 1.0

        return (importance + bonus, place_rank)

    best = sorted(data, key=score, reverse=True)[0]

    lat = best.get("lat")
    lon = best.get("lon")
    osm_id = best.get("osm_id")
    osm_type = best.get("osm_type")

    return lat, lon, osm_id, osm_type

def resolver_row(row, sleep_secs: float = 1.2):
    for q in generar_queries(row):
        lat, lon, osm_id, osm_type = buscar_nominatim(q, countrycodes="es")
        if lat and lon:
            return q, lat, lon, osm_id, osm_type
        time.sleep(sleep_secs)
    return None, None, None, None, None

# =========================
# 5) LEER INPUT + DEDUP
# =========================
df = pd.read_csv(INPUT_PATH, dtype=str)

# Asegurarnos de columnas esperadas
for col in ["id_municipio", "nombre", "provincia_nombre", "ccaa_nombre"]:
    if col not in df.columns:
        print(f"⚠️ No existe columna: {col} (si no aplica, el script seguirá)")

# 1 fila por municipio
df = df.drop_duplicates(subset=["id_municipio"], keep="first").reset_index(drop=True)

df_lookup = df[["id_municipio", "nombre", "provincia_nombre", "ccaa_nombre"]].copy()

print("Lookup municipios:", df_lookup.shape)

# =========================
# 6) CARGAR CACHE
# =========================
if CACHE_PATH.exists():
    cache = pd.read_csv(CACHE_PATH, dtype=str)
else:
    cache = pd.DataFrame(columns=["id_municipio", "lat", "lon", "osm_id", "osm_type", "osm_query_usada", "osm_pass"])

# 1 fila por municipio en cache
if not cache.empty:
    cache = cache.drop_duplicates(subset=["id_municipio"], keep="last").reset_index(drop=True)

cache_ids = set(cache["id_municipio"].astype(str)) if not cache.empty else set()

pendientes = df_lookup[~df_lookup["id_municipio"].astype(str).isin(cache_ids)].copy()
print("Pendientes:", len(pendientes))

# Límite por ejecución (batch)
MAX_REQUESTS = 250
pendientes = pendientes.head(MAX_REQUESTS)
print("Pendientes en este batch:", len(pendientes))

# =========================
# 7) PASS 1 (queries fuertes)
# =========================
nuevos = []
for _, row in pendientes.iterrows():
    # usamos el resolver que prueba varias queries
    q_ok, lat, lon, osm_id, osm_type = resolver_row(row, sleep_secs=1.2)

    nuevos.append({
        "id_municipio": str(row["id_municipio"]),
        "lat": lat,
        "lon": lon,
        "osm_id": osm_id,
        "osm_type": osm_type,
        "osm_query_usada": q_ok,
        "osm_pass": "pass_1"
    })

    time.sleep(1.2)  # respetar rate limit

if nuevos:
    cache = pd.concat([cache, pd.DataFrame(nuevos)], ignore_index=True)

cache = cache.drop_duplicates(subset=["id_municipio"], keep="last").reset_index(drop=True)
cache.to_csv(CACHE_PATH, index=False, encoding="utf-8-sig")
print("✅ Cache guardado:", CACHE_PATH)

# =========================
# 8) MERGE FINAL Y GUARDAR DIM GEO
# =========================
df_final = df.merge(
    cache[["id_municipio", "lat", "lon", "osm_id", "osm_type", "osm_query_usada", "osm_pass"]],
    on="id_municipio",
    how="left"
)

print("Columnas",df_final.head(5))
print("Tipo Datos", df_final.info())

# 1) Definir columnas finales
cols_final = [
    "id_municipio",
    "lat",
    "lon",
    "osm_id",
    "osm_type",
    "osm_query_usada",
    "osm_pass"
]
df_final = df_final[cols_final]
df_final = df_final.drop_duplicates(subset=["id_municipio"], keep="first").reset_index(drop=True)
df_final.to_csv(OUTPUT_PATH, index=False, encoding="utf-8-sig")
print("✅ Dim geo guardada:", OUTPUT_PATH)

# =========================
# 9) VALIDACIONES
# =========================
total = df_final.shape[0]
con_latlon = df_final[df_final["lat"].notna() & df_final["lon"].notna()].shape[0]
sin_latlon = total - con_latlon
cobertura_pct = round((con_latlon / total) * 100, 2)

print("\n--- COBERTURA OSM ---")
print(f"Total municipios: {total}")
print(f"Con lat/lon: {con_latlon}")
print(f"Sin lat/lon: {sin_latlon}")
print(f"Cobertura (%): {cobertura_pct}")

print("\nEjemplo:")
cols_show = [c for c in ["id_municipio", "nombre", "provincia_nombre", "ccaa_nombre", "lat", "lon", "osm_pass"] if c in df_final.columns]
print(df_final[cols_show].head(10))

df_final["lat"] = pd.to_numeric(df_final["lat"], errors="coerce")
df_final["lon"] = pd.to_numeric(df_final["lon"], errors="coerce")

fuera_rango = df_final[(df_final["lat"] < 26) | (df_final["lat"] > 45) | (df_final["lon"] < -20) | (df_final["lon"] > 6)]
print("Registros fuera de rango España (QA):", fuera_rango.shape[0])
