import time
import re
import unicodedata
import requests
import pandas as pd
from pathlib import Path

# =========================
# CONFIG
# =========================
INPUT_PATH = Path("../data/clean/dim_geografia_es_latlon.csv")   # ajusta si tu ruta difiere
OUTPUT_PATH = Path("../data/clean/dim_geografia_es_latlon_osm.csv")
CACHE_OSM_PATH = Path("../data/clean/nominatim_cache.csv")

NOMINATIM_URL = "https://nominatim.openstreetmap.org/search"
HEADERS = {
    # IMPORTANTE: Nominatim requiere User-Agent identificable
    "User-Agent": "CulturaTrip_TFM (academic; contact: morales.vega.andrea@gmail.com)"
}

# =========================
# Helpers texto
# =========================
def limpiar(s: str) -> str:
    if pd.isna(s):
        return ""
    s = str(s).strip()
    s = re.sub(r"\s+", " ", s)
    return s.rstrip(",")

def sin_tildes(s: str) -> str:
    s = unicodedata.normalize("NFKD", str(s))
    return "".join(c for c in s if not unicodedata.combining(c))

def normaliza_prov(p: str) -> str:
    p = limpiar(p).lower()
    # quita formas tipo "valencia/valència"
    p = p.split("/")[0]
    return sin_tildes(p)

# =========================
# Query builder
# =========================
def build_queries(row):
    nombre = limpiar(row.get("nombre", ""))
    prov = limpiar(row.get("provincia", ""))
    isla = limpiar(row.get("isla", ""))

    prov_simple = prov.split("/")[0]  # por si viene provincia/variant

    base = [
        f"{nombre}, {prov_simple}, Spain",
        f"{nombre}, {prov_simple}",
        f"{nombre}, Spain",
        f"{nombre}"
    ]

    # Si tiene isla válida, ayuda mucho para Baleares/Canarias
    if isla and isla.lower() != "no aplica":
        base = [
            f"{nombre}, {isla}, Spain",
            f"{nombre}, {isla}, {prov_simple}, Spain",
            *base
        ]

    # añade variantes sin tildes
    queries = []
    for q in base:
        q1 = limpiar(q)
        q2 = limpiar(sin_tildes(q))
        if q1:
            queries.append(q1)
        if q2:
            queries.append(q2)

    # quitar duplicados manteniendo orden
    seen, final = set(), []
    for q in queries:
        if q not in seen:
            final.append(q)
            seen.add(q)
    return final

# =========================
# Nominatim call (con filtros)
# =========================
def nominatim_search(q: str, countrycodes="es", limit=5):
    params = {
        "q": q,
        "format": "json",
        "addressdetails": 1,
        "limit": limit,
        "countrycodes": countrycodes
    }
    r = requests.get(NOMINATIM_URL, params=params, headers=HEADERS, timeout=30)
    r.raise_for_status()
    return r.json()

def pick_best(results, prov_obj: str = None):
    """
    Elige el mejor candidato:
    1) Filtra tipos de lugar para evitar resultados no-municipio
    2) Si prov_obj existe: intenta que coincida con 'state'/'province'/'county'
    3) Fallback: primer resultado (Nominatim ordena por relevancia)
    """
    if not results:
        return None, None, None

    place_ok = {"city", "town", "village", "municipality", "hamlet", "locality"}

    prov_norm = normaliza_prov(prov_obj) if prov_obj else None

    # 1) intenta con match de provincia + tipo de lugar
    if prov_norm:
        for item in results:
            ptype = (item.get("type") or "").lower()
            if ptype and ptype not in place_ok:
                continue

            addr = item.get("address", {}) or {}
            state = addr.get("state", "") or ""
            province = addr.get("province", "") or ""
            county = addr.get("county", "") or ""

            if (
                normaliza_prov(state) == prov_norm
                or normaliza_prov(province) == prov_norm
                or normaliza_prov(county) == prov_norm
            ):
                return item.get("lat"), item.get("lon"), item.get("display_name")

    # 2) fallback: primer item que sea un tipo de lugar aceptado
    for item in results:
        ptype = (item.get("type") or "").lower()
        if not ptype or ptype in place_ok:
            return item.get("lat"), item.get("lon"), item.get("display_name")

    # 3) último fallback: primer resultado
    best = results[0]
    return best.get("lat"), best.get("lon"), best.get("display_name")

# =========================
# MAIN
# =========================
df = pd.read_csv(INPUT_PATH, dtype=str)

# Asegurar columnas obligatorias
for col in ["id_municipio", "provincia", "nombre"]:
    if col not in df.columns:
        raise ValueError(f"Falta columna obligatoria en {INPUT_PATH}: {col}")

# Aviso (no rompe) si no vienen columnas políticas nuevas
for col in ["id_ccaa", "comunidad autonoma"]:
    if col not in df.columns:
        print(f"Aviso: no viene la columna '{col}' (no rompe el script, pero revisa tu pipeline).")

# Tolerancia a nombre de longitud (lon vs lng)
if "lng" not in df.columns and "lon" in df.columns:
    df.rename(columns={"lon": "lng"}, inplace=True)

# cargar cache OSM
if CACHE_OSM_PATH.exists():
    cache_osm = pd.read_csv(CACHE_OSM_PATH, dtype=str)
else:
    cache_osm = pd.DataFrame(columns=["id_municipio", "osm_query", "lat_osm", "lng_osm", "osm_display_name"])

# asegurar una fila por id_municipio
if not cache_osm.empty:
    cache_osm = cache_osm.drop_duplicates(subset=["id_municipio"], keep="last")

# identificar nulos
mask_nulos = df["lat"].isna() | df["lng"].isna()
df_nulos = df[mask_nulos].copy()

print("Municipios sin lat/lng (antes OSM):", df_nulos.shape[0])

# quitar los que ya están en cache_osm
cache_ids = set(cache_osm["id_municipio"].astype(str)) if not cache_osm.empty else set()
df_nulos = df_nulos[~df_nulos["id_municipio"].astype(str).isin(cache_ids)].copy()

print("Municipios a consultar en Nominatim (nuevos):", df_nulos.shape[0])

nuevos = []

for i, (_, row) in enumerate(df_nulos.iterrows(), start=1):
    prov_obj = row.get("provincia", "")
    ok = False

    for q in build_queries(row):
        try:
            results = nominatim_search(q, countrycodes="es", limit=5)
            lat, lng, disp = pick_best(results, prov_obj=prov_obj)
        except requests.RequestException:
            lat, lng, disp = None, None, None
        finally:
            # Rate limit friendly: 1 request / ~1 segundo
            time.sleep(1.1)

        if lat and lng:
            nuevos.append({
                "id_municipio": str(row["id_municipio"]),
                "osm_query": q,
                "lat_osm": str(lat),
                "lng_osm": str(lng),  # guardamos como lng para coherencia interna
                "osm_display_name": disp
            })
            ok = True
            break

    # pequeña pausa extra entre municipios si NO se resolvió (opcional)
    if not ok:
        time.sleep(0.5)

    if i % 20 == 0:
        print(f"Progreso OSM: {i}/{df_nulos.shape[0]}")

# guardar cache OSM
if nuevos:
    cache_osm = pd.concat([cache_osm, pd.DataFrame(nuevos)], ignore_index=True)
    cache_osm = cache_osm.drop_duplicates(subset=["id_municipio"], keep="last")
    cache_osm.to_csv(CACHE_OSM_PATH, index=False, encoding="utf-8")

print("Cache OSM guardado en:", CACHE_OSM_PATH)

# =========================
# MERGE OSM AL DATASET BASE
# =========================
df = df.merge(
    cache_osm[["id_municipio", "lat_osm", "lng_osm"]],
    on="id_municipio",
    how="left"
)

# =========================
# RELLENAR SOLO DONDE FALTABA
# =========================
df["lat"] = df["lat"].fillna(df["lat_osm"])
df["lng"] = df["lng"].fillna(df["lng_osm"])

# =========================
# LIMPIEZA FINAL DE COLUMNAS
# =========================
df = df.drop(columns=["lat_osm", "lng_osm"], errors="ignore")

# =========================
# 🔑 CONVERSIÓN A FORMATO NUMÉRICO (CLAVE)
# =========================
df["lat"] = pd.to_numeric(df["lat"], errors="coerce")
df["lng"] = pd.to_numeric(df["lng"], errors="coerce")

# =========================
# VALIDACIÓN FINAL
# =========================
print("\n--- VALIDACIÓN FINAL ---")
print(df[["lat", "lng"]].isna().sum())
print("Lat min/max:", df["lat"].min(), df["lat"].max())
print("Lng min/max:", df["lng"].min(), df["lng"].max())

# =========================
# EXPORT FINAL LIMPIO
# =========================
df.to_csv(OUTPUT_PATH, index=False, encoding="utf-8")
print("Dataset final limpio y tipado guardado en:", OUTPUT_PATH)
