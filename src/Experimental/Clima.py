# ============================================================
# Clima.py  |  Pipeline clima Open-Meteo (provincia -> mes)
# - Descarga clima histórico por provincia (centroide)
# - Maneja rate limit (429) con retries + backoff
# - Cachea resultados por provincia para no re-llamar la API
# - Agrega a nivel mensual para EDA / Feature Engineering
# ============================================================

import os
import re
import time
import unicodedata
from datetime import date, timedelta
import requests
import pandas as pd
from pathlib import Path

# =========================
# 0) Helpers
# =========================
def load_csv(path: str) -> pd.DataFrame:
    """Carga CSV con detección de separador y fallback de encoding."""
    try:
        return pd.read_csv(path, encoding="utf-8", sep=None, engine="python")
    except UnicodeDecodeError:
        return pd.read_csv(path, encoding="latin1", sep=None, engine="python")


def normaliza(s: str) -> str:
    """Normaliza strings: lower, trim, sin acentos, guiones/espacios consistentes."""
    if pd.isna(s):
        return s
    s = str(s).strip().lower()
    s = unicodedata.normalize("NFKD", s).encode("ascii", "ignore").decode("utf-8")
    s = re.sub(r"\s*-\s*", "-", s)  # " - " -> "-"
    s = re.sub(r"\s+", " ", s)      # espacios múltiples -> 1
    return s


def _pick_col(df: pd.DataFrame, candidates: list[str]) -> str | None:
    """Devuelve la primera columna existente en df de la lista de candidatos."""
    cols = set(df.columns)
    for c in candidates:
        if c in cols:
            return c
    return None


# =========================
# 1) Open-Meteo: request robusto
# =========================
def get_clima(lat, lon, start="2023-01-01", end="2026-12-31",
             max_retries=5, sleep_seconds=2) -> pd.DataFrame:
    """
    Descarga clima diario histórico desde Open-Meteo Archive.
    Nota: archive SOLO trae histórico; se capa end_date a (hoy - 1 día).
    """
    url = "https://archive-api.open-meteo.com/v1/archive"

    hoy = date.today()
    end_cap = min(pd.to_datetime(end).date(), hoy - timedelta(days=1))

    params = {
        "latitude": float(lat),
        "longitude": float(lon),
        "start_date": start,
        "end_date": end_cap.isoformat(),
        "daily": "temperature_2m_mean,precipitation_sum",
        "timezone": "Europe/Madrid"
    }

    for intento in range(max_retries):
        try:
            r = requests.get(url, params=params, timeout=60)

            if r.status_code == 429:
                wait = sleep_seconds * (intento + 1)
                print(f"⏳ Rate limit (429). Reintentando en {wait}s...")
                time.sleep(wait)
                continue

            r.raise_for_status()

            daily = r.json().get("daily", {})
            df = pd.DataFrame(daily)

            # Open-Meteo suele traer "time" como fechas
            if df.empty or "time" not in df.columns:
                return pd.DataFrame()

            return df

        except requests.exceptions.RequestException as e:
            wait = sleep_seconds * (intento + 1)
            print(f"⚠️ Error intento {intento + 1}: {e}")
            time.sleep(wait)

    print("❌ Falló después de varios intentos")
    return pd.DataFrame()


def get_clima_cached(lat, lon, cpro, raw_dir, start="2023-01-01", end="2026-12-31") -> pd.DataFrame:
    """
    Cachea el clima diario por provincia (cpro) en data/raw/clima_cache/
    para no volver a llamar la API.
    """
    cache_dir = os.path.join(raw_dir, "clima_cache")
    os.makedirs(cache_dir, exist_ok=True)

    cpro_2 = str(cpro).zfill(2)
    cache_file = os.path.join(cache_dir, f"clima_prov_{cpro_2}.csv")

    if os.path.exists(cache_file):
        print(f"♻️ Usando cache para provincia cpro={cpro_2}")
        return pd.read_csv(cache_file)

    df = get_clima(lat, lon, start=start, end=end)

    if not df.empty:
        df.to_csv(cache_file, index=False)

    return df



# =========================
# 2) Rutas del proyecto
# =========================
BASE_DIR = Path(__file__).resolve().parents[2]

RAW_DIR = BASE_DIR / "data" / "raw"
INTERIM_DIR = BASE_DIR / "data" / "interim"   # ✅ NUEVO
CLEAN_DIR = BASE_DIR / "data" / "clean"
OUTPUTS_DIR = BASE_DIR / "outputs"
EXPERIMENTAL_DIR = BASE_DIR / "data" / "Experimental"

RAW_DIR.mkdir(parents=True, exist_ok=True)
INTERIM_DIR.mkdir(parents=True, exist_ok=True)  # ✅ NUEVO
CLEAN_DIR.mkdir(parents=True, exist_ok=True)
OUTPUTS_DIR.mkdir(parents=True, exist_ok=True)
EXPERIMENTAL_DIR.mkdir(parents=True, exist_ok=True)

# =========================
# 3) Reconstrucción de geografía (municipio + OSM)
# =========================

muni_path = os.path.join(CLEAN_DIR, "dim_municipio_final.csv")
osm_path = os.path.join(CLEAN_DIR, "dim_geografia_municipio_osm.csv")

df_muni = load_csv(muni_path)
df_osm = load_csv(osm_path)

# --- Limpieza robusta de nombres de columnas (BOM/espacios) ---
df_osm.columns = (
    df_osm.columns
    .astype(str)
    .str.replace("\ufeff", "", regex=False)  # BOM
    .str.strip()
)

print("✅ Columnas OSM (limpias):", list(df_osm.columns))

# Asegurar tipos
df_muni["id_municipio"] = df_muni["id_municipio"].astype(str)
df_osm["id_municipio"] = df_osm["id_municipio"].astype(str)

# Merge administrativo + geográfico
df_geo = df_muni.merge(
    df_osm[["id_municipio", "lat", "lon"]],
    on="id_municipio",
    how="left"
)

# Renombrar para mantener compatibilidad con el resto del pipeline
df_geo = df_geo.rename(columns={
    "lon": "lng",
    "id_provincia": "cpro",
    "provincia_nombre": "provincia"
})

# Formateo
df_geo["cpro"] = df_geo["cpro"].astype(str).str.zfill(2)
df_geo["lat"] = pd.to_numeric(df_geo["lat"], errors="coerce")
df_geo["lng"] = pd.to_numeric(df_geo["lng"], errors="coerce")

# Eliminar registros sin coordenadas
df_geo = df_geo.dropna(subset=["lat", "lng", "cpro", "provincia"]).copy()

print(f"✅ Geografía reconstruida correctamente. Registros: {len(df_geo)}")

# =========================
# 4) Centroide por provincia
# =========================
df_prov = (
    df_geo.groupby(["cpro", "provincia"], as_index=False)
          .agg(lat_prov=("lat", "mean"),
               lon_prov=("lng", "mean"))
)

print(f"✅ Provincias a procesar: {len(df_prov)}")


# =========================
# 5) Descargar clima por provincia (con cache)
# =========================
climas = []

for i, row in df_prov.iterrows():
    cpro = row["cpro"]
    prov = row["provincia"]
    lat = row["lat_prov"]
    lon = row["lon_prov"]

    print(f"📍 Procesando clima: {prov} (cpro={cpro}) [{i+1}/{len(df_prov)}]")

    df_c = get_clima_cached(lat, lon, cpro, RAW_DIR, start="2023-01-01", end="2026-12-31")

    if df_c.empty:
        print(f"⚠️ Sin datos para {prov} (cpro={cpro})")
        continue

    df_c["cpro"] = cpro
    df_c["provincia"] = prov
    df_c["lat_prov"] = lat
    df_c["lon_prov"] = lon

    climas.append(df_c)

    # Pausa suave (si hubo cache es rápido igual; si prefieres, puedes subir a 2-3s)
    time.sleep(1)

if not climas:
    raise RuntimeError("❌ No se descargó clima para ninguna provincia (climas vacío).")

df_clima_prov_daily = pd.concat(climas, ignore_index=True)


# =========================
# 6) Agregación mensual (provincia-mes)
# =========================
df_clima_prov_daily["date"] = pd.to_datetime(df_clima_prov_daily["time"], errors="coerce")
df_clima_prov_daily = df_clima_prov_daily.dropna(subset=["date"]).copy()

df_clima_prov_daily["mes"] = df_clima_prov_daily["date"].dt.to_period("M").astype(str)

# Asegurar numéricos
df_clima_prov_daily["temperature_2m_mean"] = pd.to_numeric(df_clima_prov_daily["temperature_2m_mean"], errors="coerce")
df_clima_prov_daily["precipitation_sum"] = pd.to_numeric(df_clima_prov_daily["precipitation_sum"], errors="coerce")

df_clima_prov_mes = (
    df_clima_prov_daily
    .groupby(["cpro", "provincia", "mes"], as_index=False)
    .agg(
        temp_media_mes=("temperature_2m_mean", "mean"),
        precip_total_mes=("precipitation_sum", "sum")
    )
)

# =========================
# 7) ID ACTIVIDAD (PK)
# =========================
df_clima_prov_mes = df_clima_prov_mes.reset_index(drop=True)
df_clima_prov_mes["id_clima"] = df_clima_prov_mes.index + 1
# =========================
# 7) Revisiones rápidas
# =========================
print("\n=== Preview df_clima_prov_mes ===")
print(df_clima_prov_mes.head())

print("\n=== Info df_clima_prov_mes ===")
print(df_clima_prov_mes.info())

print("\n=== Nulos df_clima_prov_mes ===")
print(df_clima_prov_mes.isna().sum())

print("\n=== Duplicados df_clima_prov_mes ===")
print(df_clima_prov_mes.duplicated(subset=["cpro", "mes"]).sum())

# =========================
# 8) Guardar outputs
# =========================
out_daily = os.path.join(EXPERIMENTAL_DIR, "clima_provincia_daily.csv")
out_month = os.path.join(EXPERIMENTAL_DIR, "clima_provincia_mes.csv")

df_clima_prov_daily.to_csv(out_daily, index=False)
df_clima_prov_mes.to_csv(out_month, index=False)

print(f"\n✅ Guardado: {out_daily}")
print(f"✅ Guardado: {out_month}")

# ============================================================
# NOTA IMPORTANTE:
# - NO hagas self-merge (merge del clima consigo mismo).
# - El merge correcto es con OTRO dataset usando key: ['cpro','mes']
#   Ej: df_otro.merge(df_clima_prov_mes, on=['cpro','mes'], how='left')
# ============================================================
