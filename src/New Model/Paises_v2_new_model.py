import os
import pandas as pd
import numpy as np
import unicodedata
import re
import requests
from pathlib import Path

# =========================
# 0) HELPERS
# =========================
def normaliza(s: str) -> str:
    """lower + trim + sin acentos + espacios limpios"""
    if pd.isna(s) or str(s).strip() == "":
        return ""
    s = str(s).strip().lower()
    s = unicodedata.normalize("NFKD", s).encode("ascii", "ignore").decode("utf-8")
    s = re.sub(r"\s+", " ", s)
    return s

def load_csv(path: str) -> pd.DataFrame:
    """Lee CSV evitando que códigos como 'NA' se conviertan en NaN."""
    try:
        return pd.read_csv(
            path,
            encoding="utf-8",
            sep=None,
            engine="python",
            keep_default_na=False,
            na_filter=False,
            dtype=str
        )
    except UnicodeDecodeError:
        return pd.read_csv(
            path,
            encoding="latin1",
            sep=None,
            engine="python",
            keep_default_na=False,
            na_filter=False,
            dtype=str
        )
# =========================
# 1) RUTAS
# =========================
BASE_DIR = Path(__file__).resolve().parents[2]
RAW_DIR = BASE_DIR / "data" / "raw"
CLEAN_DIR = BASE_DIR / "data" / "clean"
OUTPUTS_DIR = BASE_DIR / "outputs"

RAW_DIR.mkdir(parents=True, exist_ok=True)
CLEAN_DIR.mkdir(parents=True, exist_ok=True)
OUTPUTS_DIR.mkdir(parents=True, exist_ok=True)

path_in = os.path.join(RAW_DIR, "Paises.csv")
path_out = os.path.join(CLEAN_DIR, "dim_pais.csv")

# =========================
# 2) CARGAR
# =========================
df = load_csv(path_in)
print("Columnas detectadas:", df.columns.tolist())
print(df.head())

# =========================
# 3) NORMALIZAR COLUMNAS
# =========================
df.columns = df.columns.str.strip().str.lower()

rename_map = {}
if "name" in df.columns:
    rename_map["name"] = "pais"
if "code" in df.columns:
    rename_map["code"] = "id_pais"

df = df.rename(columns=rename_map)

if "pais" not in df.columns or "id_pais" not in df.columns:
    raise ValueError("No encuentro columnas 'pais' e 'id_pais' (code). Revisa nombres reales del CSV.")

# =========================
# 4) LIMPIEZA DE DATOS
# =========================
df["pais"] = df["pais"].apply(normaliza)
df["id_pais"] = df["id_pais"].astype(str).str.strip().str.upper()
# Normalizar id_pais como string (y arreglar el caso NA si hubiese sido mal interpretado)
df["id_pais"] = df["id_pais"].astype(str).str.strip().str.upper()
# Si por alguna razón quedó 'NAN' como texto, convertirlo a vacío y luego filtrar
df.loc[df["id_pais"].isin(["NAN", "NONE"]), "id_pais"] = ""

# Reemplazos (mantenible) - (lo dejo igual que tuyo)
reemplazos = {
    "spain": "españa",
    "saudi arabia": "arabia saudita",
    "lithuania": "lituania",
    "latvia": "letonia",
    "cyprus": "chipre",
    "slovakia": "republica eslovaca",
    "romania": "rumania",
    "mauritius": "mauricio",
    "slovenia": "eslovenia",
    "ethiopia": "etiopia",
    "bahrain": "bahrein",
    "germany": "alemania",
    "brazil": "brasil",
    "belgium": "belgica",
    "italy": "italia",
    "ireland": "irlanda",
    "japan": "japon",
    "norway": "noruega",
    "moldova, republic of": "moldavia",
    "bolivia, plurinational state of": "bolivia",
    "bonaire, sint eustatius and saba": "bonaire",
    "bosnia and herzegovina": "bosnia y herzegovina",
    "algeria": "argelia",
    "cape verde": "cabo verde",
    "cayman islands": "islas caiman",
    "croatia": "croacia",
    "czech republic": "republica checa",
    "denmark": "dinamarca",
    "egypt": "egipto",
    "france": "francia",
    "finland": "finlandia",
    "greece": "grecia",
    "equatorial guinea": "guinea ecuatorial",
    "hungary": "hungria",
    "iran, islamic republic of": "iran",
    "iceland": "islandia",
    "jordan": "jordania",
    "korea, republic of": "corea",
    "lebanon": "libano",
    "luxembourg": "luxemburgo",
    "macedonia, the former yugoslav republic of": "macedonia del norte",
    "morocco": "marruecos",
    "netherlands": "paises bajos",
    "new zealand": "nueva zelanda",
    "poland": "polonia",
    "singapore": "singapur",
    "sweden": "suecia",
    "switzerland": "suiza",
    "tanzania, united republic of": "tanzania",
    "thailand": "tailandia",
    "trinidad and tobago": "trinidad y tobago",
    "tunisia": "tunez",
    "turkey": "turquia",
    "ukraine": "ucrania",
    "united arab emirates": "emiratos arabes unidos",
    "united kingdom": "reino unido",
    "united states": "estados unidos de america",
    "venezuela, bolivarian republic of": "venezuela",
    "dominican republic": "republica dominicana",
}
df["pais"] = df["pais"].replace(reemplazos)

df = df[df["id_pais"] != ""].copy()
df = df.drop_duplicates(subset=["id_pais"]).reset_index(drop=True)
print(df[df["pais"].eq("namibia")][["pais", "id_pais"]])

# =========================
# 5) GEO NAMES: LAT/LON POR PAÍS (✅ 1 sola llamada + centroide)
# =========================
USERNAME = "andreamoralesvega"
GEONAMES_HTTPS = "https://api.geonames.org/countryInfoJSON"
GEONAMES_HTTP  = "http://api.geonames.org/countryInfoJSON"

def descargar_countryinfo():
    """Descarga 1 vez. Si HTTPS falla (SSL), usa HTTP."""
    params = {"username": USERNAME}

    try:
        r = requests.get(GEONAMES_HTTPS, params=params, timeout=60)
        r.raise_for_status()
        return r.json()
    except Exception as e:
        print("⚠️ HTTPS falló (SSL):", e)

    r = requests.get(GEONAMES_HTTP, params=params, timeout=60)
    r.raise_for_status()
    return r.json()

def to_float(x):
    try:
        return float(str(x).strip())
    except:
        return np.nan

print("\n📡 Descargando countryInfoJSON (GeoNames) 1 sola vez...")
data = descargar_countryinfo()
geos = data.get("geonames", [])
print("✅ Registros recibidos de GeoNames:", len(geos))

# (Opcional) mirar qué columnas trae GeoNames en un registro
if len(geos) > 0:
    print("Ejemplo de llaves GeoNames:", list(geos[0].keys())[:25])

# Crear mapa ISO2 -> (lat, lon) usando centroide del bounding box
geo_map = {}
for g in geos:
    iso2 = str(g.get("countryCode", "")).strip().upper()
    if not iso2:
        continue

    north = to_float(g.get("north"))
    south = to_float(g.get("south"))
    east  = to_float(g.get("east"))
    west  = to_float(g.get("west"))

    # Centroide simple
    if pd.notna(north) and pd.notna(south) and pd.notna(east) and pd.notna(west):
        lat = (north + south) / 2
        lon = (east + west) / 2
    else:
        lat, lon = np.nan, np.nan

    geo_map[iso2] = (lat, lon)

# Asegurar ISO2 limpio
df["id_pais"] = df["id_pais"].astype(str).str.strip().str.upper()

# Mapear
df["lat"] = df["id_pais"].map(lambda x: geo_map.get(x, (np.nan, np.nan))[0])
df["lon"] = df["id_pais"].map(lambda x: geo_map.get(x, (np.nan, np.nan))[1])

# QA: cuáles quedaron sin match
qa_no_match = df[df["lat"].isna() | df["lon"].isna()][["id_pais", "pais"]].copy()

print("\n--- QA GEO ---")
print("Total países:", df.shape[0])
print("Nulos lat:", df["lat"].isna().sum(), "| Nulos lon:", df["lon"].isna().sum())

if not qa_no_match.empty:
    qa_path = OUTPUTS_DIR / "qa_dim_pais_latlon_no_match.csv"
    qa_no_match.to_csv(qa_path, index=False, encoding="utf-8-sig")
    print("📝 QA guardado:", qa_path)
    print(qa_no_match.head(30))

# =========================
# 6) GUARDAR DIM PAÍS
# =========================
df.to_csv(path_out, index=False, encoding="utf-8-sig")
print("\n✅ dim_pais guardado en:", path_out)
print("Shape final:", df.shape)
