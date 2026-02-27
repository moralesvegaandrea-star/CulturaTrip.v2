import pandas as pd
import os
import numpy as np
import unicodedata
import re
import requests
import matplotlib.pyplot as plt
from pathlib import Path

def load_csv(path):
        try:
            return pd.read_csv(
                path,
                encoding="utf-8",
                sep=None,  # 🔑 detección automática
                engine="python"  # 🔑 necesario para sep=None
            )
        except UnicodeDecodeError:
            return pd.read_csv(
                path,
                encoding="latin1",
                sep=None,
                engine="python"
            )
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

BASE_DIR = Path(__file__).resolve().parents[2]

RAW_DIR = BASE_DIR / "data" / "raw"
CLEAN_DIR = BASE_DIR / "data" / "clean"

RAW_DIR.mkdir(parents=True, exist_ok=True)
CLEAN_DIR.mkdir(parents=True, exist_ok=True)

def load_csv(path):
    try:
        return pd.read_csv(
            path,
            encoding="utf-8",
            sep=None,
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



df_paises= load_csv(os.path.join(RAW_DIR, "Paises.csv",))

# Revision Dataset
df_paises.head(5)
print("Columnas",df_paises.head(20))
df_paises.info()
print("Tipo Datos", df_paises.info())
df_paises.isna().sum()
print("Nulos",df_paises.isna().sum())
df_paises.duplicated().sum()
print("Duplicados", df_paises.duplicated().sum())
# Lowercase + strip + normaliza provincia
df_paises = df_paises.apply(
    lambda x: x.str.lower() if x.dtype == "object" else x
)
df_paises["Name"] = df_paises["Name"].apply(normaliza)
# Renombrar titulos
df_paises.rename(columns={
        "Name": "pais",
        "Code": "codigo",
    }, inplace=True)
df_paises.loc[df_paises["pais"] == "spain", "pais"] = "españa"
df_paises.loc[df_paises["pais"] == "saudi arabia", "pais"] = "arabia saudi"
df_paises.loc[df_paises["pais"] == "lithuania", "pais"] = "lituania"
df_paises.loc[df_paises["pais"] == "latvia", "pais"] = "letonia"
df_paises.loc[df_paises["pais"] == "cyprus", "pais"] = "chipre"
df_paises.loc[df_paises["pais"] == "slovakia", "pais"] = "republica eslovaca"
df_paises.loc[df_paises["pais"] == "romania", "pais"] = "rumania"
df_paises.loc[df_paises["pais"] == "mauritius", "pais"] = "mauricio"
df_paises.loc[df_paises["pais"] == "slovenia", "pais"] = "eslovenia"
df_paises.loc[df_paises["pais"] == "ethiopia", "pais"] = "etiopia"
df_paises.loc[df_paises["pais"] == "bahrain", "pais"] = "bahrein"
df_paises.loc[df_paises["pais"] == "germany", "pais"] = "alemania"
df_paises.loc[df_paises["pais"] == "brazil", "pais"] = "brasil"
df_paises.loc[df_paises["pais"] == "belgium", "pais"] = "belgica"
df_paises.loc[df_paises["pais"] == "italy", "pais"] = "italia"
df_paises.loc[df_paises["pais"] == "ireland", "pais"] = "irlanda"
df_paises.loc[df_paises["pais"] == "japan", "pais"] = "japon"
df_paises.loc[df_paises["pais"] == "norway", "pais"] = "noruega"
df_paises.loc[df_paises["pais"] == "moldova, republic of", "pais"] = "moldavia"
df_paises.loc[df_paises["pais"] == "saudi arabia", "pais"] = "arabia saudita"
df_paises.loc[df_paises["pais"] == "bolivia, plurinational state of", "pais"] = "bolivia"
df_paises.loc[df_paises["pais"] == "bonaire, sint eustatius and saba", "pais"] = "bonaire"
df_paises.loc[df_paises["pais"] == "bosnia and herzegovina", "pais"] = "bosnia y herzegovina"
df_paises.loc[df_paises["pais"] == "algeria", "pais"] = "argelia"
df_paises.loc[df_paises["pais"] == "azerbaijan", "pais"] = "azerbaiyán"
df_paises.loc[df_paises["pais"] == "belize", "pais"] = "belice"
df_paises.loc[df_paises["pais"] == "cape verde", "pais"] = "cabo verde"
df_paises.loc[df_paises["pais"] == "belize", "pais"] = "belice"
df_paises.loc[df_paises["pais"] == "cayman islands", "pais"] = "islas caiman"
df_paises.loc[df_paises["pais"] == "croatia", "pais"] = "croacia"
df_paises.loc[df_paises["pais"] == "czech republic", "pais"] = "republica checa"
df_paises.loc[df_paises["pais"] == "denmark", "pais"] = "dinamarca"
df_paises.loc[df_paises["pais"] == "egypt", "pais"] = "egipto"
df_paises.loc[df_paises["pais"] == "france", "pais"] = "francia"
df_paises.loc[df_paises["pais"] == "finland", "pais"] = "finlandia"
df_paises.loc[df_paises["pais"] == "greece", "pais"] = "grecia"
df_paises.loc[df_paises["pais"] == "equatorial guinea", "pais"] = "guinea ecuatorial"
df_paises.loc[df_paises["pais"] == "hungary", "pais"] = "hungria"
df_paises.loc[df_paises["pais"] == "finland", "pais"] = "finlandia"
df_paises.loc[df_paises["pais"] == "iran, islamic republic of", "pais"] = "iran"
df_paises.loc[df_paises["pais"] == "iceland", "pais"] = "islandia"
df_paises.loc[df_paises["pais"] == "jordan", "pais"] = "jordania"
df_paises.loc[df_paises["pais"] == "korea, republic of", "pais"] = "corea"
df_paises.loc[df_paises["pais"] == "lebanon", "pais"] = "libano"
df_paises.loc[df_paises["pais"] == "lithuania", "pais"] = "lituania"
df_paises.loc[df_paises["pais"] == "luxembourg", "pais"] = "luxemburgo"
df_paises.loc[df_paises["pais"] == "macedonia, the former yugoslav republic of", "pais"] = "macedonia del norte"
df_paises.loc[df_paises["pais"] == "morocco", "pais"] = "marruecos"
df_paises.loc[df_paises["pais"] == "netherlands", "pais"] = "paises bajos"
df_paises.loc[df_paises["pais"] == "new zealand", "pais"] = "nueva zelanda"
df_paises.loc[df_paises["pais"] == "poland", "pais"] = "polonia"
df_paises.loc[df_paises["pais"] == "singapore", "pais"] = "singapur"
df_paises.loc[df_paises["pais"] == "sweden", "pais"] = "suecia"
df_paises.loc[df_paises["pais"] == "switzerland", "pais"] = "suiza"
df_paises.loc[df_paises["pais"] == "tanzania, united republic of", "pais"] = "tanzania"
df_paises.loc[df_paises["pais"] == "thailand", "pais"] = "tailandia"
df_paises.loc[df_paises["pais"] == "trinidad and tobago", "pais"] = "trinidad y tobago"
df_paises.loc[df_paises["pais"] == "tunisia", "pais"] = "tunez"
df_paises.loc[df_paises["pais"] == "turkey", "pais"] = "turquia"
df_paises.loc[df_paises["pais"] == "ukraine", "pais"] = "ucrania"
df_paises.loc[df_paises["pais"] == "united arab emirates", "pais"] = "emiratos arabes unidos"
df_paises.loc[df_paises["pais"] == "united kingdom", "pais"] = "reino unido"
df_paises.loc[df_paises["pais"] == "united states", "pais"] = "estados unidos de america"
df_paises.loc[df_paises["pais"] == "venezuela, bolivarian republic of", "pais"] = "venezuela"
df_paises.loc[df_paises["pais"] == "dominican republic", "pais"] = "republica dominicana"
print("Columnas",df_paises.head(5))

# =========================
# 2) CONFIG GEONAMES (HTTP para evitar SSL)
# =========================
GEONAMES_URL = "http://api.geonames.org/searchJSON"
USERNAME = "andreamoralesvega"

# =========================
# 4) FUNCIÓN GEONAMES (MULTI-RESULTADOS + MEJOR CANDIDATO)
# =========================
def geonames_country_latlon(country_code: str, username: str):
    if pd.isna(country_code) or str(country_code).strip() == "":
        return np.nan, np.nan
    params = {
        "country": str(country_code).strip().upper(),  # ISO2 en mayúscula
        "username": username
    }

    r = requests.get(GEONAMES_URL, params=params, timeout=30)
    r.raise_for_status()
    data = r.json()

    geos = data.get("geonames", [])
    if not geos:
        return np.nan, np.nan

    lat = geos[0].get("lat", np.nan)
    lon = geos[0].get("lng", np.nan)
    return float(lat), float(lon)
# Aplicarlo a tu dataset y crear columnas lat/lon
df_paises["lat"], df_paises["lon"] = zip(*df_paises["codigo"].apply(lambda c: geonames_country_latlon(c, USERNAME)))

# Nueva Revision Dataset
df_paises.head(5)
print("Columnas",df_paises.head(5))
df_paises.info()
print("Tipo Datos", df_paises.info())
df_paises.isna().sum()
print("Nulos",df_paises.isna().sum())
df_paises.duplicated().sum()
print("Duplicados", df_paises.duplicated().sum())

#Importar nuevo dataset a cvs
os.makedirs(CLEAN_DIR, exist_ok=True)
output_path = os.path.join(CLEAN_DIR, "paises_old_version.csv")
df_paises.to_csv(
    output_path,
    index=False,
    encoding="utf-8-sig"
)
print("listado de paises guardado")