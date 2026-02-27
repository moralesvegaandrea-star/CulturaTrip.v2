import pandas as pd
import os
import numpy as np
import unicodedata
import re
import matplotlib.pyplot as plt
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

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
RAW_DIR = os.path.join(BASE_DIR, "data", "raw")
CLEAN_DIR = os.path.join(BASE_DIR, "data", "clean")
OUTPUTS_DIR = os.path.join(BASE_DIR, "outputs")
NOTEBOOKS_DIR = os.path.join(BASE_DIR, "notebooks")

if not os.path.exists(CLEAN_DIR):
    os.makedirs(CLEAN_DIR)


# Aportar distancia, capacidad (oferta) y tipo de conexión aérea por periodo y destino.
df_aereos = load_csv(os.path.join(RAW_DIR, "conectividad_aerea_capacidad_asientos.csv"))

# Revision Dataset
df_aereos.head(5)
print("Columnas",df_aereos.head(5))
df_aereos.info()
print("Tipo Datos", df_aereos.info())
df_aereos.isna().sum()
print("Nulos",df_aereos.isna().sum())
df_aereos.duplicated().sum()
print("Duplicados", df_aereos.duplicated().sum())

# Lowercase + strip + normaliza provincia
df_aereos = df_aereos.apply(
    lambda x: x.str.lower() if x.dtype == "object" else x
)
df_aereos.columns = df_aereos.columns.str.strip().str.lower()
if "PAIS_ORIGEN" in df_aereos.columns:
    df_aereos["PAIS_ORIGEN"] = df_aereos["PAIS_ORIGEN"].apply(normaliza)
if "CIUDAD_DESTINO" in df_aereos.columns:
    df_aereos["CIUDAD_DESTINO"] = df_aereos["CIUDAD_DESTINO"].apply(normaliza)
if "TIPO_ORIGEN" in df_aereos.columns:
    df_aereos["TIPO_ORIGEN"] = df_aereos["TIPO_ORIGEN"].apply(normaliza)
# Cambio de formato
cols_to_numeric = [
    "distancia_media",
    "variacion_interanual_asientos"
]
for col in cols_to_numeric:
    if col in df_aereos.columns:  # 🛡️ protección
        df_aereos[col] = (
            df_aereos[col]
            .astype(str)
            .str.replace(".", "", regex=False)   # separador miles
            .str.replace(",", ".", regex=False)  # decimal
        )
        df_aereos[col] = pd.to_numeric(df_aereos[col], errors="coerce")

# Revision Dataset 2
df_aereos.head(5)
print("Columnas",df_aereos.head(5))
df_aereos.info()
print("Tipo Datos", df_aereos.info())
df_aereos.isna().sum()
print("Nulos",df_aereos.isna().sum())
df_aereos.duplicated().sum()
print("Duplicados", df_aereos.duplicated().sum())

freq_pais_origen = df_aereos["pais_origen"].value_counts().reset_index()
freq_pais_origen.columns = ["pais_origen","frecuencia"]
print(freq_pais_origen)

df_aereos = df_aereos[
    df_aereos["pais_origen"].str.strip().str.lower() != "total"
]
print(df_aereos["pais_origen"].value_counts().head(25))

# =========================
# 9) Guardar dataset final
# =========================
output_path = os.path.join(
    CLEAN_DIR,
    "conectividad_aerea_capacidad_asientos_clean.csv"
)

df_aereos.to_csv(output_path, index=False)

print(f"Dataset guardado en: {output_path}")
