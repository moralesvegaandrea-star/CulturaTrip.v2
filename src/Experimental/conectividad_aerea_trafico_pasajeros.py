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


# Este dataset mide DEMANDA
df_trafico = load_csv(os.path.join(RAW_DIR, "conectividad_aerea_trafico_pasajeros.csv"))
# Revision Dataset
df_trafico.head(5)
print("Columnas",df_trafico.head(5))
df_trafico.info()
print("Tipo Datos", df_trafico.info())
df_trafico.isna().sum()
print("Nulos",df_trafico.isna().sum())
df_trafico.duplicated().sum()
print("Duplicados", df_trafico.duplicated().sum())

# Lowercase + strip + normaliza provincia
df_aereos = df_trafico.apply(
    lambda x: x.str.lower() if x.dtype == "object" else x
)
df_trafico.columns = df_trafico.columns.str.strip().str.lower()
if "pais_origen" in df_trafico.columns:
    df_trafico["pais_origen"] = df_trafico["pais_origen"].apply(normaliza)
if "ciudad_destino" in df_trafico.columns:
    df_trafico["ciudad_destino"] = df_trafico["ciudad_destino"].apply(normaliza)
if "tipo_origen" in df_trafico.columns:
    df_trafico["tipo_origen"] = df_trafico["tipo_origen"].apply(normaliza)

cols_needed = [
    "año",
    "mes",
    "pais_origen",
    "tipo_origen",
    "ciudad_destino",
    "pasajeros"
]

df_trafico = df_trafico[cols_needed]

print("Columnas",df_trafico.head(5))

freq_pais_origen = df_trafico["pais_origen"].value_counts().reset_index()
freq_pais_origen.columns = ["pais_origen","frecuencia"]
print(freq_pais_origen)

df_trafico = df_trafico[
    df_trafico["pais_origen"].str.strip().str.lower() != "total"
]
print(df_trafico["pais_origen"].value_counts().head(25))


# =========================
# 9) Guardar dataset final
# =========================
output_path = os.path.join(
    CLEAN_DIR,
    "conectividad_aerea_trafico_pasajeros_clean.csv"
)

df_trafico.to_csv(output_path, index=False)

print(f"Dataset guardado en: {output_path}")