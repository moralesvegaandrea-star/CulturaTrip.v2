import pandas as pd
import os
import numpy as np
import unicodedata
import re
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
INTERIM_DIR = BASE_DIR / "data" / "interim"   # ✅ NUEVO
CLEAN_DIR = BASE_DIR / "data" / "clean"
OUTPUTS_DIR = BASE_DIR / "outputs"
EXPERIMENTAL_DIR = BASE_DIR / "data" / "Experimental"

RAW_DIR.mkdir(parents=True, exist_ok=True)
INTERIM_DIR.mkdir(parents=True, exist_ok=True)  # ✅ NUEVO
CLEAN_DIR.mkdir(parents=True, exist_ok=True)
OUTPUTS_DIR.mkdir(parents=True, exist_ok=True)
EXPERIMENTAL_DIR.mkdir(parents=True, exist_ok=True)

if not os.path.exists(CLEAN_DIR):
    os.makedirs(CLEAN_DIR)

df_capacidad = pd.read_csv(
    os.path.join(INTERIM_DIR, "conectividad_aerea_capacidad_asientos_clean.csv")
)

df_pasajeros = pd.read_csv(
    os.path.join(INTERIM_DIR, "conectividad_aerea_trafico_pasajeros_clean.csv")
)

print(df_capacidad.shape)
print(df_pasajeros.shape)

print(df_pasajeros.columns.tolist())
print(df_capacidad.columns.tolist())

# Definir llaves para Merge
merge_keys = [
    "año",
    "mes",
    "pais_origen",
    "tipo_origen",
    "ciudad_destino"
]
# Hacer Merge con  left join
df_aereo = pd.merge(
    df_capacidad,
    df_pasajeros,
    on=merge_keys,
    how="left"
)
# Revision
print(df_aereo.shape)
df_aereo.head(5)
print("Columnas",df_aereo.head(5))
df_aereo.info()
print("Tipo Datos", df_aereo.info())
df_aereo.isna().sum()
print("Nulos",df_aereo.isna().sum())
df_aereo.duplicated().sum()
print("Duplicados", df_aereo.duplicated().sum())
print("Nulos en pasajeros:")
print(df_aereo["pasajeros"].isna().mean())

df_aereo["pasajeros_calc"] = df_aereo["pasajeros"].fillna(0)
df_aereo["hay_pasajeros"] = df_aereo["pasajeros"].notna().astype(int)

print(df_aereo[["pasajeros", "pasajeros_calc", "hay_pasajeros"]].head())

#La variable de pasajeros presenta valores nulos en determinadas combinaciones de origen,
# destino y periodo, debido a la ausencia de datos reportados en la fuente original.
# En lugar de imputar artificialmente estos valores, se mantuvieron como nulos y
# se crearon variables auxiliares para el cálculo de indicadores agregados, preservando
# la integridad del dato original

df_paises = pd.read_csv(
    os.path.join(CLEAN_DIR, "dim_pais.csv")
)
print(df_paises.shape)
print(df_paises.head(5))

df_aereo["pais_origen"] = df_aereo["pais_origen"].apply(normaliza)
df_paises["pais"] = df_paises["pais"].apply(normaliza)

paises_aereo = set(df_aereo["pais_origen"].dropna().unique())
paises_ref = set(df_paises["pais"].dropna().unique())

print("Paises en conectividad aérea:", len(paises_aereo))
print("Paises en dataset de países:", len(paises_ref))

paises_ok = paises_aereo.intersection(paises_ref)
print("Coinciden:", len(paises_ok))

paises_faltantes = paises_aereo - paises_ref
print("NO encontrados en paises.csv:")
for p in sorted(paises_faltantes):
    print("-", p)

print(df_aereo.columns.tolist())
print(df_paises.columns.tolist())


df_paises_merge = df_paises[["pais", "id_pais", "lat", "lon"]].copy()
df_paises_merge = df_paises_merge.rename(columns={"lat": "lat_pais", "lon": "lon_pais"})

df_aereo = df_aereo.merge(
    df_paises_merge,
    left_on="pais_origen",
    right_on="pais",
    how="left"
).drop(columns=["pais"])  # quita la col duplicada del dim

print(df_aereo.head())

cols_needed = [
    "año", "mes", "pais_origen", "id_pais",
    "ciudad_destino", "tipo_origen", "asientos", "distancia_media",
    "variacion_interanual_asientos", "pasajeros", "pasajeros_calc", "hay_pasajeros",
    "lat_pais", "lon_pais"
]


df_aereo = df_aereo[cols_needed]
print("Columnas",df_aereo.head(5))
print(df_aereo.columns.tolist())
df_aereo.isna().sum()
print("Nulos",df_aereo.isna().sum())
df_aereo.duplicated().sum()
print("Duplicados", df_aereo.duplicated().sum())

# =========================
# 9) Guardar dataset final
# =========================
output_path = os.path.join(
    EXPERIMENTAL_DIR,
    "conectividad_aerea_merged_clean.csv"
)

df_aereo.to_csv(output_path, index=False)
print(f"Dataset guardado en: {output_path}")