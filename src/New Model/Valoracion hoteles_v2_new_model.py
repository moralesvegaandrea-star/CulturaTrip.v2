import os
import re
import unicodedata
import numpy as np
import pandas as pd
from pathlib import Path

# =========================
# HELPERS
# =========================
def load_csv(path: str) -> pd.DataFrame:
    try:
        return pd.read_csv(path, encoding="utf-8", sep=None, engine="python")
    except UnicodeDecodeError:
        return pd.read_csv(path, encoding="latin1", sep=None, engine="python")

def normaliza(s: str) -> str:
    if pd.isna(s):
        return ""
    s = str(s).strip().lower()
    s = unicodedata.normalize("NFKD", s).encode("ascii", "ignore").decode("utf-8")
    s = s.replace("–", "-").replace("—", "-")
    s = re.sub(r"\s*-\s*", "-", s)
    s = re.sub(r"\s+", " ", s)
    return s

def clean_headers(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df.columns = (
        df.columns.astype(str)
        .str.replace("\ufeff", "", regex=False)
        .str.strip()
        .str.lower()
    )
    return df

def to_float_es(x):
    if pd.isna(x):
        return np.nan
    x = str(x).strip()
    x = x.replace(".", "").replace(",", ".").replace("%", "")
    if x.lower() in ["nan", "none", "null", "-", "—", ""]:
        return np.nan
    try:
        return float(x)
    except ValueError:
        return np.nan

# Llave estable (misma lógica que VUT)
def ccaa_key(s: str) -> str:
    s = normaliza(s)

    if "navarra" in s:
        return "navarra"
    if "asturias" in s:
        return "asturias"
    if "madrid" in s:
        return "madrid"
    if "murcia" in s:
        return "murcia"
    if "rioja" in s:
        return "rioja"
    if "balears" in s or "illes" in s:
        return "balears"

    if "," in s:
        return s.split(",")[0].strip()

    return s

# =========================
# RUTAS
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

PATH_IN = os.path.join(RAW_DIR, "ind_satisfaccion_menciones_hotel.csv")
PATH_DIM_PROV = os.path.join(CLEAN_DIR, "dim_provincia_base.csv")
PATH_DIM_CCAA = os.path.join(CLEAN_DIR, "dim_ccaa_base.csv")

# =========================
# 1) CARGA
# =========================
df = load_csv(PATH_IN)
df = clean_headers(df)

cols_needed = ["mes", "categoria_alojamiento", "ccaa", "provincia", "indice_satisfaccion_hotelera"]
missing = set(cols_needed) - set(df.columns)
if missing:
    raise KeyError(f"Faltan columnas en el input: {missing}")

df = df[cols_needed].copy()
df = df.rename(columns={"indice_satisfaccion_hotelera": "valoraciones"})

# =========================
# 2) NORMALIZAR TEXTO
# =========================
for c in ["ccaa", "provincia", "categoria_alojamiento"]:
    df[c] = df[c].apply(normaliza)

map_prov = {
    "ciudad autonoma de ceuta": "ceuta",
    "ciudad autónoma de ceuta": "ceuta",
    "ciudad autonoma de melilla": "melilla",
    "ciudad autónoma de melilla": "melilla",
}
df["provincia"] = df["provincia"].replace(map_prov)

df["ccaa_key"] = df["ccaa"].apply(ccaa_key)

# =========================
# 3) NUMÉRICOS
# =========================
df["valoraciones"] = df["valoraciones"].apply(to_float_es)
df["valoraciones"] = pd.to_numeric(df["valoraciones"], errors="coerce")

# =========================
# 4) CONSOLIDAR PK
# =========================
pk = ["mes", "ccaa_key", "provincia", "categoria_alojamiento"]

df = (
    df.groupby(pk, as_index=False)
      .agg(valoraciones=("valoraciones", "mean"))
)

# Imputación
df["valoraciones"] = df["valoraciones"].fillna(
    df.groupby(["categoria_alojamiento", "ccaa_key", "mes"])["valoraciones"].transform("mean")
)
df["valoraciones"] = df["valoraciones"].fillna(
    df.groupby(["categoria_alojamiento", "mes"])["valoraciones"].transform("mean")
)
df["valoraciones"] = df["valoraciones"].fillna(df["valoraciones"].mean())

# =========================
# 5) DIMENSIONES
# =========================
dim_prov = pd.read_csv(PATH_DIM_PROV, dtype=str, encoding="utf-8-sig")
dim_ccaa = pd.read_csv(PATH_DIM_CCAA, dtype=str, encoding="utf-8-sig")

dim_prov = clean_headers(dim_prov)
dim_ccaa = clean_headers(dim_ccaa)

dim_prov["id_pais"] = dim_prov.get("id_pais", "ES").astype(str)
dim_prov["id_provincia"] = dim_prov["id_provincia"].astype(str).str.zfill(2)
dim_prov["provincia_nombre"] = dim_prov["provincia_nombre"].astype(str).apply(normaliza)

dim_ccaa["id_ccaa"] = dim_ccaa["id_ccaa"].astype(str).str.zfill(2)
dim_ccaa["ccaa_nombre"] = dim_ccaa["ccaa_nombre"].astype(str).apply(normaliza)
dim_ccaa["ccaa_key"] = dim_ccaa["ccaa_nombre"].apply(ccaa_key)

# MUY IMPORTANTE: 1 fila por ccaa_key (evita duplicar)
dim_ccaa = dim_ccaa.drop_duplicates(subset=["ccaa_key"]).copy()

# =========================
# 6) MAPEO A IDS
# =========================
df.insert(0, "id_pais", "ES")

# provincia -> id_provincia
df["provincia_norm"] = df["provincia"].apply(normaliza)

df = df.merge(
    dim_prov[["id_pais", "id_provincia", "provincia_nombre"]],
    left_on=["id_pais", "provincia_norm"],
    right_on=["id_pais", "provincia_nombre"],
    how="left"
).drop(columns=["provincia_nombre", "provincia_norm"], errors="ignore")

# ccaa_key -> id_ccaa y NOMBRE OFICIAL
df = df.merge(
    dim_ccaa[["id_ccaa", "ccaa_key", "ccaa_nombre"]],
    on="ccaa_key",
    how="left"
)

# ESTANDARIZAR TEXTO: usa el nombre oficial de la dimensión
df["ccaa"] = df["ccaa_nombre"]
df = df.drop(columns=["ccaa_nombre"], errors="ignore")

# =========================
# 7) QA (principiante)
# =========================
qa_no_ccaa = df[df["id_ccaa"].isna()][["ccaa_key"]].drop_duplicates().sort_values("ccaa_key")
qa_no_prov = df[df["id_provincia"].isna()][["provincia"]].drop_duplicates().sort_values("provincia")

print("\n--- QA ---")
print("Filas sin id_ccaa:", df["id_ccaa"].isna().sum())
print("Filas sin id_provincia:", df["id_provincia"].isna().sum())

if not qa_no_ccaa.empty:
    qa_path = os.path.join(OUTPUTS_DIR, "qa_hvaloracion_ccaa_no_match.csv")
    qa_no_ccaa.to_csv(qa_path, index=False, encoding="utf-8-sig")
    print("📝 QA CCAA guardado:", qa_path)

if not qa_no_prov.empty:
    qa_path = os.path.join(OUTPUTS_DIR, "qa_hvaloracion_provincia_no_match.csv")
    qa_no_prov.to_csv(qa_path, index=False, encoding="utf-8-sig")
    print("📝 QA provincias guardado:", qa_path)

# =========================
# 8) ORDEN / GUARDAR (INTERIM)
# =========================
df["valoraciones"] = df["valoraciones"].round(2)

orden = [
    "id_pais",
    "id_ccaa",
    "id_provincia",
    "ccaa",
    "ccaa_key",
    "provincia",
    "mes",
    "categoria_alojamiento",
    "valoraciones"
]
df = df[[c for c in orden if c in df.columns]].copy()

out_path = INTERIM_DIR / "df_hvaloracion_limpio.csv"   # ✅ CAMBIO AQUÍ
df.to_csv(out_path, index=False, encoding="utf-8-sig")

print("\n✅ Guardado (interim):", out_path)
print("✅ Nulos id_ccaa:", df["id_ccaa"].isna().sum())

freq_vivienda = df["ccaa"].value_counts().reset_index()
freq_vivienda.columns = ["ccaa", "frecuencia"]
print(freq_vivienda)

