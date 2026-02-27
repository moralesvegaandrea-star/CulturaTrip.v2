import os
import re
import unicodedata
import numpy as np
import pandas as pd
from pathlib import Path

# =========================
# HELPERS
# =========================
def load_csv(path):
    try:
        return pd.read_csv(path, encoding="utf-8", sep=None, engine="python", dtype=str)
    except UnicodeDecodeError:
        return pd.read_csv(path, encoding="latin1", sep=None, engine="python", dtype=str)

def normaliza(s: str) -> str:
    if pd.isna(s):
        return ""
    s = str(s).strip().lower()
    s = unicodedata.normalize("NFKD", s).encode("ascii", "ignore").decode("utf-8")
    s = re.sub(r"\s+", " ", s)
    s = s.replace("–", "-").replace("—", "-")
    return s

def clean_headers(df):
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

# ✅ función sencilla para crear una llave estable de CCAA
def ccaa_key(s: str) -> str:
    s = normaliza(s)

    # casos “problemáticos” (los tuyos)
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
    if "balears" in s or "illes balears" in s or "illes" in s:
        return "balears"

    # regla general: si trae coma, usar lo de antes de la coma
    # ejemplo: "madrid, comunidad de" -> "madrid"
    if "," in s:
        return s.split(",")[0].strip()

    return s

# =========================
# ✅ PERIODO ANTELACION (estacionalidad)
# =========================
def antelacion_por_mes(mes):
    """
    Regla simple:
    - Alta demanda: junio, julio, agosto, diciembre -> "2-3 meses"
    - Resto -> "1 mes"
    """
    try:
        mes = int(mes)
    except (TypeError, ValueError):
        return pd.NA

    if mes in [6, 7, 8, 12]:
        return "2-3 meses"
    else:
        return "1 mes"

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

PATH_IN = os.path.join(RAW_DIR, "vut_fuente_privada_ccaa.csv")
PATH_DIM_CCAA = os.path.join(CLEAN_DIR, "dim_ccaa_base.csv")

# =========================
# 1) CARGA
# =========================
df = load_csv(PATH_IN)
df = clean_headers(df)

print("Columnas detectadas VUT:", df.columns.tolist())
print("Shape VUT raw:", df.shape)

need = {"mes", "ccaa", "tipo_vivienda"}
missing = need - set(df.columns)
if missing:
    raise KeyError(f"Faltan columnas en VUT: {missing}")

# =========================
# 2) NORMALIZAR TEXTO
# =========================
df["ccaa"] = df["ccaa"].apply(normaliza)
df["tipo_vivienda"] = df["tipo_vivienda"].apply(normaliza)
df = df.rename(columns={"tipo_vivienda": "categoria_alojamiento"})

# ✅ crear llave estable
df["ccaa_key"] = df["ccaa"].apply(ccaa_key)

print("\nTop CCAA (raw):")
print(df["ccaa"].value_counts().head(20))
print("\nTop CCAA_KEY:")
print(df["ccaa_key"].value_counts().head(20))

# =========================
# 3) LIMPIEZA NUMÉRICA
# =========================
for col in ["valoraciones", "precio"]:
    if col not in df.columns:
        df[col] = np.nan

df["valoraciones"] = df["valoraciones"].apply(to_float_es)
df["precio"] = df["precio"].apply(to_float_es)

# =========================
# 4) FILTROS
# =========================
df = df[df["ccaa"] != "total nacional"].copy()
df = df[~df["categoria_alojamiento"].isin(["otros", "no aplica"])].copy()

# =========================
# 5) AGREGAR
# =========================
df["mes"] = pd.to_numeric(df["mes"], errors="coerce")

df_agg = (
    df.groupby(["mes", "ccaa", "ccaa_key", "categoria_alojamiento"], as_index=False)
      .agg(
          valoraciones=("valoraciones", "mean"),
          precio=("precio", "mean")
      )
)

df_agg = df_agg.dropna(subset=["precio"]).copy()
print("Shape VUT agg:", df_agg.shape)

# =========================
# 6) DIM_CCAA + KEY
# =========================
dim_ccaa = pd.read_csv(PATH_DIM_CCAA, dtype=str, encoding="utf-8-sig")
dim_ccaa = clean_headers(dim_ccaa)

dim_ccaa["ccaa_nombre"] = dim_ccaa["ccaa_nombre"].apply(normaliza)
dim_ccaa["id_ccaa"] = dim_ccaa["id_ccaa"].astype(str).str.zfill(2)

# ✅ crear la misma llave en la dimensión
dim_ccaa["ccaa_key"] = dim_ccaa["ccaa_nombre"].apply(ccaa_key)

print("\nTop DIM CCAA_NOMBRE:")
print(dim_ccaa["ccaa_nombre"].value_counts().head(20))
print("\nTop DIM CCAA_KEY:")
print(dim_ccaa["ccaa_key"].value_counts().head(20))

# id_pais fijo
df_agg.insert(0, "id_pais", "ES")

# ✅ merge por ccaa_key (NO por nombre completo)
df_agg = df_agg.merge(
    dim_ccaa[["id_ccaa", "ccaa_key"]],
    on="ccaa_key",
    how="left"
)

# =========================
# 6.1) QA PRINCIPIANTE: ver qué ccaa_key no matchea
# =========================
no_match = (
    df_agg[df_agg["id_ccaa"].isna()][["ccaa", "ccaa_key"]]
    .drop_duplicates()
    .sort_values(["ccaa_key", "ccaa"])
)

print("\n❗ Filas sin id_ccaa:", df_agg["id_ccaa"].isna().sum())
print("ccaa_key sin match (únicas):")
print(no_match)

if not no_match.empty:
    qa_path = os.path.join(OUTPUTS_DIR, "qa_vut_ccaa_no_match.csv")
    no_match.to_csv(qa_path, index=False, encoding="utf-8-sig")
    print("📝 QA guardado:", qa_path)

# =========================
# 7) ESTRUCTURA PRECIOS
# =========================
df_agg = df_agg.rename(columns={"precio": "precio_checkin_entre_semana"})
df_agg["precio_checkin_fin_semana"] = df_agg["precio_checkin_entre_semana"]

# =========================
# 8) ✅ PERIODO ANTELACION (incluido)
# =========================
df_agg["periodo_antelacion"] = df_agg["mes"].apply(antelacion_por_mes)

print("\nDistribución periodo_antelacion:")
print(df_agg["periodo_antelacion"].value_counts(dropna=False))

# =========================
# 9) REDONDEO
# =========================
for col in ["precio_checkin_entre_semana", "precio_checkin_fin_semana", "valoraciones"]:
    df_agg[col] = pd.to_numeric(df_agg[col], errors="coerce")

df_agg["precio_checkin_entre_semana"] = df_agg["precio_checkin_entre_semana"].round(2)
df_agg["precio_checkin_fin_semana"] = df_agg["precio_checkin_fin_semana"].round(2)
df_agg["valoraciones"] = df_agg["valoraciones"].round(2)

# =========================
# 10) ORDEN FINAL
# =========================
orden = [
    "id_pais",
    "id_ccaa",
    "ccaa",
    "ccaa_key",
    "mes",
    "categoria_alojamiento",
    "valoraciones",
    "precio_checkin_entre_semana",
    "precio_checkin_fin_semana",
    "periodo_antelacion"
]
df_agg = df_agg[[c for c in orden if c in df_agg.columns]].copy()

# ✅ GUARDAR (en interim)
out_path = os.path.join(INTERIM_DIR, "df_vivienda_modelo_final.csv")
df_agg.to_csv(out_path, index=False, encoding="utf-8-sig")
print("\n✅ Guardado:", out_path)
print("✅ Nulos id_ccaa:", df_agg["id_ccaa"].isna().sum())