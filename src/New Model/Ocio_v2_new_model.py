import os
import pandas as pd
import unicodedata
import re
import numpy as np
from pathlib import Path

# =========================
# 0) HELPERS
# =========================
def normaliza(s: str) -> str:
    """lower, sin tildes, espacios limpios."""
    if pd.isna(s):
        return ""
    s = str(s).strip().lower()
    s = unicodedata.normalize("NFKD", s).encode("ascii", "ignore").decode("utf-8")
    s = re.sub(r"\s+", " ", s)
    s = s.replace("–", "-").replace("—", "-")
    return s

def to_float_es(x):
    """Convierte '1.234,56' -> 1234.56. Si falla o es vacío, NaN."""
    if pd.isna(x):
        return np.nan
    x = str(x).strip()
    if x.lower() in ["nan", "none", "null", "-", "—", ""]:
        return np.nan
    x = x.replace(".", "").replace(",", ".").replace("%", "")
    return pd.to_numeric(x, errors="coerce")

def clean_headers(df):
    df = df.copy()
    df.columns = (
        df.columns.astype(str)
        .str.replace("\ufeff", "", regex=False)
        .str.strip()
        .str.lower()
    )
    return df

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

path_in = os.path.join(RAW_DIR, "actividades_ocio.xlsx")
path_dim_prov = os.path.join(CLEAN_DIR, "dim_provincia_base.csv")

# =========================
# 2) CARGAR ACTIVIDADES
# =========================
df = pd.read_excel(path_in, header=0)
df = clean_headers(df)

print("Columnas actividades:", df.columns.tolist())
print("Shape raw:", df.shape)

# Renombres (alinear a tu modelo)
df = df.rename(columns={
    "ccaa": "comunidad_autonoma",
    "categoria": "subcategoria"
})

# Validar columnas mínimas
need = {"mes", "producto", "subcategoria", "comunidad_autonoma", "provincia", "gasto_total", "precio_medio_entrada"}
missing = need - set(df.columns)
if missing:
    raise KeyError(f"Faltan columnas en actividades_ocio.xlsx: {missing}")

# =========================
# 3) LIMPIEZA TEXTO (NO tocar mes como texto)
# =========================
for col in ["provincia", "comunidad_autonoma", "producto", "subcategoria"]:
    df[col] = df[col].astype(str).apply(normaliza)

# mes numérico
df["mes"] = pd.to_numeric(df["mes"], errors="coerce")

# =========================
# 4) NUMÉRICOS
# =========================
df["gasto_total"] = df["gasto_total"].apply(to_float_es)
df["precio_medio_entrada"] = df["precio_medio_entrada"].apply(to_float_es)

# =========================
# 5) FILTRAR "TOTAL NACIONAL"
# =========================
df = df[df["provincia"] != "total nacional"].copy()

# =========================
# 6) AGREGAR POR PROVINCIA
# =========================
df_agg = (
    df.groupby(["mes", "producto", "subcategoria", "comunidad_autonoma", "provincia"], as_index=False)
      .agg(
          gasto_total_promedio=("gasto_total", "mean"),
          precio_medio_entrada_promedio=("precio_medio_entrada", "mean")
      )
)

df_agg["gasto_total_promedio"] = df_agg["gasto_total_promedio"].round(2)
df_agg["precio_medio_entrada_promedio"] = df_agg["precio_medio_entrada_promedio"].round(2)

print("Shape agg:", df_agg.shape)

# =========================
# 7) CATEGORÍA (MAP)
# =========================
map_categoria = {
    "deportes y aventuras": "paisaje naturaleza",
    "actividades infantiles": "paisaje naturaleza",
    "parques tematicos": "paisaje naturaleza",
    "museo y exposiciones": "paisaje urbano",
    "cultura, teatro y danza": "paisaje urbano",
    "ferias": "compras",
    "cursos": "servicios",
    "conferencias": "otros",
    "gastronomia": "comida y bebida",
    "musica": "vida nocturna",
    "musicales": "vida nocturna",
    "cine": "vida nocturna"
}
df_agg["categoria"] = df_agg["subcategoria"].map(map_categoria)

# =========================
# 8) CARGAR dim_provincia_base (FK real)
# =========================
df_prov = pd.read_csv(path_dim_prov, dtype=str, encoding="utf-8-sig")
df_prov = clean_headers(df_prov)

# Validar columnas mínimas dim_prov
need_prov = {"id_pais", "id_provincia", "provincia_nombre"}
missing_prov = need_prov - set(df_prov.columns)
if missing_prov:
    raise KeyError(f"dim_provincia_base.csv no tiene columnas requeridas: {missing_prov}")

# Normalizar provincia_nombre
df_prov["provincia_nombre_norm"] = df_prov["provincia_nombre"].astype(str).apply(normaliza)

# =========================
# 9) ALIAS DE PROVINCIAS (evita no-match típicos)
# =========================
alias_prov = {
    "ciudad autonoma de ceuta": "ceuta",
    "ciudad autonoma de melilla": "melilla",
    "ciudad autónoma de ceuta": "ceuta",
    "ciudad autónoma de melilla": "melilla",
}

df_agg["provincia"] = df_agg["provincia"].replace(alias_prov)
df_prov["provincia_nombre_norm"] = df_prov["provincia_nombre_norm"].replace(alias_prov)

# =========================
# 10) MERGE POR (id_pais + provincia_norm)
# =========================
df_agg.insert(0, "id_pais", "ES")
df_agg["provincia_norm"] = df_agg["provincia"].apply(normaliza)

# Por seguridad: una fila por provincia en dim_prov
df_prov_small = (
    df_prov[["id_pais", "id_provincia", "provincia_nombre_norm"]]
    .drop_duplicates(subset=["id_pais", "provincia_nombre_norm"])
    .copy()
)

df_agg = df_agg.merge(
    df_prov_small,
    left_on=["id_pais", "provincia_norm"],
    right_on=["id_pais", "provincia_nombre_norm"],
    how="left"
)

# =========================
# 11) QA: PROVINCIAS SIN MATCH
# =========================
mask_no = df_agg["id_provincia"].isna()
no_match = (
    df_agg.loc[mask_no, ["provincia"]]
    .drop_duplicates()
    .sort_values("provincia")
)

print("❗ Filas sin id_provincia:", mask_no.sum())
if not no_match.empty:
    qa_path = os.path.join(OUTPUTS_DIR, "qa_ocio_provincias_no_match.csv")
    no_match.to_csv(qa_path, index=False, encoding="utf-8-sig")
    print("📝 QA guardado:", qa_path)
    print("Provincias no match:", no_match["provincia"].tolist())

# Limpieza columnas auxiliares
df_agg = df_agg.drop(columns=["provincia_norm", "provincia_nombre_norm"], errors="ignore")

# =========================
# 12) ORDEN FINAL + GUARDAR
# =========================
orden = [
    "id_pais",
    "id_provincia",
    "mes",
    "categoria",
    "producto",
    "subcategoria",
    "comunidad_autonoma",
    "provincia",
    "gasto_total_promedio",
    "precio_medio_entrada_promedio"
]
orden_existente = [c for c in orden if c in df_agg.columns]
resto = [c for c in df_agg.columns if c not in orden_existente]
df_agg = df_agg[orden_existente + resto]

out_path = os.path.join(INTERIM_DIR, "fact_actividades_ocio_provincia.csv")
df_agg.to_csv(out_path, index=False, encoding="utf-8-sig")

print("✅ Guardado:", out_path)
print("Shape final:", df_agg.shape)
print("✅ Nulos id_provincia:", df_agg["id_provincia"].isna().sum())
