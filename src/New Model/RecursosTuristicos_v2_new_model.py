import os
import pandas as pd
import unicodedata
import re
from pathlib import Path
# =========================
# 0) HELPERS
# =========================
def normaliza(s: str) -> str:
    if pd.isna(s):
        return ""
    s = str(s).strip().lower()
    s = unicodedata.normalize("NFKD", s).encode("ascii", "ignore").decode("utf-8")
    s = re.sub(r"\s+", " ", s)
    return s

def to_float_es(x):
    """Convierte '1.234,56' -> 1234.56. Si falla, NaN."""
    if pd.isna(x):
        return pd.NA
    x = str(x).strip()
    x = x.replace(".", "").replace(",", ".")
    return pd.to_numeric(x, errors="coerce")

def load_csv_auto(path):
    try:
        return pd.read_csv(path, encoding="utf-8", sep=None, engine="python")
    except UnicodeDecodeError:
        return pd.read_csv(path, encoding="latin1", sep=None, engine="python")

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

path_in = os.path.join(RAW_DIR, "atracciones_recursos_turisticos.csv")
path_dim_prov = os.path.join(CLEAN_DIR, "dim_provincia_base.csv")

# =========================
# 2) CARGAR
# =========================
df = load_csv_auto(path_in)
df.columns = df.columns.str.strip().str.lower()

# Renombrar a estándar
df = df.rename(columns={
    "ccaa": "comunidad_autonoma"
})

# Normalizar textos clave
for col in ["provincia", "comunidad_autonoma", "categoria"]:
    if col in df.columns:
        df[col] = df[col].astype(str).apply(normaliza)

# =========================
# 3) LIMPIEZA CATEGORÍA
# =========================
if "categoria" in df.columns:
    df["categoria"] = df["categoria"].replace({
        "desconocido": "otros",
        "activo naturaleza": "paisaje naturaleza",
        "activo urbano": "paisaje urbano",
        "museos": "paisaje urbano"
    })

# =========================
# 4) NUMÉRICOS
# =========================
cols_to_numeric = ["valoracion_por_categoria", "valoracion_general", "total_opiniones_categoria"]
for col in cols_to_numeric:
    if col in df.columns:
        df[col] = df[col].apply(to_float_es)

# =========================
# 5) AGREGACIÓN (PROVINCIA-LEVEL)
# =========================
df_agg = (
    df.groupby(["categoria", "comunidad_autonoma", "provincia"], as_index=False)
      .agg(
          valoracion_por_categoria_promedio=("valoracion_por_categoria", "mean"),
          valoracion_general_promedio=("valoracion_general", "mean"),
          total_opiniones_categoria_promedio=("total_opiniones_categoria", "mean")
      )
)

df_agg["valoracion_por_categoria_promedio"] = df_agg["valoracion_por_categoria_promedio"].round(2)
df_agg["valoracion_general_promedio"] = df_agg["valoracion_general_promedio"].round(2)

# =========================
# 6) ENRIQUECER CON dim_provincia_base (FK real)
# =========================
df_prov = pd.read_csv(path_dim_prov, dtype=str)
df_prov.columns = df_prov.columns.str.strip()

# Normalizar provincia_nombre para match
df_prov["provincia_nombre_norm"] = df_prov["provincia_nombre"].astype(str).apply(normaliza)

# Agregar id_pais
df_agg.insert(0, "id_pais", "ES")
df_agg["provincia_norm"] = df_agg["provincia"].apply(normaliza)

df_agg = df_agg.merge(
    df_prov[["id_pais", "id_provincia", "provincia_nombre", "provincia_nombre_norm"]],
    left_on=["id_pais", "provincia_norm"],
    right_on=["id_pais", "provincia_nombre_norm"],
    how="left"
)

# QA: provincias sin match
qa_no_match = df_agg[df_agg["id_provincia"].isna()][["provincia"]].drop_duplicates()
qa_path = os.path.join(OUTPUTS_DIR, "qa_recursos_provincias_no_match.csv")
qa_no_match.to_csv(qa_path, index=False, encoding="utf-8-sig")

print("✅ QA provincias sin match:", qa_path)
print("Nulos id_provincia:", df_agg["id_provincia"].isna().sum())

# Limpieza auxiliares
df_agg = df_agg.drop(columns=["provincia_norm", "provincia_nombre_norm"], errors="ignore")

# (Opcional) gid_provincia
df_agg["gid_provincia"] = df_agg["id_pais"] + "-" + df_agg["id_provincia"].astype(str)

# =========================
# 7) ORDEN FINAL Y GUARDAR (INTERIM)
# =========================
orden = [
    "id_pais",
    "id_provincia",
    "gid_provincia",
    "categoria",
    "comunidad_autonoma",
    "provincia",
    "valoracion_por_categoria_promedio",
    "valoracion_general_promedio",
    "total_opiniones_categoria_promedio"
]
orden_existente = [c for c in orden if c in df_agg.columns]
resto = [c for c in df_agg.columns if c not in orden_existente]
df_agg = df_agg[orden_existente + resto]

out_path = INTERIM_DIR / "fact_recursos_turisticos_provincia.csv"  # ✅ CAMBIO AQUÍ
df_agg.to_csv(out_path, index=False, encoding="utf-8-sig")

print("✅ Guardado (interim):", out_path)
print("Shape:", df_agg.shape)

