import os
import pandas as pd
from pathlib import Path
import numpy as np

# =========================
# 1) RUTAS
# =========================
BASE_DIR = Path(__file__).resolve().parents[2]

RAW_DIR = BASE_DIR / "data" / "raw"
INTERIM_DIR = BASE_DIR / "data" / "interim"
CLEAN_DIR = BASE_DIR / "data" / "clean"
OUTPUTS_DIR = BASE_DIR / "outputs"

RAW_DIR.mkdir(parents=True, exist_ok=True)
INTERIM_DIR.mkdir(parents=True, exist_ok=True)
CLEAN_DIR.mkdir(parents=True, exist_ok=True)
OUTPUTS_DIR.mkdir(parents=True, exist_ok=True)

# =========================
# 2) INPUTS / OUTPUT
# =========================
OCIO_PATH = os.path.join(INTERIM_DIR, "fact_actividades_ocio_provincia.csv")
RECURSOS_PATH = os.path.join(INTERIM_DIR, "fact_recursos_turisticos_provincia.csv")

PATH_DIM_PROV = os.path.join(CLEAN_DIR, "dim_provincia_base.csv")
# Fallback opcional (si dim_provincia_base no trae id_ccaa)
PATH_DIM_MUNI = os.path.join(CLEAN_DIR, "dim_municipio_final.csv")

OUT_PATH = os.path.join(CLEAN_DIR, "fact_actividades_provincia_enriquecida.csv")

# =========================
# 3) CARGAR DATASETS
# =========================
df_ocio = pd.read_csv(OCIO_PATH, dtype=str, encoding="utf-8-sig")
df_rec = pd.read_csv(RECURSOS_PATH, dtype=str, encoding="utf-8-sig")
dim_prov = pd.read_csv(PATH_DIM_PROV, dtype=str, encoding="utf-8-sig")

# Normalizar headers (minúscula + strip)
for df in [df_ocio, df_rec, dim_prov]:
    df.columns = df.columns.str.strip().str.lower()

# =========================
# 4) VALIDACIONES BÁSICAS
# =========================
for req in ["id_pais", "id_provincia"]:
    if req not in df_ocio.columns:
        raise KeyError(f"Falta columna '{req}' en {OCIO_PATH}")
    if req not in df_rec.columns:
        raise KeyError(f"Falta columna '{req}' en {RECURSOS_PATH}")

if "categoria" not in df_ocio.columns:
    raise KeyError("Falta columna 'categoria' en OCIO (debe venir del mapeo).")
if "categoria" not in df_rec.columns:
    raise KeyError("Falta columna 'categoria' en RECURSOS.")

# Padding provincia
df_ocio["id_provincia"] = df_ocio["id_provincia"].astype(str).str.zfill(2)
df_rec["id_provincia"] = df_rec["id_provincia"].astype(str).str.zfill(2)

# =========================
# 5) ENRIQUECER id_ccaa (desde dim_provincia_base)
# =========================
# Esperado: dim_provincia_base tiene id_pais, id_provincia, (idealmente id_ccaa)
# Si NO existe id_ccaa, hacemos fallback con dim_municipio_final

dim_prov_small_cols = [c for c in ["id_pais", "id_provincia", "id_ccaa"] if c in dim_prov.columns]
dim_prov_small = dim_prov[dim_prov_small_cols].drop_duplicates()

if "id_ccaa" not in dim_prov_small.columns:
    print("⚠️ dim_provincia_base NO tiene id_ccaa. Usando fallback con dim_municipio_final...")

    df_muni = pd.read_csv(PATH_DIM_MUNI, dtype=str, encoding="utf-8-sig")
    df_muni.columns = df_muni.columns.str.strip().str.lower()

    # Creamos tabla provincia -> ccaa desde municipios
    df_muni["id_provincia"] = df_muni["id_provincia"].astype(str).str.zfill(2)
    df_muni["id_ccaa"] = df_muni["id_ccaa"].astype(str).str.zfill(2)

    dim_prov_small = (
        df_muni[["id_pais", "id_provincia", "id_ccaa"]]
        .dropna()
        .drop_duplicates()
        .copy()
    )

# Aplicar padding a id_ccaa por seguridad
dim_prov_small["id_ccaa"] = dim_prov_small["id_ccaa"].astype(str).str.zfill(2)

# Merge de id_ccaa a ocio (por provincia)
df_ocio = df_ocio.merge(
    dim_prov_small,
    on=["id_pais", "id_provincia"],
    how="left"
)

print("✅ QA: filas OCIO sin id_ccaa:", df_ocio["id_ccaa"].isna().sum())

# =========================
# 6) UNIR OCIO + RECURSOS (por ids + categoria)
# =========================
cols_rec = [
    "id_pais",
    "id_provincia",
    "categoria",
    "valoracion_por_categoria_promedio",
    "valoracion_general_promedio",
    "total_opiniones_categoria_promedio"
]
cols_rec = [c for c in cols_rec if c in df_rec.columns]

df_act = df_ocio.merge(
    df_rec[cols_rec],
    on=["id_pais", "id_provincia", "categoria"],
    how="left"
)

# flag valoraciones
df_act["hay_valoracion"] = df_act.get("valoracion_general_promedio").notna()

# =========================
# 7) ID ACTIVIDAD (PK)
# =========================
df_act = df_act.reset_index(drop=True)
df_act["id_actividad"] = df_act.index + 1

# =========================
# 8) ORDEN FINAL (SIN LAT/LON)
# =========================
orden = [
    "id_actividad",
    "id_pais",
    "id_ccaa",
    "id_provincia",
    "mes",
    "categoria",
    "producto",
    "subcategoria",
    "comunidad_autonoma",
    "provincia",
    "gasto_total_promedio",
    "precio_medio_entrada_promedio",
    "valoracion_por_categoria_promedio",
    "valoracion_general_promedio",
    "total_opiniones_categoria_promedio",
    "hay_valoracion"
]

orden_existente = [c for c in orden if c in df_act.columns]
resto = [c for c in df_act.columns if c not in orden_existente]
df_act = df_act[orden_existente + resto]


def to_float_auto(x):
    """
    Convierte valores manejando:
    - 1.234,56  → 1234.56
    - 1234.56   → 1234.56
    - 1234      → 1234
    """
    if pd.isna(x):
        return np.nan

    x = str(x).strip()

    if x.lower() in ["nan", "none", "null", "-", "—", ""]:
        return np.nan

    # Caso español con coma decimal
    if "," in x and "." in x:
        x = x.replace(".", "").replace(",", ".")

    # Caso solo coma decimal
    elif "," in x:
        x = x.replace(",", ".")

    return pd.to_numeric(x, errors="coerce")

cols_money = ["gasto_total_promedio", "precio_medio_entrada_promedio"]
for c in cols_money:
    if c in df_act.columns:
        df_act[c] = df_act[c].apply(to_float_auto)

cols_ratings = [
    "valoracion_por_categoria_promedio",
    "valoracion_general_promedio",
    "total_opiniones_categoria_promedio"
]
for c in cols_ratings:
    if c in df_act.columns:
        df_act[c] = df_act[c].apply(to_float_auto)

# 2) Convertir métricas de reputación a numéricas
for c in ["valoracion_por_categoria_promedio", "valoracion_general_promedio"]:
    if c in df_act.columns:
        if df_act[c].max() > 10:
            df_act[c] = df_act[c] / 100

# 3) (Opcional pero recomendado) tipificar mes como int
df_act["mes"] = pd.to_numeric(df_act["mes"], errors="coerce").astype("Int64")

# 4) (Opcional) asegurar boolean real en hay_valoracion
df_act["hay_valoracion"] = df_act["hay_valoracion"].astype(bool)

# 5) (Opcional) redondear
df_act["gasto_total_promedio"] = df_act["gasto_total_promedio"].round(2)
df_act["precio_medio_entrada_promedio"] = df_act["precio_medio_entrada_promedio"].round(2)
df_act["valoracion_por_categoria_promedio"] = df_act["valoracion_por_categoria_promedio"].round(2)
df_act["valoracion_general_promedio"] = df_act["valoracion_general_promedio"].round(2)

# Asegurar que hay_valoracion sea boolean correcto ANTES de usarlo para imputar
df_act["hay_valoracion"] = df_act["hay_valoracion"].astype(str).str.lower().isin(["true", "1", "yes"])

# Columnas numéricas que pueden venir vacías
rating_cols = ["valoracion_por_categoria_promedio", "valoracion_general_promedio"]
opinions_col = "total_opiniones_categoria_promedio"

# Convertir rating cols a numérico (por si quedó algún string raro)
for c in rating_cols:
    if c in df_act.columns:
        df_act[c] = pd.to_numeric(df_act[c], errors="coerce")

if opinions_col in df_act.columns:
    df_act[opinions_col] = pd.to_numeric(df_act[opinions_col], errors="coerce")

# Imputación: cuando NO hay valoración, forzar 0 (evita "" en CSV)
mask_no_val = ~df_act["hay_valoracion"]
for c in rating_cols:
    if c in df_act.columns:
        df_act.loc[mask_no_val, c] = 0

if opinions_col in df_act.columns:
    df_act.loc[mask_no_val, opinions_col] = 0
    # Dejarlo entero (sin .0)
    df_act[opinions_col] = df_act[opinions_col].round(0).astype(int)

df_act["total_opiniones_categoria_promedio"] = (
    df_act["total_opiniones_categoria_promedio"]
    .round(0)
    .astype("Int64")  # permite nulos
)
print(df_act[[
    "gasto_total_promedio",
    "precio_medio_entrada_promedio",
    "valoracion_por_categoria_promedio",
    "valoracion_general_promedio",
    "total_opiniones_categoria_promedio"
]].dtypes)
print(df_act.dtypes)
check_cols = [
    "valoracion_por_categoria_promedio",
    "valoracion_general_promedio",
    "total_opiniones_categoria_promedio",
]
for c in check_cols:
    if c in df_act.columns:
        print(c, "vacíos:", (df_act[c].astype(str).str.strip() == "").sum())
df_act["id_ccaa"] = df_act["id_ccaa"].astype(str).str.zfill(2)
df_act["id_provincia"] = df_act["id_provincia"].astype(str).str.zfill(2)
df_act["id_pais"] = df_act["id_pais"].astype(str).str.upper().str.strip()
# =========================
# 9) GUARDAR
# =========================
df_act.to_csv(OUT_PATH, index=False, encoding="utf-8-sig")
print("✅ Guardado:", OUT_PATH)
print("Shape final:", df_act.shape)
print("✅ Nulos id_ccaa:", df_act["id_ccaa"].isna().sum() if "id_ccaa" in df_act.columns else "no existe")
