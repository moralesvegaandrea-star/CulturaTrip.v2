import os
import pandas as pd
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parents[2]

RAW_DIR = BASE_DIR / "data" / "raw"
INTERIM_DIR = BASE_DIR / "data" / "interim"   # ✅ NUEVO
CLEAN_DIR = BASE_DIR / "data" / "clean"
OUTPUTS_DIR = BASE_DIR / "outputs"

RAW_DIR.mkdir(parents=True, exist_ok=True)
INTERIM_DIR.mkdir(parents=True, exist_ok=True)  # ✅ NUEVO
CLEAN_DIR.mkdir(parents=True, exist_ok=True)
OUTPUTS_DIR.mkdir(parents=True, exist_ok=True)

ID_PAIS = "ES"

path_muni = os.path.join(INTERIM_DIR, "dim_municipio_base.csv")
path_prov = os.path.join(CLEAN_DIR, "dim_provincia_base.csv")
path_ccaa = os.path.join(CLEAN_DIR, "dim_ccaa_base.csv")
path_rel_isla = os.path.join(CLEAN_DIR, "rel_municipio_isla.csv")

df_muni = pd.read_csv(path_muni, dtype=str)
df_prov = pd.read_csv(path_prov, dtype=str)
df_ccaa = pd.read_csv(path_ccaa, dtype=str)
df_rel = pd.read_csv(path_rel_isla, dtype=str)

# =========================
# 1) ASEGURAR id_pais EN TODOS (POR SI FALTA)
# =========================
for df in [df_muni, df_prov, df_ccaa, df_rel]:
    df.columns = df.columns.str.strip()

if "id_pais" not in df_muni.columns:
    df_muni.insert(0, "id_pais", ID_PAIS)
else:
    df_muni["id_pais"] = ID_PAIS

if "id_pais" not in df_prov.columns:
    df_prov.insert(0, "id_pais", ID_PAIS)
else:
    df_prov["id_pais"] = ID_PAIS

if "id_pais" not in df_ccaa.columns:
    df_ccaa.insert(0, "id_pais", ID_PAIS)
else:
    df_ccaa["id_pais"] = ID_PAIS

if "id_pais" not in df_rel.columns:
    df_rel.insert(0, "id_pais", ID_PAIS)
else:
    df_rel["id_pais"] = ID_PAIS

# =========================
# 2) PADDING DE IDS (SEGURIDAD)
# =========================
df_muni["id_provincia"] = df_muni["id_provincia"].astype(str).str.zfill(2)
df_muni["id_ccaa"] = df_muni["id_ccaa"].astype(str).str.zfill(2)

df_prov["id_provincia"] = df_prov["id_provincia"].astype(str).str.zfill(2)
df_ccaa["id_ccaa"] = df_ccaa["id_ccaa"].astype(str).str.zfill(2)

# =========================
# 3) MERGES CON CLAVES COHERENTES (id_pais + id)
# =========================
# Provincia: trae provincia_nombre
cols_prov = [c for c in ["id_pais", "id_provincia", "provincia_nombre"] if c in df_prov.columns]
df_muni = df_muni.merge(df_prov[cols_prov].drop_duplicates(), on=["id_pais", "id_provincia"], how="left")

# CCAA: trae ccaa_nombre
cols_ccaa = [c for c in ["id_pais", "id_ccaa", "ccaa_nombre"] if c in df_ccaa.columns]
df_muni = df_muni.merge(df_ccaa[cols_ccaa].drop_duplicates(), on=["id_pais", "id_ccaa"], how="left")

# Islas: trae id_isla e isla (opcional)
# (si rel tiene id_pais, mejor usarlo también)
rel_cols = [c for c in ["id_pais", "id_municipio", "id_isla", "isla"] if c in df_rel.columns]
df_rel_small = df_rel[rel_cols].drop_duplicates()

on_keys = ["id_municipio"]
if "id_pais" in df_rel_small.columns and "id_pais" in df_muni.columns:
    on_keys = ["id_pais", "id_municipio"]

df_muni = df_muni.merge(df_rel_small, on=on_keys, how="left")

# =========================
# 4) AGREGAR gid_municipio (RECOMENDADO)
# =========================
df_muni["gid_municipio"] = df_muni["id_pais"] + "-" + df_muni["id_municipio"].astype(str)

# (Opcional) gid_provincia y gid_ccaa en municipio (útil para joins)
df_muni["gid_provincia"] = df_muni["id_pais"] + "-" + df_muni["id_provincia"].astype(str)
df_muni["gid_ccaa"] = df_muni["id_pais"] + "-" + df_muni["id_ccaa"].astype(str)

# =========================
# 5) QA BÁSICO
# =========================
print("Nulos provincia_nombre:", df_muni["provincia_nombre"].isna().sum() if "provincia_nombre" in df_muni.columns else "col no existe")
print("Nulos ccaa_nombre:", df_muni["ccaa_nombre"].isna().sum() if "ccaa_nombre" in df_muni.columns else "col no existe")
print("Municipios con isla:", df_muni["id_isla"].notna().sum() if "id_isla" in df_muni.columns else "col no existe")

# Provincias con islas (España)
provincias_islas = ["07", "35", "38"]
df_islas_check = df_muni[df_muni["id_provincia"].isin(provincias_islas)]

print("Total municipios en provincias con islas:", df_islas_check.shape[0])
print("Municipios con isla:", df_islas_check["id_isla"].notna().sum() if "id_isla" in df_islas_check.columns else "col no existe")
print("Municipios sin isla:", df_islas_check["id_isla"].isna().sum() if "id_isla" in df_islas_check.columns else "col no existe")
print(df_rel.dtypes)
print(df_muni.dtypes)
# =========================
# 6) GUARDAR
# =========================
out_path = os.path.join(CLEAN_DIR, "dim_municipio_final.csv")
df_muni.to_csv(out_path, index=False, encoding="utf-8-sig")
print("✅ Guardado:", out_path)
