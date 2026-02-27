import os
import pandas as pd
from pathlib import Path

# =========================
# 1) RUTAS DEL PROYECTO
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

path_islas = os.path.join(RAW_DIR, "25codislas.xlsx")
path_dim_base = os.path.join(INTERIM_DIR, "dim_municipio_base.csv")

ID_PAIS = "ES"

# =========================
# 2) FUNCIÓN PARA LIMPIAR CADA HOJA
# =========================
def parse_islas_sheet(raw_sheet: pd.DataFrame, sheet_name: str) -> pd.DataFrame:
    provincia = str(raw_sheet.iloc[1, 0]).strip()

    header_row = 2
    headers = raw_sheet.iloc[header_row].astype(str).str.strip().str.lower().tolist()

    df = raw_sheet.iloc[header_row + 1:].copy()
    df.columns = headers

    df = df.dropna(how="all").reset_index(drop=True)

    required = {"cpro", "cisla", "isla", "cmun", "dc", "nombre"}
    if not required.issubset(set(df.columns)):
        return pd.DataFrame()

    df["cpro"] = df["cpro"].astype(str).str.zfill(2)
    df["cmun"] = df["cmun"].astype(str).str.zfill(3)
    df["dc"] = df["dc"].astype(str).str.zfill(1)
    df["cisla"] = df["cisla"].astype(str).str.zfill(3)

    df["provincia_header"] = provincia
    df["sheet"] = str(sheet_name)
    return df

# =========================
# 3) LEER TODAS LAS HOJAS Y UNIR
# =========================
sheets = pd.read_excel(path_islas, sheet_name=None, header=None)

dfs = []
for sheet_name, raw_sheet in sheets.items():
    if not str(sheet_name).isdigit():
        continue
    df_sheet = parse_islas_sheet(raw_sheet, sheet_name)
    if not df_sheet.empty:
        dfs.append(df_sheet)

df_islas = pd.concat(dfs, ignore_index=True)

print("✅ 25codislas unificado:", df_islas.shape)

# =========================
# 4) CREAR IDS
# =========================
df_islas["id_municipio_parcial"] = df_islas["cpro"] + df_islas["cmun"] + df_islas["dc"]
df_islas["id_isla"] = df_islas["cpro"] + df_islas["cisla"]

# =========================
# 5) MERGE CON DIM MUNICIPIO BASE
# =========================
df_base = pd.read_csv(path_dim_base, dtype=str)
df_base["id_municipio"] = df_base["id_municipio"].astype(str).str.zfill(8)
df_base["id_municipio_parcial"] = df_base["id_municipio_parcial"].astype(str).str.zfill(6)

df_islas = df_islas.merge(
    df_base[["id_municipio", "id_municipio_parcial", "id_provincia", "id_ccaa"]],
    on="id_municipio_parcial",
    how="left"
)

# =========================
# 6) QA
# =========================
df_islas_no_match = df_islas[df_islas["id_municipio"].isna()].copy()
print("Islas sin match en dim_municipio_base:", df_islas_no_match.shape[0])

# =========================
# 7) OUTPUTS (ACTUALIZADOS)
# =========================

# A) Relación municipio ↔ isla (tabla puente)
df_municipio_isla = df_islas[[
    "id_municipio",
    "id_municipio_parcial",
    "id_isla",
    "isla"
]].drop_duplicates().copy()

# Agregar id_pais + gid (opcional)
df_municipio_isla.insert(0, "id_pais", ID_PAIS)
df_municipio_isla["gid_isla"] = df_municipio_isla["id_pais"] + "-" + df_municipio_isla["id_isla"]
df_municipio_isla["gid_municipio"] = df_municipio_isla["id_pais"] + "-" + df_municipio_isla["id_municipio"]

# B) Dimensión de islas (dim_isla)
df_dim_isla = df_islas[[
    "id_isla",
    "isla",
    "cpro",
    "provincia_header"
]].drop_duplicates().copy()

# Estandarizar nombre: cpro -> id_provincia
df_dim_isla = df_dim_isla.rename(columns={"cpro": "id_provincia"})

# Agregar id_pais + gid
df_dim_isla.insert(0, "id_pais", ID_PAIS)
df_dim_isla["gid_isla"] = df_dim_isla["id_pais"] + "-" + df_dim_isla["id_isla"]

print(df_dim_isla.dtypes)

# =========================
# 8) GUARDAR
# =========================
out_rel = os.path.join(CLEAN_DIR, "rel_municipio_isla.csv")
out_dim_isla = os.path.join(CLEAN_DIR, "dim_isla.csv")
out_qa = os.path.join(OUTPUTS_DIR, "qa_25codislas_no_en_base.csv")

df_municipio_isla.to_csv(out_rel, index=False, encoding="utf-8-sig")
df_dim_isla.to_csv(out_dim_isla, index=False, encoding="utf-8-sig")
df_islas_no_match.to_csv(out_qa, index=False, encoding="utf-8-sig")

print("\n✅ Guardado actualizado:")
print(" -", out_rel)
print(" -", out_dim_isla)
print(" -", out_qa)
