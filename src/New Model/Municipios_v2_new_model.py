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

path_codmun = os.path.join(RAW_DIR, "25codmun.xlsx")
path_dim_base = os.path.join(INTERIM_DIR, "dim_municipio_base.csv")  # del Sprint 1

# =========================
# 2) FUNCIÓN PARA LIMPIAR CADA HOJA
# =========================
def parse_sheet(raw_sheet: pd.DataFrame, sheet_name: str) -> pd.DataFrame:
    """
    - provincia está en A2 -> fila 1, col 0
    - headers reales en fila 3 -> index 2
    - datos empiezan en fila 4 -> index 3
    """
    # Provincia en A2
    provincia = str(raw_sheet.iloc[1, 0]).strip()

    # Headers en fila 3 (index 2)
    header_row = 2
    headers = raw_sheet.iloc[header_row].astype(str).str.strip().str.lower().tolist()

    # Datos desde fila 4
    df = raw_sheet.iloc[header_row + 1:].copy()
    df.columns = headers

    # Quitar filas vacías
    df = df.dropna(how="all").reset_index(drop=True)

    # Asegurar que existan columnas clave
    required = {"cpro", "cmun", "dc"}
    if not required.issubset(set(df.columns)):
        # Si faltan columnas, devolvemos vacío para no romper
        return pd.DataFrame()

    # Normalizar códigos (padding correcto)
    df["cpro"] = df["cpro"].astype(str).str.zfill(2)
    df["cmun"] = df["cmun"].astype(str).str.zfill(3)
    df["dc"] = df["dc"].astype(str).str.zfill(1)

    # Columna provincia desde el header
    df["provincia_header"] = provincia
    df["sheet"] = str(sheet_name)

    return df

# =========================
# 3) LEER TODAS LAS HOJAS Y UNIR
# =========================
sheets = pd.read_excel(path_codmun, sheet_name=None, header=None)

dfs = []
for sheet_name, raw_sheet in sheets.items():
    # Solo hojas con nombre numérico (por si hay hojas tipo "README")
    if not str(sheet_name).isdigit():
        continue

    df_sheet = parse_sheet(raw_sheet, sheet_name)
    if not df_sheet.empty:
        dfs.append(df_sheet)

df_codmun = pd.concat(dfs, ignore_index=True)

print("✅ 25codmun unificado:", df_codmun.shape)
print(df_codmun.head(5))

# =========================
# 4) CARGAR DIM BASE (SPRINT 1) Y PREPARAR PARA COMPARAR
# =========================
df_base = pd.read_csv(path_dim_base, dtype=str)

# Asegurar padding en base
df_base["id_municipio"] = df_base["id_municipio"].astype(str).str.zfill(8)
df_base["id_ccaa"] = df_base["id_ccaa"].astype(str).str.zfill(2)
df_base["id_provincia"] = df_base["id_provincia"].astype(str).str.zfill(2)

# Reconstruir cmun y dc desde id_municipio (para poder hacer merge)
df_base["cpro"] = df_base["id_municipio"].str[2:4]
df_base["cmun"] = df_base["id_municipio"].str[4:7]
df_base["dc"]   = df_base["id_municipio"].str[7:8]

# =========================
# 5) ENRIQUECER 25codmun CON id_ccaa e id_municipio (usando la base)
# =========================
# Hacemos merge por cpro+cmun+dc
df_codmun = df_codmun.merge(
    df_base[["id_municipio", "id_ccaa", "id_provincia", "cpro", "cmun", "dc"]],
    on=["cpro", "cmun", "dc"],
    how="left",
    suffixes=("", "_base")
)

# =========================
# 6) REPORTES DE VALIDACIÓN (QA)
# =========================

# (A) Registros en 25codmun que NO aparecen en la base
df_codmun_no_match = df_codmun[df_codmun["id_municipio"].isna()].copy()

# (B) Municipios en la base que NO aparecen en 25codmun
# Para esto, comparamos por (cpro,cmun,dc) usando un set
base_keys = set(zip(df_base["cpro"], df_base["cmun"], df_base["dc"]))
codmun_keys = set(zip(df_codmun["cpro"], df_codmun["cmun"], df_codmun["dc"]))

missing_in_codmun = base_keys - codmun_keys

df_missing_in_codmun = pd.DataFrame(
    list(missing_in_codmun),
    columns=["cpro", "cmun", "dc"]
).merge(
    df_base[["id_municipio", "id_ccaa", "id_provincia", "cpro", "cmun", "dc", "nombre"]],
    on=["cpro", "cmun", "dc"],
    how="left"
)
print(df_codmun.dtypes)
# =========================
# 7) GUARDAR OUTPUTS
# =========================
# Guardar 25codmun limpio/enriquecido
out_codmun_clean = os.path.join(INTERIM_DIR, "codmun_25_limpio.csv")
df_codmun.to_csv(out_codmun_clean, index=False, encoding="utf-8-sig")

# Reporte QA: filas de 25codmun sin match en base
out_no_match = os.path.join(OUTPUTS_DIR, "qa_25codmun_no_en_base.csv")
df_codmun_no_match.to_csv(out_no_match, index=False, encoding="utf-8-sig")

# Reporte QA: filas en base que faltan en 25codmun
out_missing = os.path.join(OUTPUTS_DIR, "qa_base_no_en_25codmun.csv")
df_missing_in_codmun.to_csv(out_missing, index=False, encoding="utf-8-sig")

print("\n✅ Guardado:")
print(" -", out_codmun_clean)
print(" -", out_no_match, "(25codmun sin match en base)")
print(" -", out_missing, "(base sin aparecer en 25codmun)")

print("\nResumen QA:")
print("25codmun sin match en base:", df_codmun_no_match.shape[0])
print("base sin aparecer en 25codmun:", df_missing_in_codmun.shape[0])
