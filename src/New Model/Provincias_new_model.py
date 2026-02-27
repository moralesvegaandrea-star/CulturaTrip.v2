import os
import pandas as pd
from pathlib import Path

# =========================
# 0) HELPER: limpiar headers (BOM + espacios)
# =========================
def clean_headers(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df.columns = (
        df.columns.astype(str)
        .str.replace("\ufeff", "", regex=False)  # BOM invisible
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

path_codmun = os.path.join(INTERIM_DIR, "codmun_25_limpio.csv")
path_dim_isla = os.path.join(CLEAN_DIR, "dim_isla.csv")

# =========================
# 2) CARGAR DATASETS
# =========================
df_codmun = pd.read_csv(path_codmun, dtype=str, encoding="utf-8-sig")
df_isla = pd.read_csv(path_dim_isla, dtype=str, encoding="utf-8-sig")

df_codmun = clean_headers(df_codmun)
df_isla = clean_headers(df_isla)

print("📌 Columnas codmun:", list(df_codmun.columns))
print("📌 Columnas dim_isla:", list(df_isla.columns))

# =========================
# 3) CREAR dim_provincia_base DESDE codmun
# =========================
# codmun debe traer: cpro + provincia_header
need_codmun = {"cpro", "provincia_header"}
missing_codmun = need_codmun - set(df_codmun.columns)
if missing_codmun:
    raise KeyError(f"codmun_25_limpio.csv no trae columnas requeridas: {missing_codmun}")

df_prov = df_codmun[["cpro", "provincia_header"]].drop_duplicates().copy()
df_prov = df_prov.rename(columns={
    "cpro": "id_provincia",
    "provincia_header": "provincia_nombre"
})

# padding + limpieza
df_prov["id_provincia"] = df_prov["id_provincia"].astype(str).str.strip().str.zfill(2)
df_prov["provincia_nombre"] = df_prov["provincia_nombre"].astype(str).str.strip().str.lower()
# =========================
# 4) FIX CEUTA / MELILLA (NOMBRES)
# =========================
map_prov_especial = {
    "ciudad autonoma de ceuta": "ceuta",
    "ciudad autónoma de ceuta": "ceuta",
    "ceuta, ciudad autonoma de": "ceuta",
    "ceuta, ciudad autónoma de": "ceuta",
    "ciudad autonoma de melilla": "melilla",
    "ciudad autónoma de melilla": "melilla",
    "melilla, ciudad autonoma de": "melilla",
    "melilla, ciudad autónoma de": "melilla",
}
df_prov["provincia_nombre"] = df_prov["provincia_nombre"].replace(map_prov_especial)

# quitar duplicados por id_provincia
df_prov = df_prov.drop_duplicates(subset=["id_provincia"]).reset_index(drop=True)

# =========================
# 5) AGREGAR id_pais + gid_provincia
# =========================
df_prov.insert(0, "id_pais", "ES")
df_prov["gid_provincia"] = df_prov["id_pais"] + "-" + df_prov["id_provincia"]
print(df_prov.dtypes)

# =========================
# 6) GUARDAR dim_provincia_base
# =========================
out_dim_prov = os.path.join(CLEAN_DIR, "dim_provincia_base.csv")
df_prov.to_csv(out_dim_prov, index=False, encoding="utf-8-sig")
print("✅ Guardado:", out_dim_prov)

# =========================
# 7) VALIDACIÓN: ISLAS → PROVINCIAS (STRING)
# =========================

# Normalizar id_provincia desde dim_isla
if "cpro" in df_isla.columns:
    df_isla["id_provincia"] = (
        df_isla["cpro"]
        .astype(str)
        .str.strip()
        .str.zfill(2)
    )

elif "id_isla" in df_isla.columns:
    df_isla["id_provincia"] = (
        df_isla["id_isla"]
        .astype(str)
        .str.strip()
        .str[:2]      # primeros 2 dígitos
        .str.zfill(2)
    )

else:
    raise KeyError("dim_isla debe tener 'cpro' o 'id_isla' para validar provincias.")

# Normalizar id_provincia en dim_prov
df_prov["id_provincia"] = (
    df_prov["id_provincia"]
    .astype(str)
    .str.strip()
    .str.zfill(2)
)

# Conjuntos para comparación
prov_islas = set(df_isla["id_provincia"].dropna().unique())
prov_base  = set(df_prov["id_provincia"].dropna().unique())

faltantes = sorted(list(prov_islas - prov_base))

print("\n--- VALIDACIÓN ISLAS → PROVINCIAS ---")
print("Provincias detectadas desde dim_isla:", sorted(list(prov_islas)))

if len(faltantes) == 0:
    print("✅ OK: Todas las provincias de dim_isla están dentro de dim_provincia_base.")
else:
    print("❌ ALERTA: Provincias en dim_isla pero NO en dim_provincia_base:", faltantes)
# =========================
# 8) CHECK PROVINCIAS CLAVE (07,35,38)
# =========================

must_have = ["07", "35", "38"]

for p in must_have:
    print(f"Provincia {p}: {'✅ presente' if p in prov_base else '❌ NO está'}")

# =========================
# 9) CHECK CEUTA / MELILLA (51,52)
# =========================
print("\n--- CHECK CEUTA / MELILLA (51,52) ---")
print(df_prov[df_prov["id_provincia"].isin(["51", "52"])][["id_provincia", "provincia_nombre"]])
