import os
import pandas as pd
from pathlib import Path
from sqlalchemy import create_engine, text

# =========================
# 1) RUTAS
# =========================
BASE_DIR = Path(__file__).resolve().parents[2]
path_in = BASE_DIR / "data" / "clean" / "rel_municipio_isla.csv"

# =========================
# 2) CARGAR
# =========================
df = pd.read_csv(path_in, dtype=str)
original_cols = set(df.columns)

# Columnas esperadas según CREATE TABLE
cols = [
    "id_pais",
    "id_municipio",
    "id_municipio_parcial",
    "id_isla",
    "gid_isla",
    "isla",
    "gid_municipio",
]

# Crear faltantes como NULL si el CSV no las trae (tolerante)
for c in cols:
    if c not in df.columns:
        df[c] = None

df = df[cols].copy()

faltantes = [c for c in cols if c not in original_cols]
print("✅ Columnas faltantes creadas como NULL:", faltantes)

# =========================
# 3) NORMALIZACIÓN
# =========================
df["id_pais"] = df["id_pais"].astype(str).str.strip().str.upper().str[:2]

df["id_municipio"] = df["id_municipio"].astype(str).str.strip().str[:8]

df["id_municipio_parcial"] = df["id_municipio_parcial"].astype(str).str.strip()
df.loc[df["id_municipio_parcial"].isin(["", "nan", "None", "NONE", "<NA>"]), "id_municipio_parcial"] = None
df["id_municipio_parcial"] = df["id_municipio_parcial"].astype("string").str[:6]

df["id_isla"] = df["id_isla"].astype(str).str.strip().str[:5]

df["gid_isla"] = df["gid_isla"].astype(str).str.strip()
df.loc[df["gid_isla"].isin(["", "nan", "None", "NONE", "<NA>"]), "gid_isla"] = None
df["gid_isla"] = df["gid_isla"].astype("string").str[:8]

df["isla"] = df["isla"].astype(str).str.strip()
df.loc[df["isla"].isin(["", "nan", "None", "NONE", "<NA>"]), "isla"] = None
df["isla"] = df["isla"].astype("string").str[:20]

df["gid_municipio"] = df["gid_municipio"].astype(str).str.strip()
df.loc[df["gid_municipio"].isin(["", "nan", "None", "NONE", "<NA>"]), "gid_municipio"] = None
df["gid_municipio"] = df["gid_municipio"].astype("string").str[:11]

# Quitar filas sin campos obligatorios (PK/FK)
df = df[(df["id_pais"] != "") & df["id_pais"].notna()].copy()
df = df[(df["id_municipio"] != "") & df["id_municipio"].notna()].copy()
df = df[(df["id_isla"] != "") & df["id_isla"].notna()].copy()

# Eliminar duplicados por PK compuesta
df = df.drop_duplicates(subset=["id_municipio", "id_isla"]).reset_index(drop=True)

print("✅ Snapshot listo:", df.shape)

# =========================
# 4) CONEXIÓN POSTGRES (Docker)
# =========================
engine = create_engine(
    f"postgresql+psycopg2://"
    f"{os.getenv('DB_USER','culturatrip')}:"
    f"{os.getenv('DB_PASSWORD','culturatrip')}@"
    f"{os.getenv('DB_HOST','localhost')}:"
    f"{os.getenv('DB_PORT','5433')}/"
    f"{os.getenv('DB_NAME','culturatrip')}",
    pool_pre_ping=True
)

# =========================
# 5) TRUNCATE + LOAD
# =========================
with engine.begin() as conn:
    conn.execute(text("TRUNCATE TABLE culturatrip.rel_municipio_isla CASCADE;"))

    df.to_sql(
        "rel_municipio_isla",
        conn,
        schema="culturatrip",
        if_exists="append",
        index=False,
        method="multi",
        chunksize=2000
    )

print(f"🎉 Cargadas {len(df)} relaciones municipio-isla")