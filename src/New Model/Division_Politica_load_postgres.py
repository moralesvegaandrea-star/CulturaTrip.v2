import os
import pandas as pd
from pathlib import Path
from sqlalchemy import create_engine, text

# =========================
# 1) RUTAS
# =========================
BASE_DIR = Path(__file__).resolve().parents[2]
path_in = BASE_DIR / "data" / "clean" / "dim_municipio_final.csv"

# =========================
# 2) CARGAR
# =========================
df = pd.read_csv(path_in, dtype=str)

# Columnas exactas esperadas (según tu CREATE TABLE)
cols = [
    "id_pais",
    "id_municipio",
    "id_municipio_parcial",
    "id_provincia",
    "id_ccaa",
    "id_isla",
    "nombre",
    "provincia_nombre",
    "ccaa_nombre",
    "isla",
    "gid_municipio",
    "gid_provincia",
    "gid_ccaa",
]
df = df[cols].copy()

# =========================
# 3) NORMALIZACIÓN
# =========================
df["id_pais"] = df["id_pais"].astype(str).str.strip().str.upper().str[:2]

# ids con padding cuando sean dígitos
df["id_provincia"] = df["id_provincia"].astype(str).str.strip()
df["id_provincia"] = df["id_provincia"].apply(lambda x: x.zfill(2) if x.isdigit() else x[:2])

df["id_ccaa"] = df["id_ccaa"].astype(str).str.strip()
df["id_ccaa"] = df["id_ccaa"].apply(lambda x: x.zfill(2) if x.isdigit() else x[:2])

# id_municipio CHAR(8) (texto)
df["id_municipio"] = df["id_municipio"].astype(str).str.strip().str[:8]
df["id_municipio_parcial"] = df["id_municipio_parcial"].astype(str).str.strip().str[:6]

# id_isla CHAR(5) nullable
df["id_isla"] = df["id_isla"].astype(str).str.strip()
df.loc[df["id_isla"].isin(["", "nan", "None", "NONE", "<NA>"]), "id_isla"] = None
df["id_isla"] = df["id_isla"].astype("string").str[:5]

# textos
df["nombre"] = df["nombre"].astype(str).str.strip().str[:60]
df["provincia_nombre"] = df["provincia_nombre"].astype(str).str.strip().str[:30]
df["ccaa_nombre"] = df["ccaa_nombre"].astype(str).str.strip().str[:40]

df["isla"] = df["isla"].astype(str).str.strip()
df.loc[df["isla"].isin(["", "nan", "None", "NONE", "<NA>"]), "isla"] = None
df["isla"] = df["isla"].astype("string").str[:30]

# gids (respetar longitudes)
df["gid_municipio"] = df["gid_municipio"].astype(str).str.strip().str[:20]
df["gid_provincia"] = df["gid_provincia"].astype(str).str.strip().str[:8]
df["gid_ccaa"] = df["gid_ccaa"].astype(str).str.strip().str[:8]


# Quitar filas sin PK o campos NOT NULL esenciales
df = df[df["id_municipio"].notna() & (df["id_municipio"] != "")].copy()
df = df[df["nombre"].notna() & (df["nombre"] != "")].copy()

# Evitar duplicados por PK
df = df.drop_duplicates(subset=["id_municipio"]).reset_index(drop=True)

print("✅ Snapshot listo:", df.shape)

# =========================
# 4) CONEXIÓN POSTGRES (Docker 5433)
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
    conn.execute(text("TRUNCATE TABLE culturatrip.dim_municipio CASCADE;"))

    df.to_sql(
        "dim_municipio",
        conn,
        schema="culturatrip",
        if_exists="append",
        index=False,
        method="multi",
        chunksize=2000
    )

print(f"🎉 Cargados {len(df)} municipios en culturatrip.dim_municipio")
