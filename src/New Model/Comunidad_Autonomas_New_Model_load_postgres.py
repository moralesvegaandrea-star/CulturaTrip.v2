import os
import pandas as pd
from sqlalchemy import create_engine, text
from pathlib import Path

# =========================
# 1) Leer snapshot (SIN API)
# =========================
BASE_DIR = Path(__file__).resolve().parents[2]
path_in = BASE_DIR / "data" / "clean" / "dim_ccaa_base.csv"  # ajusta el nombre si es otro

df = pd.read_csv(path_in, dtype=str)

# =========================
# 2) Normalizar columnas según CREATE TABLE
# =========================
df = df[["id_ccaa", "id_pais", "gid_ccaa", "ccaa_nombre"]].copy()

df["id_ccaa"] = df["id_ccaa"].astype(str).str.strip().str.upper().str[:2]
df["id_pais"] = df["id_pais"].astype(str).str.strip().str.upper().str[:2]

df["gid_ccaa"] = df["gid_ccaa"].astype(str).str.strip()
df.loc[df["gid_ccaa"].isin(["nan", "None", "NONE"]), "gid_ccaa"] = None
df["gid_ccaa"] = df["gid_ccaa"].astype("string").str[:5]

df["ccaa_nombre"] = df["ccaa_nombre"].astype(str).str.strip().str[:40]
df = df[df["ccaa_nombre"] != ""].copy()

# Eliminar duplicados que rompan constraints
df = df.drop_duplicates(subset=["id_ccaa"]).copy()
df = df.drop_duplicates(subset=["id_pais", "ccaa_nombre"]).copy()

print("✅ Snapshot listo:", df.shape)

# =========================
# 3) Conexión Postgres (Docker 5433)
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
# 4) TRUNCATE + LOAD
# =========================
with engine.begin() as conn:
    conn.execute(text("TRUNCATE TABLE culturatrip.dim_ccaa_base CASCADE;"))

    df.to_sql(
        "dim_ccaa_base",
        conn,
        schema="culturatrip",
        if_exists="append",
        index=False,
        method="multi",
        chunksize=1000
    )

print(f"🎉 Cargadas {len(df)} filas en culturatrip.dim_ccaa_base")
