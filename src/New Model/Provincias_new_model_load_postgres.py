import os
import pandas as pd
from pathlib import Path
from sqlalchemy import create_engine, text

# =========================
# 1) RUTAS
# =========================
BASE_DIR = Path(__file__).resolve().parents[2]
path_in = BASE_DIR / "data" / "clean" / "dim_provincia_base.csv"

# =========================
# 2) CARGAR CSV
# =========================
df = pd.read_csv(path_in, dtype=str)

# Asegurar columnas esperadas (ajusta si tu CSV tiene otro orden)
df = df[["id_pais", "id_provincia", "gid_provincia", "provincia_nombre"]].copy()

# =========================
# 3) NORMALIZAR SEGÚN CREATE TABLE
# =========================
df["id_pais"] = df["id_pais"].astype(str).str.strip().str.upper().str[:2]

# id_provincia CHAR(2) -> convertir a 2 dígitos si viene como número
df["id_provincia"] = (
    pd.to_numeric(df["id_provincia"], errors="coerce")
      .astype("Int64")
      .astype(str)
      .str.replace("<NA>", "", regex=False)
)
df["id_provincia"] = df["id_provincia"].apply(lambda x: x.zfill(2) if x.isdigit() else x[:2])
df = df[df["id_provincia"] != ""].copy()

# gid_provincia VARCHAR(5)
df["gid_provincia"] = df["gid_provincia"].astype(str).str.strip()
df.loc[df["gid_provincia"].isin(["nan", "None", "NONE"]), "gid_provincia"] = None
df["gid_provincia"] = df["gid_provincia"].astype("string").str[:5]

# provincia_nombre VARCHAR(30) NOT NULL
df["provincia_nombre"] = df["provincia_nombre"].astype(str).str.strip().str[:30]
df = df[df["provincia_nombre"] != ""].copy()

# Evitar conflictos por constraints
df = df.drop_duplicates(subset=["id_provincia"]).copy()
df = df.drop_duplicates(subset=["id_pais", "provincia_nombre"]).copy()

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
    conn.execute(text("TRUNCATE TABLE culturatrip.dim_provincia CASCADE;"))

    df.to_sql(
        "dim_provincia",
        conn,
        schema="culturatrip",
        if_exists="append",
        index=False,
        method="multi",
        chunksize=1000
    )

print(f"🎉 Cargadas {len(df)} provincias en culturatrip.dim_provincia")