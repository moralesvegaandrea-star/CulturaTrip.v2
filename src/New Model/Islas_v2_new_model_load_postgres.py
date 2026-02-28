import os
import pandas as pd
from pathlib import Path
from sqlalchemy import create_engine, text

# =========================
# 1) RUTAS
# =========================
BASE_DIR = Path(__file__).resolve().parents[2]
path_in = BASE_DIR / "data" / "clean" / "dim_isla.csv"  # ajusta si el nombre exacto cambia

# =========================
# 2) CARGAR
# =========================
df = pd.read_csv(path_in, dtype=str)

# Ajusta a las columnas esperadas por la tabla
df = df[["id_pais", "id_isla", "id_provincia", "gid_isla", "isla", "provincia_header"]].copy()

# =========================
# 3) NORMALIZAR (según CREATE TABLE)
# =========================
df["id_pais"] = df["id_pais"].astype(str).str.strip().str.upper().str[:2]

# id_provincia CHAR(2) -> padding si viene "1" -> "01"
df["id_provincia"] = (
    pd.to_numeric(df["id_provincia"], errors="ignore")
    .astype(str)
    .str.strip()
)
df["id_provincia"] = df["id_provincia"].apply(lambda x: x.zfill(2) if x.isdigit() else x[:2])

# id_isla CHAR(5) -> limpiar y cortar a 5
df["id_isla"] = df["id_isla"].astype(str).str.strip().str.upper().str[:5]

# gid_isla VARCHAR(8)
df["gid_isla"] = df["gid_isla"].astype(str).str.strip()
df.loc[df["gid_isla"].isin(["nan", "None", "NONE"]), "gid_isla"] = None
df["gid_isla"] = df["gid_isla"].astype("string").str[:8]

# isla VARCHAR(20)
df["isla"] = df["isla"].astype(str).str.strip()
df.loc[df["isla"].isin(["nan", "None", "NONE"]), "isla"] = None
df["isla"] = df["isla"].astype("string").str[:20]

# provincia_header VARCHAR(30)
df["provincia_header"] = df["provincia_header"].astype(str).str.strip()
df.loc[df["provincia_header"].isin(["nan", "None", "NONE"]), "provincia_header"] = None
df["provincia_header"] = df["provincia_header"].astype("string").str[:30]

# Quitar registros sin PK
df = df[df["id_isla"].notna() & (df["id_isla"] != "")].copy()

# Evitar duplicados de PK
df = df.drop_duplicates(subset=["id_isla"]).reset_index(drop=True)

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
    conn.execute(text("TRUNCATE TABLE culturatrip.dim_isla CASCADE;"))

    df.to_sql(
        "dim_isla",
        conn,
        schema="culturatrip",
        if_exists="append",
        index=False,
        method="multi",
        chunksize=1000
    )

print(f"🎉 Cargadas {len(df)} islas en culturatrip.dim_isla")