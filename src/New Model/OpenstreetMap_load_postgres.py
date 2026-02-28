import os
import pandas as pd
from pathlib import Path
from sqlalchemy import create_engine, text

# =========================
# 1) RUTAS
# =========================
BASE_DIR = Path(__file__).resolve().parents[2]
path_in = BASE_DIR / "data" / "clean" / "dim_geografia_municipio_osm.csv"

# =========================
# 2) CARGAR
# =========================
df = pd.read_csv(path_in, dtype=str)
df.columns = df.columns.str.strip()

# Columnas esperadas (según CREATE TABLE)
cols = [
    "id_municipio",
    "osm_id",
    "osm_type",
    "osm_query_usada",
    "osm_pass",
    "lat",
    "lon",
]

# Crear faltantes si no vienen (por si acaso)
for c in cols:
    if c not in df.columns:
        df[c] = None

df = df[cols].copy()

print("✅ Snapshot inicial:", df.shape)

# =========================
# 3) NORMALIZACIÓN / TIPOS
# =========================
df["id_municipio"] = df["id_municipio"].astype(str).str.strip().str[:8]
df["osm_id"] = df["osm_id"].astype(str).str.strip().str[:15]
df["osm_type"] = df["osm_type"].astype(str).str.strip().str[:10]
df["osm_query_usada"] = df["osm_query_usada"].astype(str).str.strip().str[:150]
df["osm_pass"] = df["osm_pass"].astype(str).str.strip().str[:150]

# lat/lon a numérico
df["lat"] = pd.to_numeric(df["lat"], errors="coerce")
df["lon"] = pd.to_numeric(df["lon"], errors="coerce")

# Filtrar obligatorios (NOT NULL)
df = df[df["id_municipio"].notna() & (df["id_municipio"] != "")]
df = df[df["osm_id"].notna() & (df["osm_id"] != "")]
df = df[df["osm_type"].notna() & (df["osm_type"] != "")]
df = df[df["osm_query_usada"].notna() & (df["osm_query_usada"] != "")]
df = df[df["osm_pass"].notna() & (df["osm_pass"] != "")]
df = df[df["lat"].notna() & df["lon"].notna()]

# Aplicar CHECK manual (para evitar que falle al insertar)
df = df[df["lat"].between(-90, 90) & df["lon"].between(-180, 180)].copy()

# Unicidad
df = df.drop_duplicates(subset=["id_municipio"]).copy()
df = df.drop_duplicates(subset=["osm_id"]).copy()

df = df.reset_index(drop=True)
print("✅ Snapshot listo para carga:", df.shape)

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
    conn.execute(text("TRUNCATE TABLE culturatrip.dim_geografia_municipio_osm CASCADE;"))

    df.to_sql(
        "dim_geografia_municipio_osm",
        conn,
        schema="culturatrip",
        if_exists="append",
        index=False,
        method="multi",
        chunksize=2000
    )

print(f"🎉 Cargados {len(df)} registros en dim_geografia_municipio_osm")