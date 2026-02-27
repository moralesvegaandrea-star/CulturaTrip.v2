import os
import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.exc import OperationalError
from pathlib import Path

# =========================
# 1) Leer snapshot (SIN API)
# =========================
BASE_DIR = Path(__file__).resolve().parents[2]
path_in = BASE_DIR / "data" / "clean" / "dim_pais.csv"

df = pd.read_csv(path_in, dtype=str)

# =========================
# 2) Normalizar para calzar con CREATE TABLE
# =========================
# Solo columnas que existen en la tabla (NO incluir created_at)
df = df[["id_pais", "pais", "lat", "lon"]].copy()

# id_pais: ISO2, 2 chars, uppercase
df["id_pais"] = (
    df["id_pais"].astype(str).str.strip().str.upper().str[:2]
)

# pais: NOT NULL, limitar a 60
df["pais"] = df["pais"].astype(str).str.strip().str[:60]
df = df[df["pais"] != ""].copy()

# lat/lon: numeric(10,7)
df["lat"] = pd.to_numeric(df["lat"], errors="coerce").round(7)
df["lon"] = pd.to_numeric(df["lon"], errors="coerce").round(7)

# Quitar duplicados por PK por si acaso
df = df.drop_duplicates(subset=["id_pais"]).reset_index(drop=True)

print("✅ Snapshot listo para cargar:", df.shape)

# =========================
# 3) Conexión: localhost o contenedor
# =========================
DB_USER = os.getenv("DB_USER", "culturatrip")
DB_PASSWORD = os.getenv("DB_PASSWORD", "culturatrip")
DB_NAME = os.getenv("DB_NAME", "culturatrip")
DB_PORT = os.getenv("DB_PORT", "5433")
SCHEMA = os.getenv("DB_SCHEMA", "culturatrip")
TABLE = "dim_pais"

HOSTS = [os.getenv("DB_HOST", "").strip(), "localhost", "culturatrip_db", "db"]
HOSTS = [h for h in HOSTS if h]  # limpia vacíos

engine = None
for host in HOSTS:
    try:
        print(f"🔎 Intentando conectar a: {host}")
        eng = create_engine(
            f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{host}:{DB_PORT}/{DB_NAME}",
            pool_pre_ping=True
        )
        with eng.connect() as conn:
            conn.execute(text("SELECT 1"))
        engine = eng
        print(f"✅ Conectado a {host}")
        break
    except Exception as e:
        print(f"❌ No se pudo conectar a {host} -> {type(e).__name__}: {e}")

if engine is None:
    raise RuntimeError("🚨 No se pudo conectar a Postgres (probé localhost, culturatrip_db y db).")

# =========================
# 4) Validar que la tabla exista y que columnas calcen
# =========================
with engine.connect() as conn:
    exists = conn.execute(text("""
        SELECT to_regclass(:tname) IS NOT NULL
    """), {"tname": f"{SCHEMA}.{TABLE}"}).scalar()

if not exists:
    raise RuntimeError(f"🚨 No existe la tabla {SCHEMA}.{TABLE}. Ejecuta tu CREATE TABLE primero.")

# =========================
# 5) Truncate + Load (sin tocar created_at)
# =========================
with engine.begin() as conn:
    conn.execute(text(f"TRUNCATE TABLE {SCHEMA}.{TABLE} CASCADE;"))

df.to_sql(
    TABLE,
    engine,
    schema=SCHEMA,
    if_exists="append",
    index=False,
    method="multi",
    chunksize=1000
)

print(f"🎉 Cargados {len(df)} países en {SCHEMA}.{TABLE}")