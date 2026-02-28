import os
import pandas as pd
from pathlib import Path
from sqlalchemy import create_engine, text

# =========================
# 1) RUTAS
# =========================
BASE_DIR = Path(__file__).resolve().parents[2]
path_in = BASE_DIR / "data" / "clean" / "fact_actividades_provincia_enriquecida.csv"

# =========================
# 2) CARGAR CSV
# =========================
df = pd.read_csv(path_in)
df.columns = df.columns.str.strip()

print("📊 Snapshot inicial:", df.shape)

# =========================
# 3) NORMALIZACIÓN
# =========================

# IDs
df["id_pais"] = df["id_pais"].astype(str).str.strip().str.upper().str[:2]
df["id_ccaa"] = df["id_ccaa"].astype(str).str.strip().str.zfill(2)
df["id_provincia"] = df["id_provincia"].astype(str).str.strip().str.zfill(2)

# Mes
df["mes"] = pd.to_numeric(df["mes"], errors="coerce").astype("Int64")
df = df[df["mes"].between(1, 12)]

# Textos
text_cols = [
    "categoria",
    "producto",
    "subcategoria",
    "comunidad_autonoma",
    "provincia"
]

for col in text_cols:
    df[col] = df[col].astype(str).str.strip()

# Numéricos
df["gasto_total_promedio"] = pd.to_numeric(df["gasto_total_promedio"], errors="coerce")
df["precio_medio_entrada_promedio"] = pd.to_numeric(df["precio_medio_entrada_promedio"], errors="coerce")

df["valoracion_por_categoria_promedio"] = pd.to_numeric(
    df["valoracion_por_categoria_promedio"], errors="coerce"
)

df["valoracion_general_promedio"] = pd.to_numeric(
    df["valoracion_general_promedio"], errors="coerce"
)

df["total_opiniones_categoria_promedio"] = pd.to_numeric(
    df["total_opiniones_categoria_promedio"], errors="coerce"
).astype("Int64")

# Boolean
df["hay_valoracion"] = df["hay_valoracion"].astype(bool)

# Eliminar filas con campos obligatorios nulos
df = df[
    df["id_pais"].notna()
    & df["id_ccaa"].notna()
    & df["id_provincia"].notna()
    & df["mes"].notna()
    & df["categoria"].notna()
    & df["producto"].notna()
    & df["subcategoria"].notna()
    & df["gasto_total_promedio"].notna()
    & df["precio_medio_entrada_promedio"].notna()
]

# Quitar duplicados según llave natural
df = df.drop_duplicates(
    subset=[
        "id_pais",
        "id_ccaa",
        "id_provincia",
        "mes",
        "categoria",
        "producto",
        "subcategoria",
    ]
)

df = df.reset_index(drop=True)

print("✅ Snapshot listo para carga:", df.shape)

# =========================
# 4) CONEXIÓN POSTGRES
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

    conn.execute(text("TRUNCATE TABLE culturatrip.fact_actividades CASCADE;"))

    df.to_sql(
        "fact_actividades",
        conn,
        schema="culturatrip",
        if_exists="append",
        index=False,
        method="multi",
        chunksize=2000
    )

print(f"🎉 Cargadas {len(df)} filas en fact_actividades")