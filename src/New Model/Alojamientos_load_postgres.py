import os
import pandas as pd
from pathlib import Path
from sqlalchemy import create_engine, text

# =========================
# 1) RUTAS
# =========================
BASE_DIR = Path(__file__).resolve().parents[2]
path_in = BASE_DIR / "data" / "clean" / "df_alojamientos.csv"

# =========================
# 2) CARGAR
# =========================
df = pd.read_csv(path_in, dtype=str)
df.columns = df.columns.str.strip()
print("📊 Snapshot inicial:", df.shape)

# =========================
# 3) COLUMNAS (ajusta si tu CSV trae nombres distintos)
# =========================
cols = [
    "id_pais",
    "id_ccaa",
    "id_provincia",
    "mes",
    "categoria_alojamiento",
    "periodo_antelacion",
    "fuente",
    "granularidad_origen",
    "nivel_geografico",
    "precio_checkin_entre_semana",
    "precio_checkin_fin_semana",
    "valoraciones_norm",
    "tiene_valoraciones",
    "es_dato_replicado",
]

# si faltan columnas, las creamos como NULL (tolerante)
for c in cols:
    if c not in df.columns:
        df[c] = None

df = df[cols].copy()

# =========================
# 4) NORMALIZACIÓN / TIPOS
# =========================
df["id_pais"] = df["id_pais"].astype(str).str.strip().str.upper().str[:2]
df["id_ccaa"] = df["id_ccaa"].astype(str).str.strip().str.zfill(2)
df["id_provincia"] = df["id_provincia"].astype(str).str.strip().str.zfill(2)

# mes
df["mes"] = pd.to_numeric(df["mes"], errors="coerce").astype("Int64")
df = df[df["mes"].between(1, 12)]

# textos (pueden ser NULL)
txt_cols = [
    "categoria_alojamiento",
    "periodo_antelacion",
    "fuente",
    "granularidad_origen",
    "nivel_geografico",
]
for c in txt_cols:
    df[c] = df[c].astype("string").str.strip()
    df.loc[df[c].isin(["", "nan", "None", "NONE", "<NA>"]), c] = None

# numéricos (con coma decimal por si acaso)
for c in ["precio_checkin_entre_semana", "precio_checkin_fin_semana", "valoraciones_norm"]:
    df[c] = df[c].astype("string").str.replace(",", ".", regex=False)
    df[c] = pd.to_numeric(df[c], errors="coerce")

# booleanos (tolerante)
def to_bool(x):
    if x is None or (isinstance(x, float) and pd.isna(x)):
        return None
    s = str(x).strip().lower()
    if s in ["1", "true", "t", "yes", "y", "si", "sí"]:
        return True
    if s in ["0", "false", "f", "no", "n"]:
        return False
    return None

df["tiene_valoraciones"] = df["tiene_valoraciones"].apply(to_bool)
df["es_dato_replicado"] = df["es_dato_replicado"].apply(to_bool)

# obligatorios (por FK y CHECK)
df = df[df["id_pais"].notna() & (df["id_pais"] != "")]
df = df[df["id_ccaa"].notna() & (df["id_ccaa"] != "")]
df = df[df["id_provincia"].notna() & (df["id_provincia"] != "")]

# llave natural: si vienen NULL, se vuelven string para dedupe consistente
dedupe_cols = ["id_pais", "id_ccaa", "id_provincia", "mes", "categoria_alojamiento", "periodo_antelacion"]
for c in ["categoria_alojamiento", "periodo_antelacion"]:
    df[c] = df[c].fillna("")

df = df.drop_duplicates(subset=dedupe_cols).reset_index(drop=True)

# si los dejaste como "" por dedupe, vuelve a NULL
for c in ["categoria_alojamiento", "periodo_antelacion"]:
    df.loc[df[c] == "", c] = None

print("✅ Snapshot listo para carga:", df.shape)

# =========================
# 5) CONEXIÓN POSTGRES (Docker)
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
# 6) TRUNCATE + LOAD
# =========================
with engine.begin() as conn:
    conn.execute(text("TRUNCATE TABLE culturatrip.fact_alojamientos CASCADE;"))

    df.to_sql(
        "fact_alojamientos",
        conn,
        schema="culturatrip",
        if_exists="append",
        index=False,
        method="multi",
        chunksize=2000
    )

print(f"🎉 Cargadas {len(df)} filas en fact_alojamientos")