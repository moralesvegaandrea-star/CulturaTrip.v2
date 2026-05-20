"""
===============================================================================
CULTURALTRIP — PIPELINE CLOUD
SCRIPT 03: Carga de Parquet a PostgreSQL
===============================================================================

QUÉ HACE ESTE SCRIPT:
    Lee los archivos Parquet que generó PySpark en el bucket GOLD de S3
    (LocalStack), y los carga como tablas en PostgreSQL.

    Es el puente entre "los datos limpios en la nube" y "la base de datos
    donde Streamlit y Tableau leen los datos".

CONCEPTOS CLAVE:
    - Parquet: formato de archivo eficiente que generamos en el paso 02.
      pandas puede leerlos con pd.read_parquet().
    - boto3: descarga los Parquet desde S3 (LocalStack) a memoria.
    - SQLAlchemy: se conecta a PostgreSQL y carga los DataFrames como tablas.
    - to_sql(): método de pandas que convierte un DataFrame en una tabla SQL.
    - if_exists="replace": si la tabla ya existe, la borra y la recrea.

CÓMO EJECUTAR:
    Desde la raíz del proyecto, con Docker corriendo:

    docker compose run --rm app python cloud_pipeline/03_carga_postgresql.py

===============================================================================
"""

import pandas as pd
import boto3
import io
import os
from sqlalchemy import create_engine

# ============================================================
# CONFIGURACIÓN
# ============================================================

# Conexión a LocalStack (S3 simulado)
LOCALSTACK_URL = "http://localstack:4566"
BUCKET_GOLD = "culturatrip-gold"
REGION = "eu-west-1"

# Conexión a PostgreSQL
# Usamos las mismas variables de entorno que ya tiene docker-compose.yml
DB_HOST = os.getenv("DB_HOST", "db")
DB_PORT = os.getenv("DB_PORT", "5432")
DB_NAME = os.getenv("DB_NAME", "culturatrip")
DB_USER = os.getenv("DB_USER", "culturatrip")
DB_PASSWORD = os.getenv("DB_PASSWORD", "culturatrip")

# Schema donde cargaremos las tablas
# Usamos "cloud" para separar las tablas del pipeline cloud
# de las tablas originales del ETL
SCHEMA = "cloud"

# Lista de carpetas Parquet en el bucket GOLD
# Cada una se convertirá en una tabla en PostgreSQL
PARQUET_TABLES = [
    # Tablas de hechos limpias
    {"s3_folder": "fact_actividades_clean",    "table_name": "fact_actividades_clean"},
    {"s3_folder": "df_alojamientos_clean",     "table_name": "fact_alojamientos_clean"},
    # Tablas agregadas
    {"s3_folder": "actividades_agregadas",     "table_name": "actividades_agregadas"},
    {"s3_folder": "alojamientos_agregados",    "table_name": "alojamientos_agregados"},
    # Dimensiones
    {"s3_folder": "dim_pais",                  "table_name": "dim_pais"},
    {"s3_folder": "dim_ccaa_base",             "table_name": "dim_ccaa_base"},
    {"s3_folder": "dim_provincia_base",        "table_name": "dim_provincia_base"},
    {"s3_folder": "dim_provincia_coordenadas_osm", "table_name": "dim_provincia_coordenadas_osm"},
    {"s3_folder": "dim_isla",                  "table_name": "dim_isla"},
    {"s3_folder": "dim_municipio_final",       "table_name": "dim_municipio_final"},
    {"s3_folder": "dim_geografia_municipio_osm", "table_name": "dim_geografia_municipio_osm"},
    {"s3_folder": "dim_tiempo",                "table_name": "dim_tiempo"},
    {"s3_folder": "rel_municipio_isla",        "table_name": "rel_municipio_isla"},
]


# ============================================================
# PASO 1: Conectar con S3 (LocalStack) y PostgreSQL
# ============================================================

print("=" * 60)
print("CULTURALTRIP — Carga de Parquet a PostgreSQL")
print("=" * 60)
print()

# --- Conectar con S3 ---
print("[1/3] Conectando con LocalStack (S3) y PostgreSQL...")

s3_client = boto3.client(
    "s3",
    endpoint_url=LOCALSTACK_URL,
    aws_access_key_id="test",
    aws_secret_access_key="test",
    region_name=REGION,
)
print(f"       S3: conectado a {LOCALSTACK_URL}")

# --- Conectar con PostgreSQL ---
# create_engine() crea una conexión reutilizable a la base de datos.
# El formato es: postgresql://usuario:contraseña@host:puerto/base_de_datos
connection_string = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
engine = create_engine(connection_string)
print(f"       PostgreSQL: conectado a {DB_HOST}:{DB_PORT}/{DB_NAME}")
print()


# ============================================================
# PASO 2: Crear el schema "cloud" en PostgreSQL
# ============================================================
# Un schema es como una "subcarpeta" dentro de la base de datos.
# Separamos las tablas del pipeline cloud de las tablas originales
# para no mezclarlas.

print("[2/3] Creando schema 'cloud' en PostgreSQL...")

with engine.connect() as conn:
    conn.execute(
        __import__("sqlalchemy").text(f"CREATE SCHEMA IF NOT EXISTS {SCHEMA}")
    )
    conn.commit()
print(f"       Schema '{SCHEMA}' listo.")
print()


# ============================================================
# PASO 3: Leer cada Parquet desde S3 y cargarlo a PostgreSQL
# ============================================================
# Para cada tabla:
#   1. Listamos los archivos .parquet en la carpeta de S3
#   2. Descargamos cada archivo a memoria (no a disco)
#   3. Lo leemos con pandas como DataFrame
#   4. Lo cargamos a PostgreSQL con to_sql()

print(f"[3/3] Cargando {len(PARQUET_TABLES)} tablas a PostgreSQL (schema: {SCHEMA})...")
print()

tablas_ok = 0
tablas_error = 0

for table_info in PARQUET_TABLES:
    s3_folder = table_info["s3_folder"]
    table_name = table_info["table_name"]

    try:
        # Paso 3a: Listar archivos .parquet en la carpeta de S3
        # Spark guarda los Parquet en carpetas con archivos como:
        #   part-00000-xxxxx.snappy.parquet
        #   _SUCCESS
        # Solo nos interesan los .parquet, no los _SUCCESS
        response = s3_client.list_objects_v2(
            Bucket=BUCKET_GOLD,
            Prefix=f"{s3_folder}/"
        )

        if "Contents" not in response:
            print(f"   ⚠  {table_name} — carpeta vacía en S3, saltando...")
            tablas_error += 1
            continue

        # Filtrar solo archivos .parquet (ignorar _SUCCESS y otros)
        parquet_keys = [
            obj["Key"] for obj in response["Contents"]
            if obj["Key"].endswith(".parquet")
        ]

        if not parquet_keys:
            print(f"   ⚠  {table_name} — no se encontraron archivos .parquet")
            tablas_error += 1
            continue

        # Paso 3b: Descargar y leer cada archivo Parquet
        # Usamos io.BytesIO para leer en memoria sin guardar en disco
        dfs = []
        for key in parquet_keys:
            obj = s3_client.get_object(Bucket=BUCKET_GOLD, Key=key)
            parquet_bytes = obj["Body"].read()
            df = pd.read_parquet(io.BytesIO(parquet_bytes))
            dfs.append(df)

        # Concatenar todos los fragmentos (normalmente es solo 1 por coalesce(1))
        df_final = pd.concat(dfs, ignore_index=True)

        # Paso 3c: Cargar a PostgreSQL
        # - schema="cloud": lo guarda en el schema "cloud" (no en "public")
        # - if_exists="replace": si la tabla ya existe, la reemplaza
        # - index=False: no guarda el índice de pandas como columna
        df_final.to_sql(
            name=table_name,
            con=engine,
            schema=SCHEMA,
            if_exists="replace",
            index=False,
        )

        print(f"   ✓  {SCHEMA}.{table_name} — {len(df_final)} filas cargadas")
        tablas_ok += 1

    except Exception as e:
        print(f"   ✗  {table_name} — ERROR: {e}")
        tablas_error += 1


# ============================================================
# RESUMEN FINAL
# ============================================================

print()
print("=" * 60)
print("RESUMEN DE CARGA")
print("=" * 60)
print(f"   Tablas cargadas:  {tablas_ok}/{len(PARQUET_TABLES)}")
print(f"   Tablas con error: {tablas_error}/{len(PARQUET_TABLES)}")
print(f"   Schema destino:   {SCHEMA}")
print(f"   Base de datos:    {DB_NAME} en {DB_HOST}:{DB_PORT}")
print()

if tablas_error == 0:
    print("✅ Carga completada exitosamente.")
    print()
    print("   Las tablas están disponibles en PostgreSQL bajo el schema 'cloud'.")
    print("   Ejemplo de consulta:")
    print(f"       SELECT * FROM {SCHEMA}.actividades_agregadas LIMIT 10;")
    print()
    print("   Las vistas SQL originales (vw_ui_*, vw_rec_*, vw_ml_*)")
    print("   siguen funcionando con las tablas del schema 'public'.")
else:
    print("⚠️  Carga completada con errores. Revisar los mensajes anteriores.")
