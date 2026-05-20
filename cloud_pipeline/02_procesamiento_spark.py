"""
===============================================================================
CULTURALTRIP — PIPELINE CLOUD
SCRIPT 02: Procesamiento con PySpark
===============================================================================

CÓMO EJECUTAR:
    docker compose exec spark /opt/spark/bin/spark-submit \
        --packages org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262 \
        /app/cloud_pipeline/02_procesamiento_spark.py

===============================================================================
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# ============================================================
# CONFIGURACIÓN
# ============================================================

LOCALSTACK_URL = "http://localstack:4566"
BUCKET_RAW = "culturatrip-raw"
BUCKET_GOLD = "culturatrip-gold"
REGION = "eu-west-1"


# ============================================================
# FUNCIÓN: Limpiar nombres de columnas
# ============================================================

def limpiar_columnas(df):
    """
    Limpia problemas de nombres de columnas:
    1. Quita el BOM invisible del inicio
    2. Renombra la columna 'año' si tiene caracteres corruptos
       SOLO columnas cortas (max 5 chars) que empiecen con 'a' y terminen con 'o'
       para NO tocar columnas largas como 'gasto_total_promedio'
    """
    # Paso 1: Quitar BOM de todas las columnas
    for col_name in df.columns:
        clean_name = col_name.replace("\ufeff", "")
        if col_name != clean_name:
            df = df.withColumnRenamed(col_name, clean_name)

    # Paso 2: Buscar la columna "año" corrupta
    # Solo columnas cortas (max 5 caracteres) que NO sean "año" ya
    for col_name in df.columns:
        if (col_name != "año"
            and len(col_name) <= 5
            and col_name.startswith("a")
            and col_name.endswith("o")
            and col_name not in ["alto", "acto"]):
            print(f"       [FIX] Renombrando columna '{col_name}' -> 'año'")
            df = df.withColumnRenamed(col_name, "año")

    return df


def leer_csv_desde_s3(spark, bucket, filename):
    """Lee un CSV desde S3 y limpia los nombres de columnas."""
    df = (
        spark.read
        .option("header", "true")
        .option("inferSchema", "true")
        .csv(f"s3a://{bucket}/{filename}")
    )
    df = limpiar_columnas(df)
    return df


# ============================================================
# PASO 1: Crear la sesión de Spark
# ============================================================

print("=" * 60)
print("CULTURALTRIP — Procesamiento con PySpark")
print("=" * 60)
print()
print("[1/6] Iniciando sesion de Spark...")

spark = (
    SparkSession.builder
    .appName("CulturaTrip_ETL_Cloud")
    .master("local[*]")
    .config("spark.hadoop.fs.s3a.endpoint", LOCALSTACK_URL)
    .config("spark.hadoop.fs.s3a.access.key", "test")
    .config("spark.hadoop.fs.s3a.secret.key", "test")
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")
print("       Spark iniciado correctamente.")
print()


# ============================================================
# PASO 2: Crear el bucket GOLD en S3
# ============================================================

print("[2/6] Creando bucket GOLD en S3...")

import boto3

s3_client = boto3.client(
    "s3",
    endpoint_url=LOCALSTACK_URL,
    aws_access_key_id="test",
    aws_secret_access_key="test",
    region_name=REGION,
)

try:
    s3_client.create_bucket(
        Bucket=BUCKET_GOLD,
        CreateBucketConfiguration={"LocationConstraint": REGION},
    )
    print(f"       Bucket '{BUCKET_GOLD}' creado.")
except Exception as e:
    print(f"       Bucket '{BUCKET_GOLD}' ya existe o aviso: {e}")

print()


# ============================================================
# PASO 3: Leer las tablas desde S3
# ============================================================

print("[3/6] Leyendo datos crudos desde S3...")

# --- Actividades ---
print("       Leyendo fact_actividades_provincia_enriquecida.csv...")
df_actividades = leer_csv_desde_s3(spark, BUCKET_RAW, "fact_actividades_provincia_enriquecida.csv")
print(f"       Columnas: {df_actividades.columns}")
print(f"       -> {df_actividades.count()} filas leidas")

# --- Alojamientos ---
print("       Leyendo df_alojamientos.csv...")
df_alojamientos = leer_csv_desde_s3(spark, BUCKET_RAW, "df_alojamientos.csv")
print(f"       Columnas: {df_alojamientos.columns}")
print(f"       -> {df_alojamientos.count()} filas leidas")

# --- Dimensiones ---
print("       Leyendo tablas dimensionales...")

dimension_files = [
    "dim_pais",
    "dim_ccaa_base",
    "dim_provincia_base",
    "dim_provincia_coordenadas_osm",
    "dim_isla",
    "dim_municipio_final",
    "dim_geografia_municipio_osm",
    "dim_tiempo",
    "rel_municipio_isla",
]

dim_dataframes = {}
for dim_name in dimension_files:
    df = leer_csv_desde_s3(spark, BUCKET_RAW, f"{dim_name}.csv")
    dim_dataframes[dim_name] = df
    print(f"       -> {dim_name}: {df.count()} filas")

print()


# ============================================================
# PASO 4: Limpiar los datos
# ============================================================

print("[4/6] Limpiando datos...")

# --- Limpiar ACTIVIDADES ---
actividades_antes = df_actividades.count()
print(f"       Columnas de actividades: {df_actividades.columns}")

df_actividades_clean = (
    df_actividades
    .dropna(subset=["gasto_total_promedio"])
    .fillna("sin_dato", subset=["categoria", "producto", "subcategoria",
                                 "comunidad_autonoma", "provincia"])
    .fillna(0, subset=["precio_medio_entrada_promedio",
                        "valoracion_por_categoria_promedio",
                        "valoracion_general_promedio",
                        "total_opiniones_categoria_promedio"])
)
actividades_despues = df_actividades_clean.count()
print(f"       Actividades: {actividades_antes} -> {actividades_despues} filas "
      f"({actividades_antes - actividades_despues} eliminadas por nulos)")

# --- Limpiar ALOJAMIENTOS ---
alojamientos_antes = df_alojamientos.count()
df_alojamientos_clean = (
    df_alojamientos
    .dropna(subset=["precio_checkin_entre_semana"])
    .fillna("sin_dato", subset=["categoria_alojamiento", "periodo_antelacion",
                                 "fuente", "granularidad_origen", "nivel_geografico"])
    .fillna(0, subset=["precio_checkin_fin_semana", "valoraciones_norm"])
)
alojamientos_despues = df_alojamientos_clean.count()
print(f"       Alojamientos: {alojamientos_antes} -> {alojamientos_despues} filas "
      f"({alojamientos_antes - alojamientos_despues} eliminadas por nulos)")

print()


# ============================================================
# PASO 5: Agregar datos (resúmenes por provincia y mes)
# ============================================================

print("[5/6] Agregando datos (resumenes por provincia y mes)...")

# --- Agregación de ACTIVIDADES ---
df_actividades_agg = (
    df_actividades_clean
    .groupBy("id_provincia", "id_ccaa", "año", "mes", "provincia", "comunidad_autonoma")
    .agg(
        F.round(F.avg("gasto_total_promedio"), 2).alias("gasto_promedio"),
        F.round(F.avg("precio_medio_entrada_promedio"), 2).alias("precio_entrada_promedio"),
        F.round(F.avg("valoracion_general_promedio"), 2).alias("valoracion_promedio"),
        F.sum("total_opiniones_categoria_promedio").alias("total_opiniones"),
        F.count("*").alias("num_actividades"),
    )
)
print(f"       Actividades agregadas: {df_actividades_agg.count()} filas")

# --- Agregación de ALOJAMIENTOS ---
df_alojamientos_agg = (
    df_alojamientos_clean
    .groupBy("id_provincia", "id_ccaa", "año", "mes", "categoria_alojamiento")
    .agg(
        F.round(F.avg("precio_checkin_entre_semana"), 2).alias("precio_semana_promedio"),
        F.round(F.avg("precio_checkin_fin_semana"), 2).alias("precio_finde_promedio"),
        F.round(F.avg("valoraciones_norm"), 2).alias("valoracion_promedio"),
        F.count("*").alias("num_registros"),
    )
)
print(f"       Alojamientos agregados: {df_alojamientos_agg.count()} filas")

print()


# ============================================================
# PASO 6: Guardar todo en formato Parquet en el bucket GOLD
# ============================================================

print("[6/6] Guardando datos en formato Parquet en S3 (bucket GOLD)...")

# --- Tablas de hechos limpias ---
print("       Guardando actividades limpias...")
(df_actividades_clean
    .coalesce(1)
    .write.mode("overwrite")
    .parquet(f"s3a://{BUCKET_GOLD}/fact_actividades_clean"))

print("       Guardando alojamientos limpios...")
(df_alojamientos_clean
    .coalesce(1)
    .write.mode("overwrite")
    .parquet(f"s3a://{BUCKET_GOLD}/df_alojamientos_clean"))

# --- Tablas agregadas ---
print("       Guardando actividades agregadas...")
(df_actividades_agg
    .coalesce(1)
    .write.mode("overwrite")
    .parquet(f"s3a://{BUCKET_GOLD}/actividades_agregadas"))

print("       Guardando alojamientos agregados...")
(df_alojamientos_agg
    .coalesce(1)
    .write.mode("overwrite")
    .parquet(f"s3a://{BUCKET_GOLD}/alojamientos_agregados"))

# --- Tablas dimensionales ---
print("       Guardando dimensiones...")
for dim_name, df in dim_dataframes.items():
    (df
        .coalesce(1)
        .write.mode("overwrite")
        .parquet(f"s3a://{BUCKET_GOLD}/{dim_name}"))
    print(f"       -> {dim_name}")


# ============================================================
# RESUMEN FINAL
# ============================================================

print()
print("=" * 60)
print("RESUMEN DE PROCESAMIENTO")
print("=" * 60)
print(f"   Bucket origen (raw):   s3://{BUCKET_RAW}/")
print(f"   Bucket destino (gold): s3://{BUCKET_GOLD}/")
print()
print("   Archivos generados en GOLD:")
print(f"   fact_actividades_clean    ({actividades_despues} filas)")
print(f"   df_alojamientos_clean     ({alojamientos_despues} filas)")
print(f"   actividades_agregadas     ({df_actividades_agg.count()} filas)")
print(f"   alojamientos_agregados    ({df_alojamientos_agg.count()} filas)")
print(f"   {len(dim_dataframes)} tablas dimensionales")
print()
print("Procesamiento completado exitosamente.")
print("   Siguiente paso: ejecutar 03_carga_postgresql.py")

spark.stop()
