"""
===============================================================================
CULTURALTRIP — PIPELINE CLOUD
SCRIPT 01: Ingesta de datos crudos a S3 (LocalStack)
===============================================================================

QUÉ HACE ESTE SCRIPT:
    Toma los archivos CSV que están en la carpeta data/clean/ de tu proyecto
    y los sube a un "bucket" de S3 simulado por LocalStack.

    Es como subir archivos a Google Drive, pero en vez de Google Drive
    usamos Amazon S3 (simulado en tu computador con LocalStack).

CONCEPTOS CLAVE:
    - boto3: librería de Python para hablar con servicios de AWS
    - bucket: una "carpeta raíz" en S3 donde guardas archivos
    - endpoint_url: la dirección donde LocalStack escucha (localhost:4566)
    - Las credenciales (aws_access_key_id) son falsas ("test/test")
      porque LocalStack no verifica credenciales reales

CÓMO EJECUTAR:
    Desde la raíz del proyecto, con Docker corriendo:

    docker compose run --rm app python cloud_pipeline/01_ingesta_s3.py

===============================================================================
"""

import boto3
import os
import sys

# ============================================================
# CONFIGURACIÓN
# ============================================================

# Dirección de LocalStack dentro de Docker.
# "localstack" es el nombre del servicio en docker-compose.yml.
# Docker lo traduce automáticamente a la IP del contenedor.
# El puerto 4566 es donde LocalStack escucha.
LOCALSTACK_URL = "http://localstack:4566"

# Nombre del bucket donde guardaremos los CSVs crudos.
# Un bucket es como una carpeta raíz en S3.
BUCKET_NAME = "culturatrip-raw"

# Región de AWS (simulada). Usamos Europa Oeste.
REGION = "eu-west-1"

# Carpeta donde están nuestros CSVs limpios.
# Esta ruta es DENTRO del contenedor Docker (el volumen ./:/app
# en docker-compose.yml monta tu proyecto en /app).
DATA_FOLDER = "data/clean"

# Lista de archivos CSV que vamos a subir.
# Son los mismos que usamos en el ETL original.
CSV_FILES = [
    "dim_pais.csv",
    "dim_ccaa_base.csv",
    "dim_provincia_base.csv",
    "dim_provincia_coordenadas_osm.csv",
    "dim_isla.csv",
    "dim_municipio_final.csv",
    "dim_geografia_municipio_osm.csv",
    "dim_tiempo.csv",
    "rel_municipio_isla.csv",
    "fact_actividades_provincia_enriquecida.csv",
    "df_alojamientos.csv",
]


# ============================================================
# PASO 1: Conectar con LocalStack (simula AWS)
# ============================================================

print("=" * 60)
print("CULTURALTRIP — Ingesta de datos a S3 (LocalStack)")
print("=" * 60)
print()

# Creamos un "cliente" de S3.
# Es como abrir la app de Google Drive antes de subir archivos.
# - endpoint_url: apunta a LocalStack en vez de a AWS real
# - aws_access_key_id/secret: credenciales falsas (LocalStack las acepta)
# - region_name: región simulada
print("[1/3] Conectando con LocalStack (S3 simulado)...")

s3_client = boto3.client(
    "s3",
    endpoint_url=LOCALSTACK_URL,
    aws_access_key_id="test",
    aws_secret_access_key="test",
    region_name=REGION,
)

print(f"       Conectado a {LOCALSTACK_URL}")
print()


# ============================================================
# PASO 2: Crear el bucket (si no existe)
# ============================================================

# Un bucket es como crear una carpeta nueva en Google Drive.
# Si ya existe, no pasa nada — simplemente continuamos.
print(f"[2/3] Creando bucket '{BUCKET_NAME}'...")

try:
    s3_client.create_bucket(
        Bucket=BUCKET_NAME,
        CreateBucketConfiguration={"LocationConstraint": REGION},
    )
    print(f"       Bucket '{BUCKET_NAME}' creado exitosamente.")
except s3_client.exceptions.BucketAlreadyOwnedByYou:
    print(f"       Bucket '{BUCKET_NAME}' ya existía — continuamos.")
except Exception as e:
    # Si hay un error inesperado, lo mostramos pero intentamos continuar
    print(f"       Aviso: {e}")
    print(f"       Intentando continuar de todos modos...")

print()


# ============================================================
# PASO 3: Subir cada archivo CSV al bucket
# ============================================================

# Recorremos la lista de archivos y subimos uno por uno.
# Es como arrastrar archivos a Google Drive.
print(f"[3/3] Subiendo {len(CSV_FILES)} archivos CSV al bucket...")
print()

archivos_subidos = 0
archivos_fallidos = 0

for csv_file in CSV_FILES:
    # Ruta completa del archivo en el disco local (dentro del contenedor)
    local_path = os.path.join(DATA_FOLDER, csv_file)

    # Verificar que el archivo existe antes de intentar subirlo
    if not os.path.exists(local_path):
        print(f"   ⚠  {csv_file} — NO ENCONTRADO en {DATA_FOLDER}/")
        archivos_fallidos += 1
        continue

    # Obtener el tamaño del archivo para mostrarlo
    file_size = os.path.getsize(local_path)
    file_size_kb = file_size / 1024

    try:
        # upload_file() sube el archivo local al bucket de S3.
        # - local_path: de dónde lo lee (tu disco)
        # - BUCKET_NAME: a qué bucket lo sube
        # - csv_file: con qué nombre lo guarda en S3 (el "key")
        s3_client.upload_file(local_path, BUCKET_NAME, csv_file)
        print(f"   ✓  {csv_file} ({file_size_kb:.1f} KB)")
        archivos_subidos += 1
    except Exception as e:
        print(f"   ✗  {csv_file} — ERROR: {e}")
        archivos_fallidos += 1


# ============================================================
# RESUMEN FINAL
# ============================================================

print()
print("=" * 60)
print("RESUMEN DE INGESTA")
print("=" * 60)
print(f"   Archivos subidos:  {archivos_subidos}/{len(CSV_FILES)}")
print(f"   Archivos fallidos: {archivos_fallidos}/{len(CSV_FILES)}")
print(f"   Bucket destino:    s3://{BUCKET_NAME}/")
print()

# Verificación: listar los archivos que quedaron en el bucket
print("Archivos en el bucket:")
try:
    response = s3_client.list_objects_v2(Bucket=BUCKET_NAME)
    if "Contents" in response:
        for obj in response["Contents"]:
            size_kb = obj["Size"] / 1024
            print(f"   📄 {obj['Key']} ({size_kb:.1f} KB)")
    else:
        print("   (vacío)")
except Exception as e:
    print(f"   Error al listar: {e}")

print()
if archivos_fallidos == 0:
    print("✅ Ingesta completada exitosamente.")
    print("   Siguiente paso: ejecutar 02_procesamiento_spark.py")
else:
    print("⚠️  Ingesta completada con errores. Revisar archivos faltantes.")
    sys.exit(1)