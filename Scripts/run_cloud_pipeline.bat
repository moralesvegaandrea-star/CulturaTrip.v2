@echo off
echo ============================================================
echo CULTURALTRIP — Pipeline Cloud Completo
echo ============================================================
echo.
echo Este script ejecuta los 3 pasos del pipeline en orden:
echo   1. Ingesta de CSVs a S3 (LocalStack)
echo   2. Procesamiento con PySpark
echo   3. Carga de Parquet a PostgreSQL
echo.
echo Prerequisito: Docker debe estar corriendo con todos los servicios.
echo Ejecuta primero: docker compose up --build
echo.
pause

echo.
echo ============================================================
echo PASO 1/3: Ingesta de CSVs a S3 (LocalStack)
echo ============================================================
echo.
docker compose run --rm app python cloud_pipeline/01_ingesta_s3.py
if %ERRORLEVEL% neq 0 (
    echo.
    echo ERROR: Fallo en la ingesta. Revisa que LocalStack este corriendo.
    pause
    exit /b 1
)

echo.
echo ============================================================
echo PASO 2/3: Procesamiento con PySpark
echo ============================================================
echo.
docker compose exec spark spark-submit --packages org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262 /app/cloud_pipeline/02_procesamiento_spark.py
if %ERRORLEVEL% neq 0 (
    echo.
    echo ERROR: Fallo en el procesamiento Spark.
    pause
    exit /b 1
)

echo.
echo ============================================================
echo PASO 3/3: Carga de Parquet a PostgreSQL
echo ============================================================
echo.
docker compose run --rm app python cloud_pipeline/03_carga_postgresql.py
if %ERRORLEVEL% neq 0 (
    echo.
    echo ERROR: Fallo en la carga a PostgreSQL.
    pause
    exit /b 1
)

echo.
echo ============================================================
echo PIPELINE COMPLETADO EXITOSAMENTE
echo ============================================================
echo.
echo Los datos han recorrido el pipeline completo:
echo   CSVs -> S3 (LocalStack) -> PySpark -> Parquet -> PostgreSQL
echo.
echo Puedes verificar los datos en:
echo   - Streamlit: http://localhost:8501
echo   - PostgreSQL: schema "cloud"
echo.
pause