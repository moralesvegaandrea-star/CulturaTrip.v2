# 🌍 CulturaTrip – Plataforma Inteligente de Análisis Turístico

## Trabajo Final de Máster – Big Data & Business Intelligence

**Autores:** 

Ana Belén Chaves Jiménez

Hilda Mireya Ibarra Mata

Montserrat Ulloa Álvarez 

Andrea Lucia Morales Vega 

Ronald Rojas Barquero


## Descripción del Proyecto

CulturaTrip es una plataforma inteligente de planificación de viajes culturales que integra datos territoriales, turísticos y económicos para generar recomendaciones personalizadas basadas en el presupuesto del usuario.

El sistema combina:

* Procesamiento de datos (ETL)

* Modelado relacional en PostgreSQL

* Vistas analíticas

* Modelos de Machine Learning

* Aplicación interactiva en Streamlit

* Pipeline Cloud-in-Local (LocalStack + PySpark)

Todo el entorno está diseñado bajo un enfoque de reproducibilidad completa mediante Docker.

Datos necesarios para reproducir: los CSV se encuentran en la dirección data/clean/.



## Objetivo

Desarrollar un sistema que permita:

* Planificar viajes culturales de forma personalizada

* Estimar costos por categoría (alojamiento, transporte, alimentación, actividades)

* Recomendar destinos óptimos

* Analizar la viabilidad del viaje según presupuesto

* Integrar modelos de Machine Learning en la toma de decisiones

## Requisitos del entorno

Antes de ejecutar el proyecto, asegúrate de tener instaladas las siguientes herramientas:

- Python 3.10 o superior
- Docker Desktop

Puedes verificar las versiones instaladas ejecutando:

```bash
python --version
docker --version
```
#### Ejemplo de salida
```
     Python 3.14.2
     Docker version 29.2.1
```

### Como Clonar el repositorio:

```bash
git clone https://github.com/moralesvegaandrea-star/CulturaTrip.v2
cd CulturaTrip.v2
```

## Datasets Utilizados

El sistema integra los siguientes datasets:
   
| Dataset                          | Objetivo                         | Origen                  | Tipo    |
| -------------------------------- | -------------------------------- | ----------------------- | ------- |
| División político-administrativa | Estructura jerárquica geográfica | INE / fuentes oficiales | Tabular |
| Georreferenciación municipal     | Coordenadas OSM                  | OpenStreetMap           | Tabular |
| Alojamiento turístico            | Análisis de precios y demanda    | Dataestur               | Tabular |
| Actividades culturales           | Gasto y valoración turística     | Dataestur               | Tabular |

### Cada dataset ha sido caracterizado considerando:

Estructura tabular

Volumen moderado (escala académica)

Actualización periódica

Requisitos de integridad referencial

Patrones de acceso analítico (JOIN, agregaciones, filtros temporales)

## Selección Tecnológica

Se selecciona PostgreSQL 16 como SGBD principal por:

* Integridad referencial robusta (claves foráneas)

* Soporte avanzado para consultas analíticas

* Escalabilidad

* Gestión de tipos numéricos de precisión financiera (NUMERIC)

* Compatibilidad con Docker para ejecución local

Alternativas consideradas:

| Tecnología | Motivo de descarte                                     |
| ---------- | ------------------------------------------------------ |
| MySQL      | Menor flexibilidad en modelado avanzado                |
| MongoDB    | No adecuado para modelo relacional con alta integridad |
| SQLite     | Limitaciones en concurrencia y escalabilidad           |


## Arquitectura del Sistema

     El sistema sigue una arquitectura por capas:

       Datos (APIs / CSV)
           ↓
      ETL Python
           ↓
    PostgreSQL (tablas relacionales)
           ↓
    Views (QA + UI + Costos + ML Features)
           ↓
    Machine Learning (.pkl)
           ↓
     Streamlit App (B2C — turista)
     Tableau (B2B — instituciones)

## Infraestructura (Docker)

El proyecto se ejecuta mediante Docker Compose con cinco servicios:


| Servicio   | Imagen                         | Descripción                          | Puerto |
| ---------- | ------------------------------ | ------------------------------------ | ------ |
| db         | postgres:16                    | Base de datos PostgreSQL             | 5433   |
| app        | culturatrip (build local)      | Aplicación Streamlit + ETL           | 8501   |
| notebook   | culturatrip (build local)      | Jupyter Notebook para análisis       | 8888   |
| localstack | localstack/localstack:3.5      | Simulador de Amazon S3 (Cloud-in-Local) | 4566   |
| spark      | apache/spark:3.5.3-python3     | Apache Spark para procesamiento PySpark | 8080   |



Definido en:

    docker-compose.yml

## Flujo Operativo del Proyecto

### Paso 1: Levantar el entorno

```bash
docker compose up --build
```

Esto inicia los 5 servicios. Verificar que todos están corriendo:

```bash
docker ps
```

Deberías ver 5 contenedores activos:

| Servicio             | URL / Puerto                                   |
| -------------------- | ---------------------------------------------- |
| Aplicación Streamlit | [http://localhost:8501](http://localhost:8501)  |
| Jupyter Notebook     | [http://localhost:8888](http://localhost:8888)  |
| PostgreSQL           | localhost:5433                                  |
| LocalStack (S3)      | [http://localhost:4566](http://localhost:4566)  |
| Spark UI             | [http://localhost:8080](http://localhost:8080)  |


### Paso 2: Ejecutar el proceso ETL (carga de datos a PostgreSQL)

#### Ejecución rápida (recomendado):

```bash
Scripts/run_etl_load_data.bat
```
#### Ejecución manual (en orden):

```bash
docker compose run --rm app python src/"New Model"/Paises_load_postgres.py
docker compose run --rm app python src/"New Model"/Comunidad_Autonomas_New_Model_load_postgres.py
docker compose run --rm app python src/"New Model"/Provincias_new_model_load_postgres.py
docker compose run --rm app python src/"New Model"/Islas_v2_new_model_load_postgres.py
docker compose run --rm app python src/"New Model"/Division_Politica_load_postgres.py
docker compose run --rm app python src/"New Model"/rel_municipio_isla_load_postgres.py
docker compose run --rm app python src/"New Model"/OpenstreetMap_load_postgres.py
docker compose run --rm app python src/"New Model"/Actividades_load_postgres.py
docker compose run --rm app python src/"New Model"/Alojamientos_load_postgres.py
docker exec -it culturatrip_db psql -U culturatrip -d culturatrip -f /docker-entrypoint-initdb.d/04_new_tables.sql
docker exec -it culturatrip_db psql -U culturatrip -d culturatrip -f /docker-entrypoint-initdb.d/06_index.sql
```

### Paso 3: Ejecutar los scripts SQL (vistas, cambios y ML)

#### Ejecución rápida (recomendado):

```bash
Scripts/run_views.bat
```

#### Ejecución manual (IMPORTANTE — respetar el orden):

Los scripts SQL tienen dependencias entre sí. Algunos deben ejecutarse dos veces porque crean objetos que otros scripts referencian. El orden correcto es:

```bash
# Primera pasada: crear todas las estructuras base
docker exec -it culturatrip_db psql -U culturatrip -d culturatrip -f /docker-entrypoint-initdb.d/07_new_changes.sql
docker exec -it culturatrip_db psql -U culturatrip -d culturatrip -f /docker-entrypoint-initdb.d/08_alter_tables.sql
docker exec -it culturatrip_db psql -U culturatrip -d culturatrip -f /docker-entrypoint-initdb.d/09_ML_views.sql
docker exec -it culturatrip_db psql -U culturatrip -d culturatrip -f /docker-entrypoint-initdb.d/10_auth.sql

# Segunda pasada: resolver dependencias cruzadas
docker exec -it culturatrip_db psql -U culturatrip -d culturatrip -f /docker-entrypoint-initdb.d/07_new_changes.sql
docker exec -it culturatrip_db psql -U culturatrip -d culturatrip -f /docker-entrypoint-initdb.d/08_alter_tables.sql
docker exec -it culturatrip_db psql -U culturatrip -d culturatrip -f /docker-entrypoint-initdb.d/09_ML_views.sql
```

**¿Por qué dos pasadas?** Los scripts 07, 08 y 09 tienen dependencias cruzadas: por ejemplo, 09_ML_views.sql puede referenciar columnas creadas por 08_alter_tables.sql, y 07_new_changes.sql puede depender de vistas creadas en 09. La primera pasada crea todos los objetos (algunos pueden dar errores por dependencias aún no creadas), y la segunda pasada resuelve esas dependencias porque ahora todos los objetos ya existen.


### Paso 4: Pipeline Cloud-in-Local (Fase 2 — Cloud Computing)

Este pipeline demuestra el flujo de datos en una arquitectura cloud simulada localmente:

```
CSVs (data/clean/) → S3 simulado (LocalStack) → PySpark (limpieza + agregación) → Parquet → PostgreSQL (schema cloud)
```

#### Ejecución rápida (recomendado):

```bash
Scripts/run_cloud_pipeline.bat
```

#### Ejecución manual (en orden):

**Prerequisito:** Instalar boto3 en el contenedor de Spark (solo la primera vez o después de recrear contenedores):

```bash
docker compose exec spark pip install boto3
```

**Etapa 1 — Ingesta a S3 simulado:**

```bash
docker compose run --rm app python cloud_pipeline/01_ingesta_s3.py
```

Sube los 11 CSVs de data/clean/ al bucket `culturatrip-raw` en LocalStack.

**Etapa 2 — Procesamiento con PySpark:**

```bash
docker compose exec spark /opt/spark/bin/spark-submit --packages org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262 /app/cloud_pipeline/02_procesamiento_spark.py
```

Lee CSVs desde S3, limpia datos (nulos, tipos), agrega por provincia y mes, y guarda como Parquet en bucket `culturatrip-gold`.

**Etapa 3 — Carga a PostgreSQL:**

```bash
docker compose run --rm app python cloud_pipeline/03_carga_postgresql.py
```

Lee los Parquet desde S3 y los carga como 13 tablas en PostgreSQL bajo el schema `cloud`.

**Etapa 4 — Data Mart (vistas SQL):**

Las vistas SQL del schema `culturatrip` (vw_ui_*, vw_rec_*, vw_ml_*) ya están creadas en el Paso 3 y funcionan sobre las tablas del schema `public`. No requieren acción adicional.


#### Verificación del pipeline cloud:

```bash
docker exec -it culturatrip_db psql -U culturatrip -d culturatrip -c "SELECT table_name FROM information_schema.tables WHERE table_schema='cloud' ORDER BY table_name;"
```

Resultado esperado: 13 tablas en el schema `cloud`.

### Validación del modelo
    docker exec -it culturatrip_db psql -U culturatrip -d culturatrip -c "
    SELECT * FROM culturatrip.vw_qa_counts_base;"

### Recomendación — Instalación desde cero:

Si quieres re-inicializar todo el sistema desde cero:

```bash
docker compose down -v
docker compose up --build
```

Luego ejecutar en orden:

```bash
Scripts/run_etl_load_data.bat
Scripts/run_views.bat
docker compose exec spark pip install boto3
Scripts/run_cloud_pipeline.bat
```

### Detener el Entorno

```bash
docker compose down
```

## Inicialización de base de datos

El sistema ejecuta automáticamente los scripts SQL ubicados en:

    /sql → /docker-entrypoint-initdb.d

Incluye:

* Creación de esquema

* Creación de tablas

* Creación de vistas

### Orden de Ejecución de Scripts SQL

| Orden | Archivo             | Descripción                                    |
| ----- | ------------------- | ---------------------------------------------- |
| 1     | 01_schema.sql       | Creación del esquema `culturatrip`             |
| 2     | 02_tables.sql       | Tablas base (dimensiones y hechos)             |
| 3     | 03_views.sql        | Vistas iniciales (QA + UI)                     |
| 4     | 04_new_tables.sql   | Tablas transaccionales (plan de viaje)         |
| 5     | 05_new_views.sql    | Modelo de costos                               |
| 6     | 06_index.sql        | Índices de rendimiento                         |
| 7     | 07_new_changes.sql  | Ajustes estructurales                          |
| 8     | 08_alter_tables.sql | Alteraciones de tablas                         |
| 9     | 09_ML_views.sql     | Vistas para Machine Learning                   |
| 10    | 10_auth.sql         | Tabla de autenticación (login de usuarios)     |

Estos scripts completan el modelo e incluyen:

- Tablas transaccionales
- Modelo de costos
- Índices
- Vistas para Machine Learning

## Ejecución del Proceso ETL

    Si el ETL se ejecuta desde el host (Windows/macOS/Linux):
    DB_HOST=localhost

    Si el ETL se ejecuta dentro de Docker:
    DB_HOST=db
 
Se puede referir a la variable de entorno llamado .env

### Nota importante:

     La carpeta `New Model` contiene espacios en su nombre. 
     En algunos entornos (Windows/Mac/Linux) esto puede generar errores al ejecutar los comandos.

    Si ocurre algún problema, se recomienda renombrar la carpeta a:

    src/New_Model

     y actualizar los comandos correspondientes.

## Modelo Físico Implementado (MVP)

### Modelo Transaccional de Plan de Viaje

El sistema incorpora un modelo transaccional que permite gestionar planes de viaje personalizados:

- fact_plan_viaje → cabecera del plan
- fact_plan_viaje_destino → destinos del plan
- fact_plan_viaje_preferencia → actividades deseadas
- fact_plan_gasto_real → gastos reales
- fact_plan_checklist → lista de preparación

Este modelo permite:

- Simulación de costos
- Seguimiento del presupuesto
- Comparación entre costo estimado y gasto real

El modelo dimensional implementado incluye:

Tablas Dimensionales

    dim_pais

    dim_ccaa_base

    dim_provincia

    dim_isla

    dim_municipio

    dim_geografia_municipio_osm

    rel_municipio_isla

Tablas de Hechos

    fact_alojamientos

    fact_actividades

Incluye:

    Claves primarias

    Claves foráneas

    Restricciones de integridad

    Tipificación adecuada (NUMERIC, SMALLINT, BOOLEAN)

## Modelo de Costos

### Rol de las Vistas en la Arquitectura

Las vistas en PostgreSQL cumplen un papel central en el sistema:

- Encapsulan la lógica de negocio
- Preparan datos para la aplicación
- Separan la capa de datos de la capa de presentación
- Permiten reutilización de lógica analítica
- Facilitan integración con Machine Learning

Ejemplos:

- vw_plan_resumen_basico
- vw_plan_costos_estimados
- vw_ml_alojamiento_features_plan

El sistema incorpora un modelo de presupuesto basado en datos oficiales (INE – EGATUR), implementado mediante:

dim_parametros_presupuesto

Distribución del presupuesto:

* Alojamiento → 35%

* Transporte → 25%

* Alimentación → 12%

* Actividades → 28%

Los costos se calculan mediante vistas:

* vw_plan_presupuesto_categoria
* vw_plan_costos_alojamiento
* vw_plan_costos_alimentacion
* vw_plan_costos_transporte
* vw_plan_costos_estimados

El modelo de costos se implementa completamente a nivel de vistas en PostgreSQL, permitiendo desacoplar la lógica de negocio de la aplicación y facilitar su reutilización en diferentes capas del sistema.

## Machine Learning

El sistema integra tres modelos:

### Modelo supervisado (Alojamiento)
- Predicción de precios
- Variables: categoría, temporada, ubicación

### Modelo no supervisado
- Clustering de provincias
- Segmentación de destinos

### Modelo avanzado
- Ranking de destinos
- Evaluación multicriterio (costo + actividades + presupuesto)
- Los modelos se ejecutan en tiempo real desde archivos .pkl.

## Aplicación (Streamlit)

La aplicación está estructurada en un flujo de 8 pantallas:

- Exploración cultural
- Gestión de Planes
- Planificación del viaje
- Resumen del plan
- Presupuesto inteligente
- Control de gastos
- Checklist de viaje
- Resumen final

Consume directamente vistas de PostgreSQL.

## Reproducibilidad

El proyecto ha sido diseñado bajo el principio de:

Reproducibilidad total del entorno, permitiendo ejecutar el sistema completo (datos, modelo y aplicación) mediante Docker sin configuraciones adicionales.


## Pipeline Cloud-in-Local (Asignatura 10 — Fundamentos de Cloud Computing)

### Arquitectura

El pipeline implementa una arquitectura Data Lakehouse simulada localmente con Docker:

```
Fuentes (INE, GeoNames, OSM)
         ↓
  S3 simulado (LocalStack)         ← Capa Raw (Data Lake)
         ↓
  PySpark (limpieza + agregación)  ← Procesamiento
         ↓
  Parquet en S3                    ← Capa Gold
         ↓
  PostgreSQL (schema cloud)        ← Data Warehouse
         ↓
  Streamlit (B2C) + Tableau (B2B)  ← Consumo dual
```

### Componentes del pipeline

| Archivo                               | Etapa                | Descripción                                          |
| ------------------------------------- | -------------------- | ---------------------------------------------------- |
| cloud_pipeline/01_ingesta_s3.py       | Etapa 1 — Raw        | Sube CSVs a bucket S3 en LocalStack con boto3        |
| cloud_pipeline/02_procesamiento_spark.py | Etapa 2 — Gold    | PySpark: limpieza, agregación, guardado en Parquet   |
| cloud_pipeline/03_carga_postgresql.py | Etapa 3 — Warehouse  | Lee Parquet desde S3 y carga a PostgreSQL (schema cloud) |
| sql/03_views.sql (+ 05, 09)          | Etapa 4 — Data Mart  | Vistas SQL optimizadas para BI (ya existentes)       |

### Equivalencia Local → Cloud (AWS)

| Componente local         | Equivalente AWS              |
| ------------------------ | ---------------------------- |
| LocalStack (S3 simulado) | Amazon S3                    |
| PySpark (contenedor)     | AWS Glue / Amazon EMR        |
| PostgreSQL (contenedor)  | Amazon RDS (PostgreSQL)      |
| Streamlit (contenedor)   | AWS App Runner               |
| Tableau Desktop          | Tableau Online / QuickSight  |


## Estructura del Proyecto

```
CulturaTrip_TFM/
│
├── Dockerfile
├── docker-compose.yml
├── requirements.txt
├── .dockerignore
├── .env
│
├── src/
│   ├── App/
│   │   └── Culturaltrip.py
│   ├── New Model/          ← loaders ETL
│   ├── Flat Model/
│   └── Experimental/
│
├── cloud_pipeline/          ← Pipeline Cloud-in-Local (NUEVO)
│   ├── 01_ingesta_s3.py
│   ├── 02_procesamiento_spark.py
│   └── 03_carga_postgresql.py
│
├── data/
│   ├── raw/
│   ├── interim/
│   └── clean/              ← CSVs limpios (fuente del pipeline)
│
├── sql/                     ← Scripts SQL (01 al 10)
│   ├── 01_schema.sql
│   ├── 02_tables.sql
│   ├── 03_views.sql
│   ├── 04_new_tables.sql
│   ├── 05_new_views.sql
│   ├── 06_index.sql
│   ├── 07_new_changes.sql
│   ├── 08_alter_tables.sql
│   ├── 09_ML_views.sql
│   └── 10_auth.sql
│
├── Scripts/                 ← Scripts de ejecución (.bat)
│   ├── run_etl_load_data.bat
│   ├── run_views.bat
│   └── run_cloud_pipeline.bat
│
├── Notebook/
├── MVP/
├── outputs/
├── assets/
├── team_members/
└── README.md
```
## Validación del Entorno

Para verificar que el modelo está correctamente creado:

```bash
docker exec -it culturatrip_db psql -U culturatrip -d culturatrip
```

Luego ejecutar:

```sql
SELECT table_name
FROM information_schema.tables
WHERE table_schema='culturatrip';
```

## Consultas Representativas del proyecto

El modelo soporta consultas analíticas tales como:

### Verificar que las vistas fueron creadas
    
    docker exec -it culturatrip_db psql -U culturatrip -d culturatrip -c "
    SELECT viewname
    FROM pg_views
    WHERE schemaname = 'culturatrip'
    ORDER BY viewname;"

### Validación – Vistas de Control de Calidad (QA)

Validación – Vistas de Control de Calidad (QA)

     docker exec -it culturatrip_db psql -U culturatrip -d culturatrip -c "
     SELECT * FROM culturatrip.vw_qa_counts_base;"

Verificar duplicados en actividades (debe devolver 0 filas)
    
     docker exec -it culturatrip_db psql -U culturatrip -d culturatrip -c "
     SELECT * FROM culturatrip.vw_qa_fact_actividades_duplicados;"

Verificar duplicados en alojamientos (debe devolver 0 filas)
  
    docker exec -it culturatrip_db psql -U culturatrip -d culturatrip -c "
    SELECT * FROM culturatrip.vw_qa_fact_alojamientos_duplicados;"

Verificar FKs municipio → provincia (debe devolver 0 filas)

    docker exec -it culturatrip_db psql -U culturatrip -d culturatrip -c "
    SELECT * FROM culturatrip.vw_qa_dim_muni_fk_prov_missing;"

Verificar municipios sin geolocalización

    docker exec -it culturatrip_db psql -U culturatrip -d culturatrip -c "
    SELECT COUNT(*) 
    FROM culturatrip.vw_qa_municipios_sin_geo;"

### Validación – Pantalla 1 (Vista UI)

Validación – Pantalla 1 (Vista UI)

Vista Global (1 sola fila)

    docker exec -it culturatrip_db psql -U culturatrip -d culturatrip -c "
    SELECT * 
    FROM culturatrip.vw_ui_pantalla1_global;"

Resultado esperado (ejemplo):

| total_paises | total_provincias | total_municipios | total_islas |
| ------------ | ---------------- | ---------------- | ----------- |
| 249          | 52               | 8132             | 11          |

Vista Detalle por País (Ejemplo España)

    docker exec -it culturatrip_db psql -U culturatrip -d culturatrip -c "
    SELECT * 
    FROM culturatrip.vw_ui_pantalla1_detalle_por_pais
    WHERE id_pais = 'ES';"

Resultado esperado:

| id_pais | pais   | total_provincias | total_municipios | total_islas |
| ------- | ------ | ---------------- | ---------------- | ----------- |
| ES      | España | 52               | 8132             | 11          |

Validación – Cobertura Geográfica

% de cobertura OSM por país

    docker exec -it culturatrip_db psql -U culturatrip -d culturatrip -c "
    SELECT *
    FROM culturatrip.vw_ui_geo_coverage_por_pais;"


Cantidad de actividades por provincia

    docker exec -it culturatrip_db psql -U culturatrip -d culturatrip -c "
    SELECT
     p.id_provincia,
     p.provincia_nombre,
     COUNT(*) AS total_actividades
    FROM culturatrip.fact_actividades fa
    JOIN culturatrip.dim_provincia p
     ON p.id_provincia = fa.id_provincia
    GROUP BY p.id_provincia, p.provincia_nombre
    ORDER BY total_actividades DESC LIMIT 10;"

Cantidad de hospedaje (alojamientos) por provincia

    docker exec -it culturatrip_db psql -U culturatrip -d culturatrip -c "
    SELECT
     p.id_provincia,
     p.provincia_nombre,
     COUNT(*) AS total_hospedajes
    FROM culturatrip.fact_alojamientos fal
    JOIN culturatrip.dim_provincia p
     ON p.id_provincia = fal.id_provincia
    GROUP BY p.id_provincia, p.provincia_nombre
    ORDER BY total_hospedajes DESC LIMIT 10;"

Otras validaciones:

     docker exec -it culturatrip_db psql -U culturatrip -d culturatrip -c "
     SELECT COUNT(*) FROM culturatrip.fact_actividades;"

     docker exec -it culturatrip_db psql -U culturatrip -d culturatrip -c "
     SELECT * FROM culturatrip.vw_plan_costos_estimados LIMIT 5;"

Si las consultas devuelven resultados, el sistema está correctamente cargado y listo para su uso.

## Limitaciones del MVP

- No se implementan índices avanzados por volumen académico reducido.
- No se incluyen procesos de actualización en tiempo real.
- No se contempla particionamiento por ahora.
- Se limita el alcance geográfico a España.
- Futuras iteraciones podrán incorporar: optimización de consultas, escalabilidad horizontal, integración de datasets adicionales, orquestación con Airflow, despliegue en Kubernetes.


## Valor Diferencial del Proyecto

CulturaTrip no solo analiza datos turísticos, sino que:

- Integra datos territoriales, económicos y de comportamiento
- Permite planificación real de viajes (no solo análisis)
- Incorpora un modelo de costos basado en fuentes oficiales (INE)
- Combina analítica, Machine Learning y experiencia de usuario
- Implementa un pipeline Cloud-in-Local que demuestra competencias en arquitectura cloud
- Permite simulación y seguimiento real del viaje
- Ofrece consumo dual: Streamlit para el turista (B2C) y Tableau para instituciones (B2B)

## Conclusión

CulturaTrip representa una solución integral de planificación de turismo cultural basada en datos, que combina:

- Ingeniería de datos

- Modelado relacional

- Machine Learning

- Visualización interactiva

- Todo dentro de un entorno reproducible y escalable.



