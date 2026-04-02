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

Procesamiento de datos (ETL)

Modelado relacional en PostgreSQL

Vistas analíticas

Modelos de Machine Learning

Aplicación interactiva en Streamlit

Todo el entorno está diseñado bajo un enfoque de reproducibilidad completa mediante Docker.

Datos necesarios para reproducir: listar los CSV se encuentran en la direccion data/clean/.



## Objetivo

Desarrollar un sistema que permita:

Planificar viajes culturales de forma personalizada

Estimar costos por categoría (alojamiento, transporte, alimentación, actividades)

Recomendar destinos óptimos

Analizar la viabilidad del viaje según presupuesto

Integrar modelos de Machine Learning en la toma de decisiones

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

Integridad referencial robusta (claves foráneas)

Soporte avanzado para consultas analíticas

Escalabilidad

Gestión de tipos numéricos de precisión financiera (NUMERIC)

Compatibilidad con Docker para ejecución local

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
        Streamlit App

## Infraestructura (Docker)

El proyecto se ejecuta mediante Docker Compose con tres servicios principales:

db → PostgreSQL 16 (base de datos)

app → ejecución de ETL y aplicación Streamlit

notebook → entorno Jupyter para análisis

    Definido en:

    docker-compose.yml

## Flujo Operativo del Proyecto

1)  Levantar el entorno

         docker compose up --build

Esto inicia o da acceso a los servicios

PostgreSQL → puerto 5433

| Servicio             | URL                                            |
| -------------------- | ---------------------------------------------- |
| Aplicación Streamlit | [http://localhost:8501](http://localhost:8501) |
| Jupyter Notebook     | [http://localhost:8888](http://localhost:8888) |


### Recomendación:
#### Construir y levantar entorno
Desde la raíz del proyecto
Si ya existe el volumen y quieres re-inicializar desde cero: 

     docker compose down -v 
y luego 

    docker compose up --build.

## Detener el Entorno

Detener servicios:

    docker compose down

## Inicialización de base de datos

El sistema ejecuta automáticamente los scripts SQL ubicados en:

    /sql → /docker-entrypoint-initdb.d

Incluye:

Creación de esquema

Creación de tablas

Creación de vistas

###  Orden de Ejecución de Scripts SQL
1. 01_schema.sql → creación del esquema
2. 02_tables.sql → tablas base
3. 03_views.sql → vistas iniciales (QA + UI)
4. 04_new_tables.sql → tablas transaccionales
5. 05_new_views.sql → modelo de costos
6. 06_index.sql → índices
7. 07_new_changes.sql → ajustes estructurales
8. 08_alter_tables.sql → alteraciones
9. 09_ML_views.sql → vistas para Machine Learning

Los scripts SQL deben ejecutarse en el siguiente orden:

    Scripts/run_views.bat

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

###  Ejecución rápida (recomendado)

Para ejecutar todo el proceso ETL de forma automática:

    Scripts/run_etl_load_data.bat

### Nota importante:

     La carpeta `New Model` contiene espacios en su nombre. 
     En algunos entornos (Windows/Mac/Linux) esto puede generar errores al ejecutar los comandos.

    Si ocurre algún problema, se recomienda renombrar la carpeta a:

    src/New_Model

     y actualizar los comandos correspondientes.

### Ejecución Manual 

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

Los scripts loaders:

Insertan en tablas del esquema culturatrip

Garantizan coherencia con el modelo relacional

### Validación del modelo
    docker exec -it culturatrip_db psql -U culturatrip -d culturatrip -c "
    SELECT * FROM culturatrip.vw_qa_counts_base;"

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

Alojamiento → 35%

Transporte → 25%

Alimentación → 12%

Actividades → 28%

Los costos se calculan mediante vistas:

vw_plan_presupuesto_categoria
vw_plan_costos_alojamiento
vw_plan_costos_alimentacion
vw_plan_costos_transporte
vw_plan_costos_estimados

El modelo de costos se implementa completamente a nivel de vistas en PostgreSQL, permitiendo desacoplar la lógica de negocio de la aplicación y facilitar su reutilización en diferentes capas del sistema.
## Machine Learning

El sistema integra tres modelos:

### Modelo supervisado (Alojamiento)

- Predicción de precios

- Variables: categoría, temporada, ubicación

### Modelo no supervisado

Clustering de provincias

Segmentación de destinos

### Modelo avanzado

- Ranking de destinos

- Evaluación multicriterio (costo + actividades + presupuesto)

- Los modelos se ejecutan en tiempo real desde archivos .pkl.

## Aplicación (Streamlit)

La aplicación está estructurada en un flujo de 8 pantallas:

- Exploración cultural
- Gestion de Planes
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


## Estructura del Proyecto
    CulturaTrip_TFM/
    │
    ├── Dockerfile
    ├── docker-compose.yml
    ├── requirements.txt
    ├── .dockerignore
    │
    ├── src/
    │   ├── App/
    │   │   └── Culturaltrip.py
    │   ├── New Model/
    │   ├── Flat Model/
    │   └── Experimental/
    │
    ├── data/
    │   ├── raw/
    │   ├── interim/
    │   └── clean/
    │
    ├── Notebook/
    └── README.md


## Consultas Representativas del TFM

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


Soporte para análisis estratégico turístico

    Las vistas correspondientes se encuentran en 03_views.sql.

Estas consultas demuestran:

Las vistas definidas en 03_views.sql cumplen dos funciones:

Control de Calidad (QA):

Validan duplicados, integridad referencial y consistencia geográfica.

Capa de Presentación (UI):

Preparan datos agregados listos para ser consumidos por Streamlit.

La vista vw_ui_pantalla1_global permite mostrar en la primera pantalla de la aplicación el número total de países, provincias, municipios e islas disponibles.

La vista vw_ui_pantalla1_detalle_por_pais permite mostrar información cultural y territorial específica para el país seleccionado por el usuario.

Esta separación garantiza:

- Reproducibilidad

- Optimización de consultas

- Arquitectura modular (DB → Views → Streamlit)

## Validación del Entorno

Para verificar que el modelo está correctamente creado:

    docker exec -it culturatrip_db psql -U culturatrip -d culturatrip

Luego ejecutar:

     SELECT table_name 
     FROM information_schema.tables
     WHERE table_schema='culturatrip';


Otras validaciones:

     docker exec -it culturatrip_db psql -U culturatrip -d culturatrip -c "
     SELECT COUNT(*) FROM culturatrip.fact_actividades;"

     docker exec -it culturatrip_db psql -U culturatrip -d culturatrip -c "
     SELECT * FROM culturatrip.vw_plan_costos_estimados LIMIT 5;"

Si las consultas devuelven resultados, el sistema está correctamente cargado y listo para su uso.

## Optimización

Se implementan índices para mejorar el rendimiento:

- Índices en planes por usuario y fechas
- Índices en relaciones de destino
- Índices en dimensiones clave

Ejemplo:

- ux_plan_unico → evita duplicados de planes

## Limitaciones del MVP

- No se implementan índices avanzados por volumen académico reducido.

- No se incluyen procesos de actualización en tiempo real.

- No se contempla particionamiento por ahora.

- Se limita el alcance geográfico a España.

- Futuras iteraciones podrán incorporar:

- Optimización de consultas

- Escalabilidad horizontal

- Integración de datasets adicionales

- Modelos de Machine Learning basados en agregaciones


## Valor Diferencial del Proyecto

CulturaTrip no solo analiza datos turísticos, sino que:

- Integra datos territoriales, económicos y de comportamiento
- Permite planificación real de viajes (no solo análisis)
- Incorpora un modelo de costos basado en fuentes oficiales (INE)
- Combina analítica, Machine Learning y experiencia de usuario
- Permite simulación y seguimiento real del viaje

Esto lo diferencia de sistemas tradicionales de recomendación turística.

## Conclusión

CulturaTrip representa una solución integral de planificación de turismo cultural basada en datos, que combina:


- Ingeniería de datos

- Modelado relacional

- Machine Learning

- Visualización interactiva

- Todo dentro de un entorno reproducible y escalable.



