# 🌍 CulturaTrip – Plataforma Inteligente de Análisis Turístico

## Trabajo Final de Máster – Big Data & Business Intelligence

**Autores:** 

Ana Belén Chaves Jiménez

Hilda Mireya Ibarra Mata

Montserrat Ulloa Álvarez 

Andrea Lucia Morales Vega 

Ronald Rojas Barquero


## Descripción del Proyecto

CulturaTrip es una plataforma de análisis turístico basada en datos, desarrollada como parte del Trabajo Final de Máster (TFM).

El proyecto implementa la capa de persistencia del sistema mediante un modelo relacional en PostgreSQL, integrando procesos ETL desarrollados en Python, análisis exploratorio (EDA) y visualización interactiva mediante Streamlit.
El objetivo académico de este repositorio es demostrar:
Selección razonada de SGBD
Modelado de datos apropiado
Implementación física reproducible
Consultas representativas alineadas con los casos de uso del TFM
Ejecución completamente local sin dependencias externas

Datos necesarios para reproducir: listar los CSV se encuentran en la direccion data/clean/.


## Datasets Utilizados

El sistema integra los siguientes datasets:
   
| Dataset                          | Objetivo                         | Origen                  | Tipo    |
| -------------------------------- | -------------------------------- | ----------------------- | ------- |
| División político-administrativa | Estructura jerárquica geográfica | INE / fuentes oficiales | Tabular |
| Georreferenciación municipal     | Coordenadas OSM                  | OpenStreetMap           | Tabular |
| Alojamiento turístico            | Análisis de precios y demanda    | Dataestur               | Tabular |
| Actividades culturales           | Gasto y valoración turística     | Dataestur               | Tabular |

Cada dataset ha sido caracterizado considerando:

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

El proyecto sigue una arquitectura modular en capas:

1) Capa de Datos 

   PostgreSQL 16 ejecutado en contenedor Docker.

2) Capa de Procesamiento

   Módulos ETL desarrollados en Python dentro de 
               
        /src.

3) Capa de Aplicación

   Dashboard interactivo desarrollado en Streamlit 
         
       (src/App/Culturaltrip.py).

4) Capa de Análisis (EDA)

   Notebooks en 

        /Notebook para exploración, limpieza y validación de datos.

5) Capa de Infraestructura

   Entorno completamente aislado mediante Docker y Docker Compose.

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

## Modelo Físico Implementado (MVP)

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

## Inicialización Automática del Modelo
Al ejecutar:

    docker compose up --build

Se inicializa automáticamente:

Creación de esquema (01_schema.sql)

Creación de tablas (02_tables.sql)

Creación de vistas representativas (03_views.sql)

Estos archivos se encuentran en la carpeta /sql y son ejecutados automáticamente por PostgreSQL al inicializar el volumen.

Recomendación:

Si ya existe el volumen y quieres re-inicializar desde cero: 

docker compose down -v y 

luego docker compose up --build”.

### Construir y levantar entorno

Desde la raíz del proyecto
    
    docker compose up --build

Acceso a los servicios
                
| Servicio             | URL                                            |
| -------------------- | ---------------------------------------------- |
| Aplicación Streamlit | [http://localhost:8501](http://localhost:8501) |
| Jupyter Notebook     | [http://localhost:8888](http://localhost:8888) |


## Ejecución del Proceso ETL

    Si el ETL se ejecuta desde el host (Windows/macOS/Linux):
    DB_HOST=localhost

    Si el ETL se ejecuta dentro de Docker:
    DB_HOST=db
 
Se puede referir a la variable de entoro llamada .env

Para cargar los datos en el modelo:

     python src/"New Model"/Paises_load_postgres.py
     python src/"New Model"/Comunidad_Autonomas_New_Model_load_postgres.py
     python src/"New Model"/Provincias_new_model_load_postgres.py
     python src/"New Model"/Islas_v2_new_model_load_postgres.py
     python src/"New Model"/Division_Politica_load_postgres.py
     python src/"New Model"/rel_municipio_isla_load_postgres.py
     python src/"New Model"/OpenstreetMap_load_postgres.py
    python src/"New Model"/Actividades_load_postgres.py
    python src/"New Model"/Alojamientos_load_postgres.py

Los scripts loaders:

Insertan en tablas del esquema culturatrip

Garantizan coherencia con el modelo relacional


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

Reproducibilidad

Optimización de consultas

Arquitectura modular (DB → Views → Streamlit)

## Validación del Entorno

Para verificar que el modelo está correctamente creado:

    docker exec -it culturatrip_db psql -U culturatrip -d culturatrip

Luego ejecutar:

     SELECT table_name 
     FROM information_schema.tables
     WHERE table_schema='culturatrip';

## Reproducibilidad

La solución es completamente reproducible en local y no requiere:

Servicios en la nube

Licencias privativas

Dependencias externas

Todo el entorno se levanta mediante Docker.

## Limitaciones del MVP

No se implementan índices avanzados por volumen académico reducido.

No se incluyen procesos de actualización en tiempo real.

No se contempla particionamiento por ahora.

Se limita el alcance geográfico a España.

Futuras iteraciones podrán incorporar:

Optimización de consultas

Escalabilidad horizontal

Integración de datasets adicionales

## Detener el Entorno

Detener servicios:

    docker compose down

Eliminar completamente el entorno (incluyendo base de datos):

    docker compose down -v

## Conclusión

Este repositorio implementa una versión funcional (MVP) del modelo de persistencia del TFM CulturaTrip, cumpliendo con los requisitos académicos de:

Modelado relacional justificado

Implementación reproducible

Consultas representativas

Ejecución local sin dependencias externas



