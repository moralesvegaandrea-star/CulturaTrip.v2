# 🌍 CulturaTrip – Plataforma Inteligente de Análisis Turístico

## Trabajo Final de Máster – Big Data & Business Intelligence

**Autora:** Andrea Morales Vega


## Descripción del Proyecto

CulturaTrip es una plataforma de análisis turístico basada en datos, desarrollada como parte del Trabajo Final de Máster (TFM).

El proyecto implementa la capa de persistencia del sistema mediante un modelo relacional en PostgreSQL, integrando procesos ETL desarrollados en Python, análisis exploratorio (EDA) y visualización interactiva mediante Streamlit.
El objetivo académico de este repositorio es demostrar:
Selección razonada de SGBD
Modelado de datos apropiado
Implementación física reproducible
Consultas representativas alineadas con los casos de uso del TFM
Ejecución completamente local sin dependencias externas

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

Para cargar los datos en el modelo:

    python src/"New Model"/Paises_load_postgres.py

Los scripts ETL:

Realizan limpieza y transformación

Normalizan datos

Insertan en tablas del esquema culturatrip

Garantizan coherencia con el modelo relacional


## Consultas Representativas del TFM

El modelo soporta consultas analíticas tales como:

     Validacion de Control de Calidad
     docker exec -it culturatrip_db psql -U culturatrip -d culturatrip -c "SELECT COUNT(*) FROM culturatrip.dim_pais;"

     Gasto medio por Comunidad Autónoma

    Ranking de actividades por provincia

    Evolución mensual de precios de alojamiento

    Análisis geoespacial por municipio

Estas consultas demuestran:

    Uso eficiente de JOIN

    Integridad referencial

    Agregaciones temporales

Soporte para análisis estratégico turístico

    Las vistas correspondientes se encuentran en 03_views.sql.


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



