/*
==================================================
Paso_2 Crear Tablas
==================================================
*/

/*
==================================================
Dimensión: dim_pais
Descripción: Tabla maestra de países
Grano: 1 fila por país
Uso: Base para jerarquía geográfica
==================================================
*/
DROP TABLE IF EXISTS culturatrip.dim_pais cascade;

CREATE TABLE culturatrip.dim_pais (
    id_pais CHAR(2) PRIMARY KEY,
    pais VARCHAR(60) NOT NULL,
    lat NUMERIC(10,7),
    lon NUMERIC(10,7),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

/*
==================================================
Dimensión: dim_ccaa_base
Descripción: Tabla maestra de comuidades autonomas
==================================================
*/
DROP TABLE IF EXISTS culturatrip.dim_ccaa_base cascade;

CREATE TABLE culturatrip.dim_ccaa_base (
   id_ccaa CHAR(2) NOT NULL PRIMARY KEY,
   id_pais CHAR(2) NOT NULL,
    gid_ccaa VARCHAR(5),
    ccaa_nombre VARCHAR(40) NOT NULL,

    CONSTRAINT fk_ccaa_pais
        FOREIGN KEY (id_pais)
        REFERENCES culturatrip.dim_pais(id_pais)
        ON DELETE RESTRICT,

    CONSTRAINT unique_ccaa_por_pais
        UNIQUE (id_pais, ccaa_nombre)
);
/*
==================================================
Dimensión: dim_provincia
Descripción: Tabla maestra de provincias
==================================================
*/
DROP TABLE IF EXISTS culturatrip.dim_provincia cascade;

CREATE TABLE culturatrip.dim_provincia (
    id_pais CHAR(2) NOT NULL,
    id_provincia CHAR(2) NOT NULL,
    gid_provincia VARCHAR(5),
    provincia_nombre VARCHAR(30) NOT NULL,

    PRIMARY KEY (id_provincia),

    CONSTRAINT fk_provincia_pais
        FOREIGN KEY (id_pais)
        REFERENCES culturatrip.dim_pais(id_pais)
        ON DELETE RESTRICT,

    CONSTRAINT unique_provincia_por_pais
        UNIQUE (id_pais, provincia_nombre)
);
/*
==================================================
Dimensión: dim_isla
Descripción: Tabla maestra de islas
==================================================
*/
DROP TABLE IF EXISTS culturatrip.dim_isla cascade;
CREATE TABLE culturatrip.dim_isla (
    id_pais CHAR(2) NOT NULL,
    id_isla CHAR(5) NOT NULL,
    id_provincia CHAR(2) NOT NULL,
    gid_isla VARCHAR(8),
    isla VARCHAR(20),
    provincia_header VARCHAR(30),

    PRIMARY KEY (id_isla),

    CONSTRAINT fk_isla_pais
        FOREIGN KEY (id_pais)
        REFERENCES culturatrip.dim_pais(id_pais)
        ON DELETE RESTRICT,

    CONSTRAINT fk_isla_provincia
        FOREIGN KEY (id_provincia)
        REFERENCES culturatrip.dim_provincia(id_provincia)
        ON DELETE RESTRICT
);

/*
==================================================
Dimensión: dim_municipio
Descripción: Tabla de municipios de Region
==================================================
*/
DROP TABLE IF EXISTS culturatrip.dim_municipio CASCADE;

CREATE TABLE culturatrip.dim_municipio (
    id_pais CHAR(2) NOT NULL,
    id_municipio CHAR(8) NOT NULL,
    id_municipio_parcial CHAR(6) NOT NULL,
    id_provincia CHAR(2) NOT NULL,
    id_ccaa CHAR(2) NOT NULL,
    id_isla CHAR(5),

    nombre VARCHAR(60) NOT NULL,
    provincia_nombre VARCHAR(30) NOT NULL,
    ccaa_nombre VARCHAR(40) NOT NULL,
    isla VARCHAR(30),

    gid_municipio CHAR(20) NOT NULL,
    gid_provincia CHAR(8) NOT NULL,
    gid_ccaa CHAR(8) NOT NULL,

    PRIMARY KEY (id_municipio),

    CONSTRAINT fk_muni_pais
        FOREIGN KEY (id_pais)
        REFERENCES culturatrip.dim_pais(id_pais)
        ON DELETE RESTRICT,

    CONSTRAINT fk_muni_ccaa
        FOREIGN KEY (id_ccaa)
        REFERENCES culturatrip.dim_ccaa_base(id_ccaa)
        ON DELETE RESTRICT,

    CONSTRAINT fk_muni_provincia
        FOREIGN KEY (id_provincia)
        REFERENCES culturatrip.dim_provincia(id_provincia)
        ON DELETE RESTRICT,

    CONSTRAINT fk_muni_isla
        FOREIGN KEY (id_isla)
        REFERENCES culturatrip.dim_isla(id_isla)
        ON DELETE RESTRICT
);

/*
==================================================
Dimensión: rel_municipio_isla
Descripción: Tabla puente
==================================================
*/
DROP TABLE IF EXISTS culturatrip.rel_municipio_isla cascade;

CREATE TABLE culturatrip.rel_municipio_isla (
    id_pais CHAR(2) NOT NULL,
    id_municipio CHAR(8) NOT NULL,
    id_municipio_parcial CHAR(6),
    id_isla CHAR(5) NOT NULL,
    gid_isla VARCHAR(8),
    isla VARCHAR(20),
    gid_municipio VARCHAR(11),

    PRIMARY KEY (id_municipio, id_isla),

    CONSTRAINT fk_rel_pais
        FOREIGN KEY (id_pais)
        REFERENCES culturatrip.dim_pais(id_pais)
        ON DELETE RESTRICT,

    CONSTRAINT fk_rel_municipio
        FOREIGN KEY (id_municipio)
        REFERENCES culturatrip.dim_municipio(id_municipio)
        ON DELETE RESTRICT,

    CONSTRAINT fk_rel_isla
        FOREIGN KEY (id_isla)
        REFERENCES culturatrip.dim_isla(id_isla)
        ON DELETE RESTRICT
);
/*
==================================================
Dimensión: dim_geografia_municipio_osm
Descripción: Tabla geoespacial
==================================================
*/
DROP TABLE IF EXISTS culturatrip.dim_geografia_municipio_osm CASCADE;

CREATE TABLE culturatrip.dim_geografia_municipio_osm (

    id_municipio VARCHAR(8) PRIMARY KEY,

    osm_id VARCHAR(15) NOT NULL,
    osm_type VARCHAR(10) NOT NULL,
    osm_query_usada VARCHAR(150) NOT NULL,
    osm_pass VARCHAR(150) NOT NULL,

    lat NUMERIC(10,7) NOT NULL CHECK (lat BETWEEN -90 AND 90),
    lon NUMERIC(10,7) NOT NULL CHECK (lon BETWEEN -180 AND 180),

    CONSTRAINT fk_geo_municipio
        FOREIGN KEY (id_municipio)
        REFERENCES culturatrip.dim_municipio(id_municipio)
        ON DELETE RESTRICT,

    CONSTRAINT unique_osm_id UNIQUE (osm_id)

);
/*
==================================================
Dimensión: dim_actividades
Descripción: Tabla actividades sociales region
==================================================
*/
DROP TABLE IF EXISTS culturatrip.fact_actividades CASCADE;

CREATE TABLE culturatrip.fact_actividades (

    id_actividad BIGSERIAL PRIMARY KEY,

    id_pais VARCHAR(2) NOT NULL,
    id_ccaa VARCHAR(2) NOT NULL,
    id_provincia VARCHAR(2) NOT NULL,

    mes SMALLINT NOT NULL CHECK (mes BETWEEN 1 AND 12),

    categoria VARCHAR(40) NOT NULL,
    producto VARCHAR(40) NOT NULL,
    subcategoria VARCHAR(40) NOT NULL,

    comunidad_autonoma VARCHAR(40) NOT NULL,
    provincia VARCHAR(40) NOT NULL,

    gasto_total_promedio NUMERIC(12,2) NOT NULL,
    precio_medio_entrada_promedio NUMERIC(10,2) NOT NULL,

    valoracion_por_categoria_promedio NUMERIC(3,2),
    valoracion_general_promedio NUMERIC(3,2),
    total_opiniones_categoria_promedio INTEGER,

    hay_valoracion BOOLEAN NOT NULL,

    -- 🔐 Claves foráneas
    CONSTRAINT fk_fact_act_pais
        FOREIGN KEY (id_pais)
        REFERENCES culturatrip.dim_pais(id_pais)
        ON DELETE RESTRICT,

    CONSTRAINT fk_fact_act_ccaa
        FOREIGN KEY (id_ccaa)
        REFERENCES culturatrip.dim_ccaa_base(id_ccaa)
        ON DELETE RESTRICT,

    CONSTRAINT fk_fact_act_provincia
        FOREIGN KEY (id_provincia)
        REFERENCES culturatrip.dim_provincia(id_provincia)
        ON DELETE RESTRICT,

    --  Evitar duplicados por llave natural
    CONSTRAINT unique_fact_actividades
        UNIQUE (id_pais, id_ccaa, id_provincia, mes, categoria, producto, subcategoria)

);
/*
==================================================
Dimensión: dim_alojamientos
Descripción: Tabla alojamientos por region
==================================================
*/
DROP TABLE IF EXISTS culturatrip.fact_alojamientos CASCADE;

CREATE TABLE culturatrip.fact_alojamientos (

    id_alojamiento BIGSERIAL PRIMARY KEY,

    id_pais VARCHAR(2) NOT NULL,
    id_ccaa VARCHAR(2) NOT NULL,
    id_provincia VARCHAR(2) NOT NULL,

    mes SMALLINT NOT NULL CHECK (mes BETWEEN 1 AND 12),

    categoria_alojamiento VARCHAR(30),
    periodo_antelacion VARCHAR(40),

    fuente VARCHAR(40),
    granularidad_origen VARCHAR(30),
    nivel_geografico VARCHAR(30),

    precio_checkin_entre_semana NUMERIC(10,2),
    precio_checkin_fin_semana NUMERIC(10,2),

    valoraciones_norm NUMERIC(5,2),

    tiene_valoraciones BOOLEAN,
    es_dato_replicado BOOLEAN,

    -- Claves foráneas
    CONSTRAINT fk_fact_aloj_pais
        FOREIGN KEY (id_pais)
        REFERENCES culturatrip.dim_pais(id_pais)
        ON DELETE RESTRICT,

    CONSTRAINT fk_fact_aloj_ccaa
        FOREIGN KEY (id_ccaa)
        REFERENCES culturatrip.dim_ccaa_base(id_ccaa)
        ON DELETE RESTRICT,

    CONSTRAINT fk_fact_aloj_provincia
        FOREIGN KEY (id_provincia)
        REFERENCES culturatrip.dim_provincia(id_provincia)
        ON DELETE RESTRICT,

    -- Evitar duplicados por llave natural
    CONSTRAINT unique_fact_alojamientos
        UNIQUE (
            id_pais,
            id_ccaa,
            id_provincia,
            mes,
            categoria_alojamiento,
            periodo_antelacion
        )

);
-- ==================================================
-- FIN DEL SCRIPT
-- Modelo CulturaTrip listo para carga de datos
-- ==================================================