-- =========================
-- dim_tiempo (simple mensual)
-- =========================
DROP TABLE IF EXISTS culturatrip.dim_tiempo;

CREATE TABLE culturatrip.dim_tiempo (
  anio        SMALLINT NOT NULL,
  mes         SMALLINT NOT NULL CHECK (mes BETWEEN 1 AND 12),
  nombre_mes  VARCHAR(15) NOT NULL,
  trimestre   SMALLINT NOT NULL CHECK (trimestre BETWEEN 1 AND 4),
  temporada   VARCHAR(10) NOT NULL CHECK (temporada IN ('alta','media','baja')),
  PRIMARY KEY (anio, mes)
);

-- Poblar rango sugerido (ajusta si quieres)
INSERT INTO culturatrip.dim_tiempo (anio, mes, nombre_mes, trimestre, temporada)
SELECT
  y.anio,
  m.mes,
  m.nombre_mes,
  ((m.mes - 1) / 3 + 1)::smallint AS trimestre,
  CASE
    WHEN m.mes IN (7,8,12) THEN 'alta'
    WHEN m.mes IN (4,5,6,9,10) THEN 'media'
    ELSE 'baja'
  END AS temporada
FROM (SELECT generate_series(2020, 2030) AS anio) y
CROSS JOIN (
  VALUES
    (1,'Enero'),(2,'Febrero'),(3,'Marzo'),(4,'Abril'),
    (5,'Mayo'),(6,'Junio'),(7,'Julio'),(8,'Agosto'),
    (9,'Septiembre'),(10,'Octubre'),(11,'Noviembre'),(12,'Diciembre')
) AS m(mes, nombre_mes);

CREATE INDEX IF NOT EXISTS idx_dim_tiempo_temporada
ON culturatrip.dim_tiempo (temporada);

-- =========================
-- fact_plan_viaje (cabecera)
-- =========================
DROP TABLE IF EXISTS culturatrip.fact_plan_viaje_preferencia;
DROP TABLE IF EXISTS culturatrip.fact_plan_viaje_destino;
DROP TABLE IF EXISTS culturatrip.fact_plan_viaje;

CREATE TABLE culturatrip.fact_plan_viaje (
  id_plan              BIGSERIAL PRIMARY KEY,

  email_usuario        VARCHAR(120) NOT NULL,

  id_pais_origen       CHAR(2) NOT NULL,
  id_pais_destino      CHAR(2) NOT NULL,

  fecha_ida            DATE NOT NULL,
  fecha_regreso        DATE NOT NULL,

  presupuesto_estimado NUMERIC(12,2) NOT NULL CHECK (presupuesto_estimado >= 0),

  -- Tipo de viaje fijo según alcance
  tipo_viaje           VARCHAR(20) NOT NULL DEFAULT 'Solo',

  -- Tipo de hospedaje viene de fact_alojamientos.categoria_alojamiento
  categoria_alojamiento VARCHAR(30) NOT NULL,

  created_at           TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

  CONSTRAINT chk_fechas_plan CHECK (fecha_regreso >= fecha_ida),

  CONSTRAINT fk_plan_pais_origen
    FOREIGN KEY (id_pais_origen) REFERENCES culturatrip.dim_pais(id_pais)
    ON DELETE RESTRICT,

  CONSTRAINT fk_plan_pais_destino
    FOREIGN KEY (id_pais_destino) REFERENCES culturatrip.dim_pais(id_pais)
    ON DELETE RESTRICT
);

CREATE INDEX IF NOT EXISTS idx_plan_email ON culturatrip.fact_plan_viaje (email_usuario);
CREATE INDEX IF NOT EXISTS idx_plan_destino_fecha ON culturatrip.fact_plan_viaje (id_pais_destino, fecha_ida);

-- =========================
-- fact_plan_viaje_destino (detalle)
-- =========================
CREATE TABLE culturatrip.fact_plan_viaje_destino (
  id_plan      BIGINT NOT NULL REFERENCES culturatrip.fact_plan_viaje(id_plan) ON DELETE CASCADE,
  orden        SMALLINT NOT NULL CHECK (orden >= 1),
  id_provincia VARCHAR(2) NOT NULL,

  PRIMARY KEY (id_plan, orden),

  CONSTRAINT fk_plan_destino_provincia
    FOREIGN KEY (id_provincia) REFERENCES culturatrip.dim_provincia(id_provincia)
    ON DELETE RESTRICT
);

CREATE INDEX IF NOT EXISTS idx_plan_destino_provincia
ON culturatrip.fact_plan_viaje_destino (id_provincia);

-- =========================
-- fact_plan_viaje_preferencia (categorías deseadas)
-- =========================
CREATE TABLE culturatrip.fact_plan_viaje_preferencia (
  id_plan   BIGINT NOT NULL REFERENCES culturatrip.fact_plan_viaje(id_plan) ON DELETE CASCADE,
  categoria VARCHAR(40) NOT NULL,
  PRIMARY KEY (id_plan, categoria)
);

CREATE INDEX IF NOT EXISTS idx_plan_pref_categoria
ON culturatrip.fact_plan_viaje_preferencia (categoria);
-- ==================================================
-- FIN DEL SCRIPT
-- Version 1
-- ==================================================

-- ==================================================
-- Tabla Plan de Gastos
-- Version 2
-- ==================================================

CREATE TABLE culturatrip.fact_plan_gasto_real (
  id_gasto     BIGSERIAL PRIMARY KEY,
  id_plan      BIGINT NOT NULL REFERENCES culturatrip.fact_plan_viaje(id_plan) ON DELETE CASCADE,
  fecha        DATE NOT NULL,
  categoria    VARCHAR(30) NOT NULL,
  descripcion  VARCHAR(150),
  monto        NUMERIC(10,2) NOT NULL CHECK (monto >= 0),
  created_at   TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- ==================================================
-- Tabla Control de Gastos
-- Version 2
-- ==================================================
CREATE TABLE IF NOT EXISTS culturatrip.fact_plan_gasto_real (
  id_gasto      BIGSERIAL PRIMARY KEY,
  id_plan       BIGINT NOT NULL REFERENCES culturatrip.fact_plan_viaje(id_plan) ON DELETE CASCADE,
  fecha         DATE NOT NULL,
  categoria     VARCHAR(30) NOT NULL,
  descripcion   VARCHAR(150),
  monto         NUMERIC(10,2) NOT NULL CHECK (monto >= 0),
  created_at    TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);