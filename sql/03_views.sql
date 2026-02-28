-- ===============================
-- 00) SANITY CHECKS (conteos)
-- ===============================
CREATE OR REPLACE VIEW culturatrip.vw_qa_counts_base AS
SELECT 'dim_pais' AS tabla, COUNT(*)::int AS n FROM culturatrip.dim_pais
UNION ALL SELECT 'dim_ccaa_base', COUNT(*)::int FROM culturatrip.dim_ccaa_base
UNION ALL SELECT 'dim_provincia', COUNT(*)::int FROM culturatrip.dim_provincia
UNION ALL SELECT 'dim_isla', COUNT(*)::int FROM culturatrip.dim_isla
UNION ALL SELECT 'dim_municipio', COUNT(*)::int FROM culturatrip.dim_municipio
UNION ALL SELECT 'rel_municipio_isla', COUNT(*)::int FROM culturatrip.rel_municipio_isla
UNION ALL SELECT 'dim_geografia_municipio_osm', COUNT(*)::int FROM culturatrip.dim_geografia_municipio_osm
UNION ALL SELECT 'fact_actividades', COUNT(*)::int FROM culturatrip.fact_actividades
UNION ALL SELECT 'fact_alojamientos', COUNT(*)::int FROM culturatrip.fact_alojamientos;

-- ===============================
-- 01) QA DIM_PAIS
-- ===============================
CREATE OR REPLACE VIEW culturatrip.vw_qa_dim_pais_duplicados AS
SELECT id_pais, COUNT(*)::int AS n
FROM culturatrip.dim_pais
GROUP BY id_pais
HAVING COUNT(*) > 1;

CREATE OR REPLACE VIEW culturatrip.vw_qa_dim_pais_latlon_nulls AS
SELECT id_pais, pais, lat, lon
FROM culturatrip.dim_pais
WHERE lat IS NULL OR lon IS NULL;

-- Para Streamlit: total países
CREATE OR REPLACE VIEW culturatrip.vw_ui_total_paises AS
SELECT COUNT(*)::int AS total_paises
FROM culturatrip.dim_pais;

-- ===============================
-- 02) QA DIM_CCAA_BASE
-- ===============================
CREATE OR REPLACE VIEW culturatrip.vw_qa_dim_ccaa_fk_pais_missing AS
SELECT c.*
FROM culturatrip.dim_ccaa_base c
LEFT JOIN culturatrip.dim_pais p
  ON c.id_pais = p.id_pais
WHERE p.id_pais IS NULL;

CREATE OR REPLACE VIEW culturatrip.vw_qa_dim_ccaa_duplicados_nombre AS
SELECT id_pais, ccaa_nombre, COUNT(*)::int AS n
FROM culturatrip.dim_ccaa_base
GROUP BY id_pais, ccaa_nombre
HAVING COUNT(*) > 1;

-- ===============================
-- 03) QA DIM_PROVINCIA
-- ===============================
CREATE OR REPLACE VIEW culturatrip.vw_qa_dim_provincia_fk_pais_missing AS
SELECT pr.*
FROM culturatrip.dim_provincia pr
LEFT JOIN culturatrip.dim_pais p
  ON pr.id_pais = p.id_pais
WHERE p.id_pais IS NULL;

CREATE OR REPLACE VIEW culturatrip.vw_qa_dim_provincia_duplicados_nombre AS
SELECT id_pais, provincia_nombre, COUNT(*)::int AS n
FROM culturatrip.dim_provincia
GROUP BY id_pais, provincia_nombre
HAVING COUNT(*) > 1;

-- Para Streamlit: provincias por país
CREATE OR REPLACE VIEW culturatrip.vw_ui_provincias_por_pais AS
SELECT id_pais, COUNT(*)::int AS total_provincias
FROM culturatrip.dim_provincia
GROUP BY id_pais;

-- ===============================
-- 04) QA DIM_ISLA
-- ===============================
CREATE OR REPLACE VIEW culturatrip.vw_qa_dim_isla_fk_pais_missing AS
SELECT i.*
FROM culturatrip.dim_isla i
LEFT JOIN culturatrip.dim_pais p
  ON i.id_pais = p.id_pais
WHERE p.id_pais IS NULL;

CREATE OR REPLACE VIEW culturatrip.vw_qa_dim_isla_fk_provincia_missing AS
SELECT i.*
FROM culturatrip.dim_isla i
LEFT JOIN culturatrip.dim_provincia p
  ON i.id_provincia = p.id_provincia
WHERE p.id_provincia IS NULL;

-- Para Streamlit: islas por país
CREATE OR REPLACE VIEW culturatrip.vw_ui_islas_por_pais AS
SELECT id_pais, COUNT(*)::int AS total_islas
FROM culturatrip.dim_isla
GROUP BY id_pais;

-- ===============================
-- 05) QA DIM_MUNICIPIO
-- ===============================
CREATE OR REPLACE VIEW culturatrip.vw_qa_dim_muni_fk_pais_missing AS
SELECT m.*
FROM culturatrip.dim_municipio m
LEFT JOIN culturatrip.dim_pais p
  ON m.id_pais = p.id_pais
WHERE p.id_pais IS NULL;

CREATE OR REPLACE VIEW culturatrip.vw_qa_dim_muni_fk_ccaa_missing AS
SELECT m.*
FROM culturatrip.dim_municipio m
LEFT JOIN culturatrip.dim_ccaa_base c
  ON m.id_ccaa = c.id_ccaa
WHERE c.id_ccaa IS NULL;

CREATE OR REPLACE VIEW culturatrip.vw_qa_dim_muni_fk_prov_missing AS
SELECT m.*
FROM culturatrip.dim_municipio m
LEFT JOIN culturatrip.dim_provincia p
  ON m.id_provincia = p.id_provincia
WHERE p.id_provincia IS NULL;

CREATE OR REPLACE VIEW culturatrip.vw_qa_dim_muni_fk_isla_missing AS
SELECT m.*
FROM culturatrip.dim_municipio m
LEFT JOIN culturatrip.dim_isla i
  ON m.id_isla = i.id_isla
WHERE m.id_isla IS NOT NULL
  AND i.id_isla IS NULL;

-- Para Streamlit: municipios por país
CREATE OR REPLACE VIEW culturatrip.vw_ui_municipios_por_pais AS
SELECT id_pais, COUNT(*)::int AS total_municipios
FROM culturatrip.dim_municipio
GROUP BY id_pais;

-- ===============================
-- 06) QA REL_MUNICIPIO_ISLA
-- ===============================
CREATE OR REPLACE VIEW culturatrip.vw_qa_rel_muni_isla_fk_muni_missing AS
SELECT r.*
FROM culturatrip.rel_municipio_isla r
LEFT JOIN culturatrip.dim_municipio m
  ON r.id_municipio = m.id_municipio
WHERE m.id_municipio IS NULL;

CREATE OR REPLACE VIEW culturatrip.vw_qa_rel_muni_isla_fk_isla_missing AS
SELECT r.*
FROM culturatrip.rel_municipio_isla r
LEFT JOIN culturatrip.dim_isla i
  ON r.id_isla = i.id_isla
WHERE i.id_isla IS NULL;

-- ===============================
-- 07) QA GEO OSM
-- ===============================
CREATE OR REPLACE VIEW culturatrip.vw_qa_geo_osm_latlon_invalid AS
SELECT *
FROM culturatrip.dim_geografia_municipio_osm
WHERE lat NOT BETWEEN -90 AND 90
   OR lon NOT BETWEEN -180 AND 180;

CREATE OR REPLACE VIEW culturatrip.vw_qa_geo_osm_fk_muni_missing AS
SELECT g.*
FROM culturatrip.dim_geografia_municipio_osm g
LEFT JOIN culturatrip.dim_municipio m
  ON g.id_municipio = m.id_municipio
WHERE m.id_municipio IS NULL;

-- municipios faltantes de geolocalización
CREATE OR REPLACE VIEW culturatrip.vw_qa_municipios_sin_geo AS
SELECT
  m.id_pais,
  m.id_municipio,
  m.nombre AS municipio_nombre,
  m.provincia_nombre,
  m.ccaa_nombre
FROM culturatrip.dim_municipio m
LEFT JOIN culturatrip.dim_geografia_municipio_osm g
  ON m.id_municipio = g.id_municipio
WHERE g.id_municipio IS NULL
ORDER BY m.id_municipio;

--- Validar conteo rápido
SELECT COUNT(*) AS municipios_sin_geo
FROM culturatrip.vw_qa_municipios_sin_geo;

--- Ver ejemplos ---
SELECT * FROM culturatrip.vw_qa_municipios_sin_geo LIMIT 20;

-- Para Streamlit: % cobertura geo por país
CREATE OR REPLACE VIEW culturatrip.vw_ui_geo_coverage_por_pais AS
SELECT
  m.id_pais,
  COUNT(*)::int AS total_municipios,
  SUM(CASE WHEN g.id_municipio IS NOT NULL THEN 1 ELSE 0 END)::int AS municipios_con_geo,
  SUM(CASE WHEN g.id_municipio IS NULL THEN 1 ELSE 0 END)::int AS municipios_sin_geo,
  ROUND(
    100.0 * SUM(CASE WHEN g.id_municipio IS NOT NULL THEN 1 ELSE 0 END) / NULLIF(COUNT(*),0),
    2
  ) AS pct_geo_cobertura
FROM culturatrip.dim_municipio m
LEFT JOIN culturatrip.dim_geografia_municipio_osm g
  ON m.id_municipio = g.id_municipio
GROUP BY m.id_pais;

-- ===============================
-- 08) QA FACT_ACTIVIDADES
-- ===============================
CREATE OR REPLACE VIEW culturatrip.vw_qa_fact_actividades_duplicados AS
SELECT id_pais, id_ccaa, id_provincia, mes, categoria, producto, subcategoria, COUNT(*)::int AS n
FROM culturatrip.fact_actividades
GROUP BY 1,2,3,4,5,6,7
HAVING COUNT(*) > 1;

CREATE OR REPLACE VIEW culturatrip.vw_qa_fact_actividades_fk_prov_missing AS
SELECT f.*
FROM culturatrip.fact_actividades f
LEFT JOIN culturatrip.dim_provincia p
  ON f.id_provincia = p.id_provincia
WHERE p.id_provincia IS NULL;

-- Para Streamlit: métricas resumidas por país (ejemplo)
CREATE OR REPLACE VIEW culturatrip.vw_ui_actividades_resumen_por_pais AS
SELECT
  id_pais,
  COUNT(*)::int AS n_registros,
  ROUND(AVG(gasto_total_promedio)::numeric, 2) AS avg_gasto_total,
  ROUND(AVG(precio_medio_entrada_promedio)::numeric, 2) AS avg_precio_entrada
FROM culturatrip.fact_actividades
GROUP BY id_pais;

-- ===============================
-- 09) QA FACT_ALOJAMIENTOS
-- ===============================
CREATE OR REPLACE VIEW culturatrip.vw_qa_fact_alojamientos_duplicados AS
SELECT id_pais, id_ccaa, id_provincia, mes, categoria_alojamiento, periodo_antelacion, COUNT(*)::int AS n
FROM culturatrip.fact_alojamientos
GROUP BY 1,2,3,4,5,6
HAVING COUNT(*) > 1;

CREATE OR REPLACE VIEW culturatrip.vw_qa_fact_alojamientos_mes_invalid AS
SELECT *
FROM culturatrip.fact_alojamientos
WHERE mes NOT BETWEEN 1 AND 12;

CREATE OR REPLACE VIEW culturatrip.vw_qa_fact_alojamientos_fk_prov_missing AS
SELECT f.*
FROM culturatrip.fact_alojamientos f
LEFT JOIN culturatrip.dim_provincia p
  ON f.id_provincia = p.id_provincia
WHERE p.id_provincia IS NULL;

-- Para Streamlit: resumen alojamientos por país
CREATE OR REPLACE VIEW culturatrip.vw_ui_alojamientos_resumen_por_pais AS
SELECT
  id_pais,
  COUNT(*)::int AS n_registros,
  ROUND(AVG(precio_checkin_entre_semana)::numeric, 2) AS avg_semana,
  ROUND(AVG(precio_checkin_fin_semana)::numeric, 2) AS avg_fin_semana,
  ROUND(AVG(valoraciones_norm)::numeric, 2) AS avg_valoraciones
FROM culturatrip.fact_alojamientos
GROUP BY id_pais;

-- ===============================
-- 10) Test Pantalla 1
-- ===============================
CREATE OR REPLACE VIEW culturatrip.vw_ui_pantalla1_detalle_por_pais AS
SELECT
  p.id_pais,
  p.pais,
  COALESCE(pr.total_provincias, 0)::int AS total_provincias,
  COALESCE(m.total_municipios, 0)::int AS total_municipios,
  COALESCE(i.total_islas, 0)::int      AS total_islas
FROM culturatrip.dim_pais p
LEFT JOIN (
  SELECT id_pais, COUNT(*) AS total_provincias
  FROM culturatrip.dim_provincia
  GROUP BY id_pais
) pr ON pr.id_pais = p.id_pais
LEFT JOIN (
  SELECT id_pais, COUNT(*) AS total_municipios
  FROM culturatrip.dim_municipio
  GROUP BY id_pais
) m ON m.id_pais = p.id_pais
LEFT JOIN (
  SELECT id_pais, COUNT(*) AS total_islas
  FROM culturatrip.dim_isla
  GROUP BY id_pais
) i ON i.id_pais = p.id_pais;

-- ===============================
-- UI - PANTALLA 1 (GLOBAL)
-- ===============================
CREATE OR REPLACE VIEW culturatrip.vw_ui_pantalla1_global AS
SELECT
  (SELECT COUNT(*) FROM culturatrip.dim_pais)::int        AS total_paises,
  (SELECT COUNT(*) FROM culturatrip.dim_provincia)::int   AS total_provincias,
  (SELECT COUNT(*) FROM culturatrip.dim_municipio)::int   AS total_municipios,
  (SELECT COUNT(*) FROM culturatrip.dim_isla)::int        AS total_islas;

-- ===============================
-- UI - PANTALLA 1 (DETALLE POR PAÍS)
-- ===============================
CREATE OR REPLACE VIEW culturatrip.vw_ui_pantalla1_detalle_por_pais AS
SELECT
  p.id_pais,
  p.pais,
  COALESCE(pr.total_provincias, 0)::int AS total_provincias,
  COALESCE(m.total_municipios, 0)::int AS total_municipios,
  COALESCE(i.total_islas, 0)::int      AS total_islas
FROM culturatrip.dim_pais p
LEFT JOIN (
  SELECT id_pais, COUNT(*) AS total_provincias
  FROM culturatrip.dim_provincia
  GROUP BY id_pais
) pr ON pr.id_pais = p.id_pais
LEFT JOIN (
  SELECT id_pais, COUNT(*) AS total_municipios
  FROM culturatrip.dim_municipio
  GROUP BY id_pais
) m ON m.id_pais = p.id_pais
LEFT JOIN (
  SELECT id_pais, COUNT(*) AS total_islas
  FROM culturatrip.dim_isla
  GROUP BY id_pais
) i ON i.id_pais = p.id_pais;

