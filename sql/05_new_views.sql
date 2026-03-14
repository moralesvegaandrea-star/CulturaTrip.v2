-- =========================
-- Creacion de plan basico de viaje
-- =========================
CREATE OR REPLACE VIEW culturatrip.vw_plan_resumen_basico AS
WITH prov_principal AS (
  SELECT
    d.id_plan,
    d.id_provincia,
    p.provincia_nombre
  FROM culturatrip.fact_plan_viaje_destino d
  JOIN culturatrip.dim_provincia p
    ON d.id_provincia = p.id_provincia
  WHERE d.orden = 1
),
cats AS (
  SELECT
    id_plan,
    STRING_AGG(
      categoria || ' (' || cantidad || ')',
      ', '
      ORDER BY categoria
    ) AS categorias_actividad
  FROM culturatrip.fact_plan_viaje_preferencia
  GROUP BY id_plan
)
SELECT
  pv.id_plan,
  pv.email_usuario,
  pv.id_pais_origen,
  po.pais AS pais_origen,
  pv.id_pais_destino,
  pd.pais AS pais_destino,
  pp.id_provincia AS id_provincia_destino,
  pp.provincia_nombre AS provincia_destino,
  pv.fecha_ida,
  pv.fecha_regreso,
  ((pv.fecha_regreso - pv.fecha_ida) + 1)::int AS dias_viaje,
  (pv.fecha_regreso - pv.fecha_ida)::int       AS noches_viaje,
  pv.presupuesto_estimado,
  pv.tipo_viaje,
  pv.categoria_alojamiento,
  COALESCE(c.categorias_actividad, '') AS categorias_actividad,
  pv.created_at
FROM culturatrip.fact_plan_viaje pv
JOIN culturatrip.dim_pais po ON pv.id_pais_origen = po.id_pais
JOIN culturatrip.dim_pais pd ON pv.id_pais_destino = pd.id_pais
LEFT JOIN prov_principal pp ON pv.id_plan = pp.id_plan
LEFT JOIN cats c           ON pv.id_plan = c.id_plan;

-- ============================================
-- 1) Vista base: actividades clasificadas por grupo de costo
-- ============================================
CREATE OR REPLACE VIEW culturatrip.vw_rec_actividades_grupo_costo AS
SELECT
    r.id_pais,
    r.id_provincia,
    r.provincia_nombre,
    r.categoria,
    r.n_registros,
    r.avg_precio_entrada,
    r.avg_gasto_total,
    CASE
        WHEN LOWER(TRIM(r.categoria)) = 'comida y bebida' THEN 'alimentacion'
        WHEN LOWER(TRIM(r.categoria)) = 'servicios' THEN 'servicios'
        WHEN LOWER(TRIM(r.categoria)) = 'otros' THEN 'otros'
        ELSE 'actividades'
    END AS grupo_costo
FROM culturatrip.vw_rec_actividades_por_provincia r;



-- ============================================
-- 2) Costos de alojamiento
-- ============================================
CREATE OR REPLACE VIEW culturatrip.vw_plan_costos_alojamiento AS
WITH plan AS (
    SELECT *
    FROM culturatrip.vw_plan_resumen_basico
)
SELECT
    p.id_plan,
    p.noches_viaje,
    p.categoria_alojamiento,
    a.precio_medio AS precio_noche_aloj,
    ROUND(COALESCE(a.precio_medio, 0) * p.noches_viaje, 2) AS alojamiento_estimado
FROM plan p
LEFT JOIN culturatrip.vw_rec_alojamiento_precio_provincia a
    ON a.id_pais = p.id_pais_destino
   AND a.id_provincia = p.id_provincia_destino
   AND a.categoria_alojamiento = p.categoria_alojamiento;



-- ============================================
-- 3) Costos de alimentación
--    Costos de alimentación (automático por días)
-- ============================================

DROP VIEW IF EXISTS culturatrip.vw_plan_costos_alimentacion CASCADE;

CREATE OR REPLACE VIEW culturatrip.vw_plan_costos_alimentacion AS
WITH plan AS (
    SELECT *
    FROM culturatrip.vw_plan_resumen_basico
),
alim_base AS (
    SELECT
        p.id_plan,
        ROUND(AVG(r.avg_precio_entrada)::numeric, 2) AS referencia_comida_bebida
    FROM plan p
    JOIN culturatrip.vw_rec_actividades_grupo_costo r
        ON r.id_pais = p.id_pais_destino
       AND r.id_provincia = p.id_provincia_destino
    WHERE LOWER(TRIM(r.categoria)) = 'comida y bebida'
    GROUP BY p.id_plan
)
SELECT
    p.id_plan,
    p.dias_viaje,
    a.referencia_comida_bebida,
    ROUND(COALESCE(a.referencia_comida_bebida, 0) * 0.12, 2) AS gasto_diario_alimentacion,
    ROUND(COALESCE(a.referencia_comida_bebida, 0) * 0.12 * p.dias_viaje, 2) AS alimentacion_estimado
FROM plan p
LEFT JOIN alim_base a
    ON p.id_plan = a.id_plan;


-- ============================================
-- 4) Costos de actividades por cantidad
-- ============================================
DROP VIEW IF EXISTS culturatrip.vw_plan_costos_actividades CASCADE;

CREATE OR REPLACE VIEW culturatrip.vw_plan_costos_actividades AS
WITH plan AS (
    SELECT *
    FROM culturatrip.vw_plan_resumen_basico
),
act_det AS (
    SELECT
        p.id_plan,
        pref.categoria,
        pref.cantidad,
        ROUND(AVG(r.avg_precio_entrada)::numeric, 2) AS precio_promedio_categoria
    FROM plan p
    JOIN culturatrip.fact_plan_viaje_preferencia pref
        ON pref.id_plan = p.id_plan
    JOIN culturatrip.vw_rec_actividades_grupo_costo r
        ON r.id_pais = p.id_pais_destino
       AND r.id_provincia = p.id_provincia_destino
       AND r.categoria = pref.categoria
    WHERE r.grupo_costo = 'actividades'
    GROUP BY p.id_plan, pref.categoria, pref.cantidad
)
SELECT
    id_plan,
    ROUND(SUM(precio_promedio_categoria * cantidad), 2) AS actividades_estimado
FROM act_det
GROUP BY id_plan;


-- ============================================
-- 5) Costos de servicios por cantidad
-- ============================================
DROP VIEW IF EXISTS culturatrip.vw_plan_costos_servicios CASCADE;

CREATE OR REPLACE VIEW culturatrip.vw_plan_costos_servicios AS
WITH plan AS (
    SELECT *
    FROM culturatrip.vw_plan_resumen_basico
),
serv_det AS (
    SELECT
        p.id_plan,
        pref.categoria,
        pref.cantidad,
        ROUND(AVG(r.avg_precio_entrada)::numeric, 2) AS precio_promedio_categoria
    FROM plan p
    JOIN culturatrip.fact_plan_viaje_preferencia pref
        ON pref.id_plan = p.id_plan
    JOIN culturatrip.vw_rec_actividades_grupo_costo r
        ON r.id_pais = p.id_pais_destino
       AND r.id_provincia = p.id_provincia_destino
       AND r.categoria = pref.categoria
    WHERE r.grupo_costo = 'servicios'
    GROUP BY p.id_plan, pref.categoria, pref.cantidad
)
SELECT
    id_plan,
    ROUND(SUM(precio_promedio_categoria * cantidad), 2) AS servicios_estimado
FROM serv_det
GROUP BY id_plan;


-- ============================================
-- 6) Costos de otros por cantidad
-- ============================================
DROP VIEW IF EXISTS culturatrip.vw_plan_costos_otros CASCADE;

CREATE OR REPLACE VIEW culturatrip.vw_plan_costos_otros AS
WITH plan AS (
    SELECT *
    FROM culturatrip.vw_plan_resumen_basico
),
otros_det AS (
    SELECT
        p.id_plan,
        pref.categoria,
        pref.cantidad,
        ROUND(AVG(r.avg_precio_entrada)::numeric, 2) AS precio_promedio_categoria
    FROM plan p
    JOIN culturatrip.fact_plan_viaje_preferencia pref
        ON pref.id_plan = p.id_plan
    JOIN culturatrip.vw_rec_actividades_grupo_costo r
        ON r.id_pais = p.id_pais_destino
       AND r.id_provincia = p.id_provincia_destino
       AND r.categoria = pref.categoria
    WHERE r.grupo_costo = 'otros'
    GROUP BY p.id_plan, pref.categoria, pref.cantidad
)
SELECT
    id_plan,
    ROUND(SUM(precio_promedio_categoria * cantidad), 2) AS otros_estimado
FROM otros_det
GROUP BY id_plan;


-- ============================================
-- 7) Costos de transporte como porcentaje
-- ============================================
CREATE OR REPLACE VIEW culturatrip.vw_plan_costos_transporte AS
WITH base AS (
    SELECT
        p.id_plan,
        COALESCE(a.alojamiento_estimado, 0)   AS alojamiento_estimado,
        COALESCE(al.alimentacion_estimado, 0) AS alimentacion_estimado,
        COALESCE(ac.actividades_estimado, 0)  AS actividades_estimado,
        COALESCE(s.servicios_estimado, 0)     AS servicios_estimado,
        COALESCE(o.otros_estimado, 0)         AS otros_estimado
    FROM culturatrip.vw_plan_resumen_basico p
    LEFT JOIN culturatrip.vw_plan_costos_alojamiento  a  ON p.id_plan = a.id_plan
    LEFT JOIN culturatrip.vw_plan_costos_alimentacion al ON p.id_plan = al.id_plan
    LEFT JOIN culturatrip.vw_plan_costos_actividades  ac ON p.id_plan = ac.id_plan
    LEFT JOIN culturatrip.vw_plan_costos_servicios    s  ON p.id_plan = s.id_plan
    LEFT JOIN culturatrip.vw_plan_costos_otros        o  ON p.id_plan = o.id_plan
)
SELECT
    id_plan,
    ROUND(
        (alojamiento_estimado + alimentacion_estimado + actividades_estimado + servicios_estimado + otros_estimado) * 0.08
    , 2) AS transporte_estimado
FROM base;

-- ============================================
-- 8) Vista final consolidada de costos estimados
-- ============================================
CREATE OR REPLACE VIEW culturatrip.vw_plan_costos_estimados AS
SELECT
    p.id_plan,
    p.dias_viaje,
    p.noches_viaje,
    COALESCE(a.alojamiento_estimado, 0)   AS alojamiento_estimado,
    COALESCE(al.alimentacion_estimado, 0) AS alimentacion_estimado,
    COALESCE(ac.actividades_estimado, 0)  AS actividades_estimado,
    COALESCE(s.servicios_estimado, 0)     AS servicios_estimado,
    COALESCE(o.otros_estimado, 0)         AS otros_estimado,
    COALESCE(t.transporte_estimado, 0)    AS transporte_estimado,
    ROUND(
        COALESCE(a.alojamiento_estimado, 0) +
        COALESCE(al.alimentacion_estimado, 0) +
        COALESCE(ac.actividades_estimado, 0) +
        COALESCE(s.servicios_estimado, 0) +
        COALESCE(o.otros_estimado, 0) +
        COALESCE(t.transporte_estimado, 0)
    , 2) AS costo_total_estimado
FROM culturatrip.vw_plan_resumen_basico p
LEFT JOIN culturatrip.vw_plan_costos_alojamiento  a  ON p.id_plan = a.id_plan
LEFT JOIN culturatrip.vw_plan_costos_alimentacion al ON p.id_plan = al.id_plan
LEFT JOIN culturatrip.vw_plan_costos_actividades  ac ON p.id_plan = ac.id_plan
LEFT JOIN culturatrip.vw_plan_costos_servicios    s  ON p.id_plan = s.id_plan
LEFT JOIN culturatrip.vw_plan_costos_otros        o  ON p.id_plan = o.id_plan
LEFT JOIN culturatrip.vw_plan_costos_transporte   t  ON p.id_plan = t.id_plan;

-- =========================
-- Resumen Plan de Gastos
-- =========================
CREATE OR REPLACE VIEW culturatrip.vw_plan_gasto_real_resumen AS
SELECT
  id_plan,
  ROUND(SUM(monto)::numeric, 2) AS gasto_real_total,
  COUNT(*)::int AS n_gastos
FROM culturatrip.fact_plan_gasto_real
GROUP BY id_plan;

-- =========================
-- Resumen Plan de Gastos
-- =========================
CREATE OR REPLACE VIEW culturatrip.vw_plan_gasto_real_por_categoria AS
SELECT
  id_plan,
  categoria,
  ROUND(SUM(monto)::numeric, 2) AS gasto_real_categoria,
  COUNT(*)::int AS n_movimientos
FROM culturatrip.fact_plan_gasto_real
GROUP BY id_plan, categoria;

-- =========================
-- Vista detalle de gastos
-- =========================
CREATE OR REPLACE VIEW culturatrip.vw_plan_gasto_real_detalle AS
SELECT
  g.id_gasto,
  g.id_plan,
  g.fecha,
  g.categoria,
  g.descripcion,
  g.monto,
  g.created_at
FROM culturatrip.fact_plan_gasto_real g;



















