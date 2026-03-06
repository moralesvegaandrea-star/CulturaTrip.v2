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
    STRING_AGG(categoria, ', ' ORDER BY categoria) AS categorias_actividad
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

-- =========================
-- Creacion de plan basico de costos
-- =========================

CREATE OR REPLACE VIEW culturatrip.vw_plan_costos_estimados AS
WITH plan AS (
  SELECT *
  FROM culturatrip.vw_plan_resumen_basico
),
act_avg AS (
  SELECT
    p.id_plan,
    ROUND(AVG(r.avg_precio_entrada)::numeric, 2) AS costo_diario_actividades
  FROM plan p
  JOIN culturatrip.fact_plan_viaje_preferencia pref
    ON pref.id_plan = p.id_plan
  JOIN culturatrip.vw_rec_actividades_por_provincia r
    ON r.id_pais = p.id_pais_destino
   AND r.id_provincia = p.id_provincia_destino
   AND r.categoria = pref.categoria
  GROUP BY p.id_plan
),
aloj AS (
  SELECT
    p.id_plan,
    ROUND(a.precio_medio::numeric, 2) AS precio_noche_aloj
  FROM plan p
  JOIN culturatrip.vw_rec_alojamiento_precio_provincia a
    ON a.id_pais = p.id_pais_destino
   AND a.id_provincia = p.id_provincia_destino
   AND a.categoria_alojamiento = p.categoria_alojamiento
)
SELECT
  p.id_plan,
  p.noches_viaje,
  p.dias_viaje,

  -- Alojamiento: precio_noche * noches
  ROUND(COALESCE(aloj.precio_noche_aloj, 0) * p.noches_viaje, 2) AS alojamiento_estimado,

  -- Actividades: costo diario * días
  ROUND(COALESCE(act.costo_diario_actividades, 0) * p.dias_viaje, 2) AS actividades_estimado,

  NULL::numeric AS transporte_estimado
FROM plan p
LEFT JOIN aloj aloj ON p.id_plan = aloj.id_plan
LEFT JOIN act_avg act ON p.id_plan = act.id_plan;