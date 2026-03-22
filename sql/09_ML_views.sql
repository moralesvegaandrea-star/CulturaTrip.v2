DROP VIEW IF EXISTS culturatrip.vw_ml_avanzado_base_provincia CASCADE;

CREATE OR REPLACE VIEW culturatrip.vw_ml_avanzado_base_provincia AS
WITH aloj AS (
    SELECT
        f.id_pais,
        f.id_provincia,
        p.provincia_nombre,
        ROUND(AVG(f.precio_checkin_entre_semana)::numeric, 2) AS precio_noche_semana,
        ROUND(AVG(f.precio_checkin_fin_semana)::numeric, 2) AS precio_noche_fin_semana,
        ROUND(
            AVG(
                (
                    COALESCE(f.precio_checkin_entre_semana, 0) +
                    COALESCE(f.precio_checkin_fin_semana, 0)
                ) / 2.0
            )::numeric,
            2
        ) AS precio_noche_promedio
    FROM culturatrip.fact_alojamientos f
    JOIN culturatrip.dim_provincia p
      ON f.id_provincia = p.id_provincia
    GROUP BY
        f.id_pais,
        f.id_provincia,
        p.provincia_nombre
),
act AS (
    SELECT
        f.id_pais,
        f.id_provincia,
        COUNT(*)::int AS n_actividades,
        ROUND(AVG(f.precio_medio_entrada_promedio)::numeric, 2) AS precio_actividad_promedio,
        ROUND(AVG(f.gasto_total_promedio)::numeric, 2) AS gasto_total_actividad_promedio
    FROM culturatrip.fact_actividades f
    GROUP BY
        f.id_pais,
        f.id_provincia
),
cfg AS (
    SELECT
        perfil_presupuesto,
        pct_alojamiento,
        pct_transporte,
        pct_alimentacion,
        pct_actividades,
        pct_servicios,
        pct_otros
    FROM culturatrip.dim_parametros_presupuesto
    WHERE perfil_presupuesto = 'standard'
)
SELECT
    a.id_pais,
    a.id_provincia,
    a.provincia_nombre,

    a.precio_noche_semana,
    a.precio_noche_fin_semana,
    a.precio_noche_promedio,

    COALESCE(ac.n_actividades, 0) AS n_actividades,
    COALESCE(ac.precio_actividad_promedio, 0) AS precio_actividad_promedio,
    COALESCE(ac.gasto_total_actividad_promedio, 0) AS gasto_total_actividad_promedio,

    cfg.perfil_presupuesto,
    cfg.pct_alojamiento,
    cfg.pct_transporte,
    cfg.pct_alimentacion,
    cfg.pct_actividades,
    cfg.pct_servicios,
    cfg.pct_otros

FROM aloj a
LEFT JOIN act ac
  ON a.id_pais = ac.id_pais
 AND a.id_provincia = ac.id_provincia
CROSS JOIN cfg;

-- ============================================
-- Modelo No Supervisado
-- Vista base por provincia
-- ============================================
DROP VIEW IF EXISTS culturatrip.vw_ml_no_supervisado_base_provincia CASCADE;

CREATE OR REPLACE VIEW culturatrip.vw_ml_no_supervisado_base_provincia AS
WITH base AS (
    SELECT
        f.id_pais,
        f.id_provincia,
        p.provincia_nombre,

        COUNT(*)::int AS total_actividades,
        COUNT(DISTINCT f.categoria)::int AS categorias_unicas,

        ROUND(AVG(f.valoracion_general_promedio)::numeric, 4) AS valoracion_general_promedio,
        ROUND(AVG(CASE WHEN f.hay_valoracion THEN 1.0 ELSE 0.0 END)::numeric, 4) AS tasa_con_valoracion,

        ROUND(AVG(LN(COALESCE(f.gasto_total_promedio, 0) + 1))::numeric, 6) AS log_gasto_total_promedio,
        ROUND(AVG(LN(COALESCE(f.precio_medio_entrada_promedio, 0) + 1))::numeric, 6) AS log_precio_medio_entrada_promedio,
        ROUND(AVG(LN(COALESCE(f.total_opiniones_categoria_promedio, 0) + 1))::numeric, 6) AS log_total_opiniones_categoria_promedio,

        COUNT(*) FILTER (WHERE LOWER(TRIM(f.categoria)) = 'comida y bebida')::int AS cnt_comida_bebida,
        COUNT(*) FILTER (WHERE LOWER(TRIM(f.categoria)) = 'servicios')::int AS cnt_servicios,
        COUNT(*) FILTER (WHERE LOWER(TRIM(f.categoria)) = 'vida nocturna')::int AS cnt_vida_nocturna,
        COUNT(*) FILTER (WHERE LOWER(TRIM(f.categoria)) = 'paisaje naturaleza')::int AS cnt_paisaje_naturaleza,
        COUNT(*) FILTER (WHERE LOWER(TRIM(f.categoria)) = 'paisaje urbano')::int AS cnt_paisaje_urbano,
        COUNT(*) FILTER (WHERE LOWER(TRIM(f.categoria)) = 'compras')::int AS cnt_compras,
        COUNT(*) FILTER (WHERE LOWER(TRIM(f.categoria)) = 'otros')::int AS cnt_otros

    FROM culturatrip.fact_actividades f
    JOIN culturatrip.dim_provincia p
      ON f.id_provincia = p.id_provincia
    GROUP BY
        f.id_pais,
        f.id_provincia,
        p.provincia_nombre
)

SELECT
    id_pais,
    id_provincia,
    provincia_nombre,

    total_actividades,
    categorias_unicas,
    COALESCE(valoracion_general_promedio, 0) AS valoracion_general_promedio,
    COALESCE(tasa_con_valoracion, 0) AS tasa_con_valoracion,
    COALESCE(log_gasto_total_promedio, 0) AS log_gasto_total_promedio,
    COALESCE(log_precio_medio_entrada_promedio, 0) AS log_precio_medio_entrada_promedio,
    COALESCE(log_total_opiniones_categoria_promedio, 0) AS log_total_opiniones_categoria_promedio,

    cnt_comida_bebida,
    cnt_servicios,
    cnt_vida_nocturna,
    cnt_paisaje_naturaleza,
    cnt_paisaje_urbano,
    cnt_compras,
    cnt_otros,

    ROUND(cnt_comida_bebida::numeric / NULLIF(total_actividades, 0), 6) AS prop_comida_bebida,
    ROUND(cnt_servicios::numeric / NULLIF(total_actividades, 0), 6) AS prop_servicios,
    ROUND(cnt_vida_nocturna::numeric / NULLIF(total_actividades, 0), 6) AS prop_vida_nocturna,
    ROUND(cnt_paisaje_naturaleza::numeric / NULLIF(total_actividades, 0), 6) AS prop_paisaje_naturaleza,
    ROUND(cnt_paisaje_urbano::numeric / NULLIF(total_actividades, 0), 6) AS prop_paisaje_urbano,
    ROUND(cnt_compras::numeric / NULLIF(total_actividades, 0), 6) AS prop_compras,
    ROUND(cnt_otros::numeric / NULLIF(total_actividades, 0), 6) AS prop_otros

FROM base;