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

-- =========================================================
-- VISTA 1
-- Base ML de alojamiento por provincia y categoría
-- =========================================================

DROP VIEW IF EXISTS culturatrip.vw_ml_alojamiento_base_provincia CASCADE;

CREATE OR REPLACE VIEW culturatrip.vw_ml_alojamiento_base_provincia AS
SELECT
    f.id_pais,
    f.id_ccaa,
    f.id_provincia,
    p.provincia_nombre,
    f.categoria_alojamiento,

    ROUND(AVG(f.valoraciones_norm)::numeric, 4) AS valoraciones_norm_prom,

    CASE
        WHEN BOOL_OR(COALESCE(f.tiene_valoraciones, false)) THEN 1
        ELSE 0
    END AS tiene_valoraciones_base,

    ROUND(AVG(f.precio_checkin_entre_semana)::numeric, 2) AS avg_precio_semana_hist,
    ROUND(AVG(f.precio_checkin_fin_semana)::numeric, 2) AS avg_precio_fin_semana_hist,

    COUNT(*)::int AS n_registros_base

FROM culturatrip.fact_alojamientos f
JOIN culturatrip.dim_provincia p
  ON f.id_provincia = p.id_provincia
WHERE f.categoria_alojamiento IS NOT NULL
  AND TRIM(f.categoria_alojamiento) <> ''
GROUP BY
    f.id_pais,
    f.id_ccaa,
    f.id_provincia,
    p.provincia_nombre,
    f.categoria_alojamiento;

-- =========================================================
-- VISTA 2
-- Features ML de alojamiento por plan
-- =========================================================

DROP VIEW IF EXISTS culturatrip.vw_ml_alojamiento_features_plan CASCADE;

CREATE OR REPLACE VIEW culturatrip.vw_ml_alojamiento_features_plan AS
WITH plan_base AS (
    SELECT
        pr.id_plan,
        pr.id_pais_destino,
        pr.id_provincia_destino,
        pr.provincia_destino,
        pr.fecha_ida,
        pr.fecha_regreso,
        pr.dias_viaje,
        pr.noches_viaje,
        pr.categoria_alojamiento,
        pr.presupuesto_estimado,
        pr.perfil_presupuesto,

        pc.presupuesto_alojamiento,

        n.noches_semana,
        n.noches_fin_semana,

        EXTRACT(MONTH FROM pr.fecha_ida)::int AS mes_viaje,
        (pr.fecha_ida - CURRENT_DATE)::int AS dias_anticipacion
    FROM culturatrip.vw_plan_resumen_basico pr
    LEFT JOIN culturatrip.vw_plan_presupuesto_categoria pc
      ON pr.id_plan = pc.id_plan
    LEFT JOIN culturatrip.vw_plan_noches_tipo_dia n
      ON pr.id_plan = n.id_plan
),
plan_con_temporada AS (
    SELECT
        pb.*,
        dt.temporada
    FROM plan_base pb
    LEFT JOIN culturatrip.dim_tiempo dt
      ON dt.anio = EXTRACT(YEAR FROM pb.fecha_ida)::int
     AND dt.mes  = pb.mes_viaje
),
plan_enriquecido AS (
    SELECT
        p.*,

        CASE
            WHEN p.dias_anticipacion IS NULL THEN NULL
            WHEN p.dias_anticipacion <= 7  THEN '1 semana'
            WHEN p.dias_anticipacion <= 14 THEN '2 semanas'
            WHEN p.dias_anticipacion <= 30 THEN '1 mes'
            WHEN p.dias_anticipacion <= 90 THEN '2-3 meses'
            ELSE '3 meses'
        END AS periodo_antelacion_label

    FROM plan_con_temporada p
)
SELECT
    p.id_plan,

    -- claves de negocio
    p.id_pais_destino AS id_pais,
    b.id_ccaa,
    p.id_provincia_destino AS id_provincia,
    p.provincia_destino,

    -- fechas y duración
    p.fecha_ida,
    p.fecha_regreso,
    p.dias_viaje,
    p.noches_viaje,
    COALESCE(p.noches_semana, 0) AS noches_semana,
    COALESCE(p.noches_fin_semana, 0) AS noches_fin_semana,

    -- presupuesto
    p.presupuesto_estimado,
    COALESCE(p.presupuesto_alojamiento, 0) AS presupuesto_alojamiento_ine,
    p.perfil_presupuesto,

    -- variables de negocio
    p.categoria_alojamiento,
    p.mes_viaje AS mes,
    p.temporada,
    p.dias_anticipacion,
    p.periodo_antelacion_label,

    -- codificación temporada según notebook
    CASE
        WHEN LOWER(COALESCE(p.temporada, '')) = 'baja'  THEN 0
        WHEN LOWER(COALESCE(p.temporada, '')) = 'media' THEN 1
        WHEN LOWER(COALESCE(p.temporada, '')) = 'alta'  THEN 2
        ELSE NULL
    END AS temporada_cod,

    -- codificación de categoría de alojamiento según notebook
    CASE LOWER(TRIM(COALESCE(p.categoria_alojamiento, '')))
        WHEN 'hotel 3 estrellas'     THEN 1
        WHEN 'hotel 4 estrellas'     THEN 2
        WHEN 'hotel 5 estrellas'     THEN 3
        WHEN 'apartamento'           THEN 4
        WHEN 'casa entera'           THEN 5
        WHEN 'habitacion privada'    THEN 6
        WHEN 'habitacion compartida' THEN 7
        WHEN 'alternativo'           THEN 8
        ELSE NULL
    END AS categoria_alojamiento_cod,

    -- codificación de antelación según notebook
    CASE p.periodo_antelacion_label
        WHEN '1 semana'   THEN 1
        WHEN '2 semanas'  THEN 2
        WHEN '1 mes'      THEN 3
        WHEN '2-3 meses'  THEN 4
        WHEN '3 meses'    THEN 5
        ELSE NULL
    END AS periodo_antelacion_cod,

    -- variables auxiliares para el modelo
    COALESCE(b.valoraciones_norm_prom, 0) AS valoraciones_norm,
    COALESCE(b.tiene_valoraciones_base, 0) AS tiene_valoraciones,

    -- columnas de auditoría / referencia
    b.avg_precio_semana_hist,
    b.avg_precio_fin_semana_hist,
    b.n_registros_base

FROM plan_enriquecido p
LEFT JOIN culturatrip.vw_ml_alojamiento_base_provincia b
  ON b.id_pais = p.id_pais_destino
 AND b.id_provincia = p.id_provincia_destino
 AND LOWER(TRIM(b.categoria_alojamiento)) = LOWER(TRIM(p.categoria_alojamiento));