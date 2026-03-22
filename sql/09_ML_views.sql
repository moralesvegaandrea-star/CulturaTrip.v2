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