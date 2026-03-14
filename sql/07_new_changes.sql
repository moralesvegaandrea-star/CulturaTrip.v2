-- ============================================
-- 0) Rehacer tabla de preferencias con cantidad
-- ============================================
DROP VIEW IF EXISTS culturatrip.vw_plan_costos_estimados CASCADE;
DROP VIEW IF EXISTS culturatrip.vw_plan_costos_transporte CASCADE;
DROP VIEW IF EXISTS culturatrip.vw_plan_costos_otros CASCADE;
DROP VIEW IF EXISTS culturatrip.vw_plan_costos_servicios CASCADE;
DROP VIEW IF EXISTS culturatrip.vw_plan_costos_actividades CASCADE;
DROP VIEW IF EXISTS culturatrip.vw_plan_costos_alimentacion CASCADE;
DROP VIEW IF EXISTS culturatrip.vw_plan_costos_alojamiento CASCADE;
DROP VIEW IF EXISTS culturatrip.vw_plan_resumen_basico CASCADE;

DROP TABLE IF EXISTS culturatrip.fact_plan_viaje_preferencia;
