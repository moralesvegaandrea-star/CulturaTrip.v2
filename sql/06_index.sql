CREATE UNIQUE INDEX IF NOT EXISTS ux_plan_unico
ON culturatrip.fact_plan_viaje (
  email_usuario,
  id_pais_origen,
  id_pais_destino,
  fecha_ida,
  fecha_regreso,
  categoria_alojamiento
);