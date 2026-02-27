/*
==================================================
Consulta 1 — Métricas territoriales del país (base para Pantalla 1)
Necesidad: Mostrar “¿Sabías que…?” con conteos de municipios, provincias, CCAA e islas.
==================================================
*/
SELECT
  p.pais,
  (SELECT COUNT(*) FROM culturatrip.dim_municipio  m  WHERE m.id_pais = p.id_pais) AS n_municipios,
  (SELECT COUNT(*) FROM culturatrip.dim_provincia  pr WHERE pr.id_pais = p.id_pais) AS n_provincias,
  (SELECT COUNT(*) FROM culturatrip.dim_ccaa_base  c  WHERE c.id_pais = p.id_pais) AS n_ccaa,
  (SELECT COUNT(*) FROM culturatrip.dim_isla       i  WHERE i.id_pais = p.id_pais) AS n_islas
FROM culturatrip.dim_pais p
WHERE p.id_pais = 'ES';

/*
