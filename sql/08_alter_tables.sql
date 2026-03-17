ALTER TABLE culturatrip.fact_plan_viaje
ADD COLUMN IF NOT EXISTS perfil_presupuesto VARCHAR(20) NOT NULL DEFAULT 'standard';

ALTER TABLE culturatrip.fact_plan_viaje
DROP CONSTRAINT IF EXISTS fk_plan_perfil_presupuesto;

ALTER TABLE culturatrip.fact_plan_viaje
ADD CONSTRAINT fk_plan_perfil_presupuesto
FOREIGN KEY (perfil_presupuesto)
REFERENCES culturatrip.dim_parametros_presupuesto(perfil_presupuesto);