-- =========================================
-- 11_alter_table.sql · Consentimientos RGPD
-- CulturaTrip · Ampliación tabla usuarios
-- =========================================

ALTER TABLE usuarios
    ADD COLUMN IF NOT EXISTS acepta_terminos           BOOLEAN NOT NULL DEFAULT FALSE,
    ADD COLUMN IF NOT EXISTS acepta_privacidad         BOOLEAN NOT NULL DEFAULT FALSE,
    ADD COLUMN IF NOT EXISTS acepta_ia_personalizada   BOOLEAN NOT NULL DEFAULT FALSE,
    ADD COLUMN IF NOT EXISTS acepta_comunicaciones     BOOLEAN NOT NULL DEFAULT FALSE,
    ADD COLUMN IF NOT EXISTS acepta_cookies            BOOLEAN NOT NULL DEFAULT FALSE;