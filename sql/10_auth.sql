-- =========================================
-- 10_auth.sql · Tabla de autenticación
-- CulturaTrip · Sistema de login
-- =========================================

CREATE TABLE IF NOT EXISTS usuarios (
    email           VARCHAR(255) PRIMARY KEY,
    nombre          VARCHAR(255) NOT NULL,
    password_hash   VARCHAR(255) NOT NULL,
    fecha_registro  TIMESTAMP   DEFAULT CURRENT_TIMESTAMP,
    acepta_terminos             BOOLEAN     NOT NULL DEFAULT FALSE,
    acepta_privacidad           BOOLEAN     NOT NULL DEFAULT FALSE,
    acepta_ia_personalizada     BOOLEAN     NOT NULL DEFAULT FALSE,
    acepta_comunicaciones       BOOLEAN     NOT NULL DEFAULT FALSE,
    acepta_cookies              BOOLEAN     NOT NULL DEFAULT FALSE
);

-- Índice para búsquedas rápidas por email
CREATE INDEX IF NOT EXISTS idx_usuarios_email
    ON usuarios (email);



