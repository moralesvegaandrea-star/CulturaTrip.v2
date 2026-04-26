-- =========================================
-- 10_auth.sql · Tabla de autenticación
-- CulturaTrip · Sistema de login
-- =========================================

CREATE TABLE IF NOT EXISTS usuarios (
    email           VARCHAR(255) PRIMARY KEY,
    nombre          VARCHAR(255) NOT NULL,
    password_hash   VARCHAR(255) NOT NULL,
    fecha_registro  TIMESTAMP   DEFAULT CURRENT_TIMESTAMP
);

-- Índice para búsquedas rápidas por email
CREATE INDEX IF NOT EXISTS idx_usuarios_email
    ON usuarios (email);



