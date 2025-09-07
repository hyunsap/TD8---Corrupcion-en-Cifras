-- ============================================
-- Configuración inicial
-- ============================================
SET client_encoding = 'UTF8';
SET timezone = 'America/Argentina/Buenos_Aires';

CREATE SCHEMA IF NOT EXISTS public;

-- ============================================
-- Entidades principales
-- ============================================

-- Fuero
CREATE TABLE fuero (
    fuero_id SERIAL PRIMARY KEY,
    nombre VARCHAR(100) NOT NULL UNIQUE
);

-- Jurisdicción
CREATE TABLE jurisdiccion (
    jurisdiccion_id SERIAL PRIMARY KEY,
    ambito VARCHAR(50) NOT NULL,
    provincia VARCHAR(50),
    departamento_judicial VARCHAR(100)
);

-- Tribunal
CREATE TABLE tribunal (
    tribunal_id SERIAL PRIMARY KEY,
    nombre VARCHAR(200) NOT NULL,
    instancia VARCHAR(50),
    domicilio_sede TEXT,
    contacto VARCHAR(200),
    jurisdiccion_id INTEGER NOT NULL,
    CONSTRAINT fk_tribunal_jurisdiccion 
        FOREIGN KEY (jurisdiccion_id) REFERENCES jurisdiccion(jurisdiccion_id)
        ON DELETE RESTRICT ON UPDATE CASCADE
);

-- Secretaría
CREATE TABLE secretaria (
    secretaria_id SERIAL PRIMARY KEY,
    nombre VARCHAR(200) NOT NULL,
    tribunal_id INTEGER NOT NULL,
    CONSTRAINT fk_secretaria_tribunal 
        FOREIGN KEY (tribunal_id) REFERENCES tribunal(tribunal_id)
        ON DELETE CASCADE ON UPDATE CASCADE
);

-- Estado Procesal
CREATE TABLE estado_procesal (
    estado_procesal_id SERIAL PRIMARY KEY,
    nombre VARCHAR(100) NOT NULL UNIQUE,
    etapa VARCHAR(100)
);

-- Tipo Delito
CREATE TABLE tipo_delito (
    tipo VARCHAR(50) PRIMARY KEY,
    detalle TEXT
);

-- Letrado
CREATE TABLE letrado (
    letrado_id SERIAL PRIMARY KEY,
    nombre VARCHAR(200) NOT NULL,
    matricula VARCHAR(50),
    colegio VARCHAR(100),
    email VARCHAR(100),
    telefono VARCHAR(50)
);

-- Expediente
CREATE TABLE expediente (
    numero_expediente VARCHAR(50) PRIMARY KEY,
    caratula TEXT NOT NULL,
    fecha_inicio DATE NOT NULL,
    fecha_ultimo_movimiento DATE,
    fuero_id INTEGER NOT NULL,
    tribunal_id INTEGER NOT NULL,
    secretaria_id INTEGER,
    estado_procesal_id INTEGER NOT NULL,
    estado_solapa VARCHAR(50), -- "Terminadas" / "En trámite"
    
    CONSTRAINT fk_expediente_fuero 
        FOREIGN KEY (fuero_id) REFERENCES fuero(fuero_id)
        ON DELETE RESTRICT ON UPDATE CASCADE,
    CONSTRAINT fk_expediente_tribunal 
        FOREIGN KEY (tribunal_id) REFERENCES tribunal(tribunal_id)
        ON DELETE RESTRICT ON UPDATE CASCADE,
    CONSTRAINT fk_expediente_secretaria 
        FOREIGN KEY (secretaria_id) REFERENCES secretaria(secretaria_id)
        ON DELETE SET NULL ON UPDATE CASCADE,
    CONSTRAINT fk_expediente_estado_procesal 
        FOREIGN KEY (estado_procesal_id) REFERENCES estado_procesal(estado_procesal_id)
        ON DELETE RESTRICT ON UPDATE CASCADE
);

-- ============================================
-- Entidades dependientes y relaciones N:M
-- ============================================

-- Parte (con surrogate key en vez de documento_cuit solo)
CREATE TABLE parte (
    parte_id SERIAL PRIMARY KEY,
    numero_expediente VARCHAR(50) NOT NULL,
    documento_cuit VARCHAR(20),
    tipo_persona VARCHAR(20) NOT NULL CHECK (tipo_persona IN ('fisica', 'juridica')),
    nombre_razon_social VARCHAR(200) NOT NULL,
    
    CONSTRAINT fk_parte_expediente 
        FOREIGN KEY (numero_expediente) REFERENCES expediente(numero_expediente)
        ON DELETE CASCADE ON UPDATE CASCADE
);

-- Rol de la parte (ej: imputado, demandante, querellante)
CREATE TABLE rol_parte (
    rol_parte_id SERIAL PRIMARY KEY,
    parte_id INTEGER NOT NULL,
    nombre VARCHAR(200) NOT NULL,
    CONSTRAINT fk_rol_parte_parte 
        FOREIGN KEY (parte_id) REFERENCES parte(parte_id)
        ON DELETE CASCADE ON UPDATE CASCADE
);

-- Relación N:M entre expediente y tipo de delito
CREATE TABLE expediente_delito (
    numero_expediente VARCHAR(50) NOT NULL,
    tipo_delito VARCHAR(50) NOT NULL,
    PRIMARY KEY (numero_expediente, tipo_delito),
    CONSTRAINT fk_expediente_delito_exp FOREIGN KEY (numero_expediente)
        REFERENCES expediente(numero_expediente)
        ON DELETE CASCADE,
    CONSTRAINT fk_expediente_delito_tipo FOREIGN KEY (tipo_delito)
        REFERENCES tipo_delito(tipo)
        ON DELETE RESTRICT
);

-- Representación (qué letrado representa a qué parte en qué expediente)
CREATE TABLE representacion (
    numero_expediente VARCHAR(50) NOT NULL,
    parte_id INTEGER NOT NULL,
    letrado_id INTEGER NOT NULL,
    rol VARCHAR(100), -- defensor, apoderado, patrocinante, etc.
    PRIMARY KEY (numero_expediente, parte_id, letrado_id),
    CONSTRAINT fk_repr_exp FOREIGN KEY (numero_expediente)
        REFERENCES expediente(numero_expediente)
        ON DELETE CASCADE,
    CONSTRAINT fk_repr_parte FOREIGN KEY (parte_id)
        REFERENCES parte(parte_id)
        ON DELETE CASCADE,
    CONSTRAINT fk_repr_letrado FOREIGN KEY (letrado_id)
        REFERENCES letrado(letrado_id)
        ON DELETE CASCADE
);

-- Resoluciones (1:N con expediente)
CREATE TABLE resolucion (
    resolucion_id SERIAL PRIMARY KEY,
    numero_expediente VARCHAR(50) NOT NULL,
    texto TEXT NOT NULL,
    fecha DATE,
    fuente TEXT,
    CONSTRAINT fk_resol_exp FOREIGN KEY (numero_expediente)
        REFERENCES expediente(numero_expediente)
        ON DELETE CASCADE
);

-- Plazos (1:N con expediente)
CREATE TABLE plazo (
    plazo_id SERIAL PRIMARY KEY,
    numero_expediente VARCHAR(50) NOT NULL,
    tipo VARCHAR(100) NOT NULL,
    fecha_inicio DATE NOT NULL,
    fecha_vencimiento DATE NOT NULL,
    dias_habiles INTEGER,
    estado VARCHAR(20) DEFAULT 'vigente' CHECK (estado IN ('vigente', 'vencido', 'cumplido')),
    tolerancia_dias INTEGER DEFAULT 0,
    
    CONSTRAINT fk_plazo_expediente 
        FOREIGN KEY (numero_expediente) REFERENCES expediente(numero_expediente)
        ON DELETE CASCADE ON UPDATE CASCADE,
    CONSTRAINT chk_fechas_plazo CHECK (fecha_vencimiento >= fecha_inicio)
);

-- ============================================
-- Índices
-- ============================================
CREATE INDEX idx_expediente_fecha_inicio ON expediente(fecha_inicio);
CREATE INDEX idx_expediente_fecha_ultimo_movimiento ON expediente(fecha_ultimo_movimiento);
CREATE INDEX idx_expediente_fuero ON expediente(fuero_id);
CREATE INDEX idx_expediente_tribunal ON expediente(tribunal_id);
CREATE INDEX idx_expediente_estado ON expediente(estado_procesal_id);
CREATE INDEX idx_parte_expediente ON parte(numero_expediente);
CREATE INDEX idx_plazo_expediente ON plazo(numero_expediente);
CREATE INDEX idx_plazo_vencimiento ON plazo(fecha_vencimiento);
CREATE INDEX idx_tribunal_jurisdiccion ON tribunal(jurisdiccion_id);

-- ============================================
-- Comentarios para documentación
-- ============================================
COMMENT ON TABLE expediente IS 'Tabla principal que contiene la información de los expedientes judiciales';
COMMENT ON TABLE parte IS 'Personas físicas o jurídicas involucradas en un expediente';
COMMENT ON TABLE rol_parte IS 'Roles específicos que una parte cumple en un expediente';
COMMENT ON TABLE representacion IS 'Relación entre parte y letrado en un expediente';
COMMENT ON TABLE expediente_delito IS 'Asociación N:M entre expedientes y delitos imputados';
COMMENT ON TABLE resolucion IS 'Resoluciones dictadas en un expediente';
COMMENT ON TABLE plazo IS 'Plazos procesales asociados a cada expediente';
