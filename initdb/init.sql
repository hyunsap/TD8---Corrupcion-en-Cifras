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
    nombre VARCHAR(200) UNIQUE NOT NULL,
    instancia VARCHAR(50),
    domicilio_sede TEXT,
    contacto VARCHAR(200),
    fuero VARCHAR(100) NOT NULL,
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
    tipo TEXT PRIMARY KEY,
    detalle TEXT
);

-- Letrado
CREATE TABLE letrado (
    letrado_id SERIAL PRIMARY KEY,
    nombre VARCHAR(200) NOT NULL UNIQUE,
    matricula VARCHAR(50),
    colegio VARCHAR(100),
    email VARCHAR(100),
    telefono VARCHAR(50)
);

-- ==============================
-- TABLA EXPEDIENTE
-- ==============================
CREATE TABLE expediente (
    numero_expediente VARCHAR(50) PRIMARY KEY,
    caratula TEXT,
    jurisdiccion TEXT,
    tribunal TEXT, -- redundancia textual
    estado TEXT,
    fecha_inicio DATE,
    fecha_ultimo_movimiento DATE,
    camara_origen TEXT,
    ano_inicio INT,
    delitos TEXT,
    fiscal TEXT,
    fiscalia TEXT,
    id_tribunal INT REFERENCES tribunal(tribunal_id) ON DELETE SET NULL
);

-- ==============================
-- TABLA RESOLUCION
-- ==============================
CREATE TABLE resolucion (
    id_resolucion SERIAL PRIMARY KEY,
    numero_expediente VARCHAR(50) REFERENCES expediente(numero_expediente) ON DELETE CASCADE,
    fecha DATE,
    nombre TEXT,
    link TEXT
);

-- ============================================
-- Entidades dependientes y relaciones N:M
-- ============================================

-- Parte
CREATE TABLE parte (
    parte_id SERIAL PRIMARY KEY,
    numero_expediente VARCHAR(50) NOT NULL,
    documento_cuit VARCHAR(20),
    tipo_persona VARCHAR(20) CHECK (tipo_persona IN ('fisica', 'juridica')),
    nombre_razon_social VARCHAR(200),
    CONSTRAINT fk_parte_expediente 
        FOREIGN KEY (numero_expediente) REFERENCES expediente(numero_expediente)
        ON DELETE CASCADE ON UPDATE CASCADE
);

-- Rol de la parte
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
    tipo_delito TEXT NOT NULL,
    PRIMARY KEY (numero_expediente, tipo_delito),
    CONSTRAINT fk_expediente_delito_exp FOREIGN KEY (numero_expediente)
        REFERENCES expediente(numero_expediente)
        ON DELETE CASCADE,
    CONSTRAINT fk_expediente_delito_tipo FOREIGN KEY (tipo_delito)
        REFERENCES tipo_delito(tipo)
        ON DELETE RESTRICT
);

-- Representación
CREATE TABLE representacion (
    numero_expediente VARCHAR(50) NOT NULL,
    parte_id INTEGER NOT NULL,
    letrado_id INTEGER NOT NULL,
    rol VARCHAR(100),
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

-- Plazos
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
-- Tabla de Jueces/Magistrados
-- ============================================

CREATE TABLE juez (
    juez_id SERIAL PRIMARY KEY,
    nombre VARCHAR(200) NOT NULL,
    email VARCHAR(100),
    telefono VARCHAR(50),
    CONSTRAINT uq_juez_nombre UNIQUE (nombre)
);

-- ============================================
-- Tabla intermedia: Relación Tribunal-Juez
-- ============================================

CREATE TABLE tribunal_juez (
    tribunal_id INTEGER NOT NULL,
    juez_id INTEGER NOT NULL,
    cargo VARCHAR(100),
    situacion VARCHAR(50) DEFAULT 'Efectivo' 
        CHECK (situacion IN ('Efectivo', 'Subrogante', 'Interino', 'Suplente')),
    fecha_desde DATE,
    fecha_hasta DATE,
    
    PRIMARY KEY (tribunal_id, juez_id),
    
    CONSTRAINT fk_tribunal_juez_tribunal 
        FOREIGN KEY (tribunal_id) REFERENCES tribunal(tribunal_id)
        ON DELETE CASCADE ON UPDATE CASCADE,
    CONSTRAINT fk_tribunal_juez_juez 
        FOREIGN KEY (juez_id) REFERENCES juez(juez_id)
        ON DELETE CASCADE ON UPDATE CASCADE,
    CONSTRAINT chk_fechas_tribunal_juez 
        CHECK (fecha_hasta IS NULL OR fecha_hasta >= fecha_desde)
);


-- ============================================
-- Índices
-- ============================================
CREATE INDEX idx_expediente_fecha_inicio ON expediente(fecha_inicio);
CREATE INDEX idx_expediente_fecha_ultimo_movimiento ON expediente(fecha_ultimo_movimiento);
CREATE INDEX idx_parte_expediente ON parte(numero_expediente);
CREATE INDEX idx_plazo_expediente ON plazo(numero_expediente);
CREATE INDEX idx_plazo_vencimiento ON plazo(fecha_vencimiento);
CREATE INDEX idx_tribunal_jurisdiccion ON tribunal(jurisdiccion_id);
CREATE INDEX idx_tribunal_juez_tribunal ON tribunal_juez(tribunal_id);
CREATE INDEX idx_tribunal_juez_juez ON tribunal_juez(juez_id);
CREATE INDEX idx_tribunal_juez_cargo ON tribunal_juez(cargo);
CREATE INDEX idx_juez_nombre ON juez(nombre);

-- ============================================
-- Comentarios
-- ============================================
COMMENT ON TABLE expediente IS 'Tabla principal que contiene la información de los expedientes judiciales';
COMMENT ON TABLE parte IS 'Personas físicas o jurídicas involucradas en un expediente';
COMMENT ON TABLE rol_parte IS 'Roles específicos que una parte cumple en un expediente';
COMMENT ON TABLE representacion IS 'Relación entre parte y letrado en un expediente';
COMMENT ON TABLE expediente_delito IS 'Asociación N:M entre expedientes y delitos imputados';
COMMENT ON TABLE resolucion IS 'Resoluciones dictadas en un expediente';
COMMENT ON TABLE plazo IS 'Plazos procesales asociados a cada expediente';
COMMENT ON TABLE juez IS 'Magistrados y jueces que integran tribunales';
COMMENT ON TABLE tribunal_juez IS 'Relación N:M entre tribunales y jueces con información de cargo y situación';
COMMENT ON COLUMN tribunal_juez.cargo IS 'Ej: Presidente, Vicepresidente, Juez de Cámara, Vocal, etc.';
COMMENT ON COLUMN tribunal_juez.situacion IS 'Estado del juez en el tribunal: Efectivo, Subrogante, Interino, Suplente';
COMMENT ON COLUMN tribunal_juez.fecha_desde IS 'Fecha de inicio en el cargo (opcional)';
COMMENT ON COLUMN tribunal_juez.fecha_hasta IS 'Fecha de fin en el cargo (NULL si está activo)';

-- ============================================
-- Datos iniciales
-- ============================================
-- Insertar jurisdicción base: Comodoro Py
INSERT INTO jurisdiccion (jurisdiccion_id, ambito, provincia, departamento_judicial)
VALUES (1, 'Federal', 'Ciudad Autónoma de Buenos Aires', 'Comodoro Py')
ON CONFLICT (jurisdiccion_id) DO NOTHING;
