
-- Configuración inicial
SET client_encoding = 'UTF8';
SET timezone = 'America/Argentina/Buenos_Aires';

-- Crear esquema principal si no existe
CREATE SCHEMA IF NOT EXISTS public;

-- Tabla Fuero
CREATE TABLE fuero (
    fuero_id SERIAL PRIMARY KEY,
    nombre VARCHAR(100) NOT NULL UNIQUE
);

-- Tabla Jurisdicción
CREATE TABLE jurisdiccion (
    jurisdiccion_id SERIAL PRIMARY KEY,
    ambito VARCHAR(50) NOT NULL,
    provincia VARCHAR(50),
    departamento_judicial VARCHAR(100)
);

-- Tabla Tribunal
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

-- Tabla Secretaría
CREATE TABLE secretaria (
    secretaria_id SERIAL PRIMARY KEY,
    nombre VARCHAR(200) NOT NULL,
    tribunal_id INTEGER NOT NULL,
    CONSTRAINT fk_secretaria_tribunal 
        FOREIGN KEY (tribunal_id) REFERENCES tribunal(tribunal_id)
        ON DELETE CASCADE ON UPDATE CASCADE
);

-- Tabla Estado Procesal
CREATE TABLE estado_procesal (
    estado_procesal_id SERIAL PRIMARY KEY,
    nombre VARCHAR(100) NOT NULL UNIQUE,
    etapa VARCHAR(100)
);

-- Tabla Tipo Delito
CREATE TABLE tipo_delito (
    tipo VARCHAR(50) PRIMARY KEY,
    detalle TEXT
);

-- Tabla Letrado
CREATE TABLE letrado (
    letrado_id SERIAL PRIMARY KEY,
    nombre VARCHAR(200) NOT NULL,
    matricula VARCHAR(50),
    colegio VARCHAR(100),
    email VARCHAR(100),
    telefono VARCHAR(50)
);

-- Tabla Expediente (entidad principal)
CREATE TABLE expediente (
    numero_expediente VARCHAR(50) PRIMARY KEY,
    caratula TEXT NOT NULL,
    fecha_inicio DATE NOT NULL,
    fecha_ultimo_movimiento DATE,
    dias_transcurridos INTEGER GENERATED ALWAYS AS (
        CURRENT_DATE - fecha_inicio
    ) STORED,
    dias_inactivos INTEGER GENERATED ALWAYS AS (
        CASE 
            WHEN fecha_ultimo_movimiento IS NULL THEN CURRENT_DATE - fecha_inicio
            ELSE CURRENT_DATE - fecha_ultimo_movimiento
        END
    ) STORED,
    fuero_id INTEGER NOT NULL,
    tribunal_id INTEGER NOT NULL,
    secretaria_id INTEGER,
    estado_procesal_id INTEGER NOT NULL,
    tipo_delito VARCHAR(50),
    letrado_id INTEGER,
    
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
        ON DELETE RESTRICT ON UPDATE CASCADE,
    CONSTRAINT fk_expediente_tipo_delito 
        FOREIGN KEY (tipo_delito) REFERENCES tipo_delito(tipo)
        ON DELETE SET NULL ON UPDATE CASCADE,
    CONSTRAINT fk_expediente_letrado 
        FOREIGN KEY (letrado_id) REFERENCES letrado(letrado_id)
        ON DELETE SET NULL ON UPDATE CASCADE
);

-- Tabla Parte (entidad débil relacionada con expediente)
CREATE TABLE parte (
    documento_cuit VARCHAR(20) NOT NULL,
    numero_expediente VARCHAR(50) NOT NULL,
    tipo_persona VARCHAR(20) NOT NULL CHECK (tipo_persona IN ('fisica', 'juridica')),
    nombre_razon_social VARCHAR(200) NOT NULL,
    
    PRIMARY KEY (documento_cuit, numero_expediente),
    CONSTRAINT fk_parte_expediente 
        FOREIGN KEY (numero_expediente) REFERENCES expediente(numero_expediente)
        ON DELETE CASCADE ON UPDATE CASCADE
);

-- Tabla RolParte (relación muchos a muchos entre expediente y parte con atributos)
CREATE TABLE rol_parte (
    numero_expediente VARCHAR(50) NOT NULL,
    documento_cuit VARCHAR(20) NOT NULL,
    nombre VARCHAR(200) NOT NULL,
    
    PRIMARY KEY (numero_expediente, documento_cuit, nombre),
    CONSTRAINT fk_rol_parte_parte 
        FOREIGN KEY (documento_cuit, numero_expediente) 
        REFERENCES parte(documento_cuit, numero_expediente)
        ON DELETE CASCADE ON UPDATE CASCADE
);

-- Tabla Plazo
CREATE TABLE plazo (
    plazo_id SERIAL PRIMARY KEY,
    numero_expediente VARCHAR(50) NOT NULL,
    tipo VARCHAR(100) NOT NULL,
    fecha_inicio DATE NOT NULL,
    fecha_vencimiento DATE NOT NULL,
    dias_habiles INTEGER,
    estado VARCHAR(20) DEFAULT 'vigente' CHECK (estado IN ('vigente', 'vencido', 'cumplido')),
    tolerancia_dias INTEGER DEFAULT 0,
    dias_restantes INTEGER GENERATED ALWAYS AS (
        fecha_vencimiento - CURRENT_DATE
    ) STORED,
    
    CONSTRAINT fk_plazo_expediente 
        FOREIGN KEY (numero_expediente) REFERENCES expediente(numero_expediente)
        ON DELETE CASCADE ON UPDATE CASCADE,
    CONSTRAINT chk_fechas_plazo CHECK (fecha_vencimiento >= fecha_inicio)
);

-- Índices para mejorar el rendimiento
CREATE INDEX idx_expediente_fecha_inicio ON expediente(fecha_inicio);
CREATE INDEX idx_expediente_fecha_ultimo_movimiento ON expediente(fecha_ultimo_movimiento);
CREATE INDEX idx_expediente_fuero ON expediente(fuero_id);
CREATE INDEX idx_expediente_tribunal ON expediente(tribunal_id);
CREATE INDEX idx_expediente_estado ON expediente(estado_procesal_id);
CREATE INDEX idx_parte_expediente ON parte(numero_expediente);
CREATE INDEX idx_plazo_expediente ON plazo(numero_expediente);
CREATE INDEX idx_plazo_vencimiento ON plazo(fecha_vencimiento);
CREATE INDEX idx_tribunal_jurisdiccion ON tribunal(jurisdiccion_id);

-- Comentarios para documentación
COMMENT ON TABLE expediente IS 'Tabla principal que contiene la información de los expedientes judiciales';
COMMENT ON COLUMN expediente.dias_transcurridos IS 'Días transcurridos desde el inicio del expediente (calculado automáticamente)';
COMMENT ON COLUMN expediente.dias_inactivos IS 'Días sin movimiento en el expediente (calculado automáticamente)';
COMMENT ON TABLE parte IS 'Personas físicas o jurídicas involucradas en un expediente';
COMMENT ON TABLE plazo IS 'Plazos procesales asociados a cada expediente';
COMMENT ON COLUMN plazo.dias_restantes IS 'Días restantes hasta el vencimiento del plazo (calculado automáticamente)';
