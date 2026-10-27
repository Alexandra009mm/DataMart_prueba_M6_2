-- 03_init_olap.sql
-- Caso: DataMart S.A.S.
-- Conexión a la base de datos analítica datamart_olap y creación del modelo dimensional estrella y tabla de rechazos

\c datamart_olap;

-- 1. Tabla de Auditoría y Rechazos de Calidad de Datos
CREATE TABLE IF NOT EXISTS tabla_rechazos (
    rechazo_id SERIAL PRIMARY KEY,
    fuente_origen VARCHAR(100) NOT NULL,
    motivo TEXT NOT NULL,
    payload_crudo JSONB,
    fecha_rechazo TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_rechazos_motivo ON tabla_rechazos(motivo);
CREATE INDEX IF NOT EXISTS idx_rechazos_fecha ON tabla_rechazos(fecha_rechazo);

-- 2. Dimensión Cliente (Soporta Clientes Identificados y Anónimos)
CREATE TABLE IF NOT EXISTS dim_cliente (
    cliente_id VARCHAR(50) PRIMARY KEY,
    pais VARCHAR(100),
    es_anonimo BOOLEAN NOT NULL DEFAULT FALSE
);

-- 3. Dimensión Producto (Descripción canónica estandarizada y categoría homologada)
CREATE TABLE IF NOT EXISTS dim_producto (
    stock_code VARCHAR(50) PRIMARY KEY,
    descripcion_canonica TEXT,
    categoria VARCHAR(50)
);

-- 4. Dimensión Tiempo (Grano por timestamp exacto normalizado en UTC)
CREATE TABLE IF NOT EXISTS dim_tiempo (
    tiempo_id TIMESTAMP PRIMARY KEY,
    anio INT NOT NULL,
    mes INT NOT NULL,
    dia INT NOT NULL,
    dia_semana VARCHAR(20) NOT NULL
);

-- 5. Tabla de Hechos: fact_ventas (Métricas de ventas brutas, netas y devoluciones)
CREATE TABLE IF NOT EXISTS fact_ventas (
    fact_id SERIAL PRIMARY KEY,
    invoice_no VARCHAR(50) NOT NULL,
    stock_code VARCHAR(50) NOT NULL REFERENCES dim_producto(stock_code),
    cliente_id VARCHAR(50) NOT NULL REFERENCES dim_cliente(cliente_id),
    tiempo_id TIMESTAMP NOT NULL REFERENCES dim_tiempo(tiempo_id),
    tipo_transaccion VARCHAR(20) NOT NULL, -- 'VENTA' o 'DEVOLUCION'
    cantidad INT NOT NULL,
    precio_unitario NUMERIC(10, 2) NOT NULL,
    revenue_bruto NUMERIC(12, 2) NOT NULL,
    revenue_neto NUMERIC(12, 2) NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_fact_invoice ON fact_ventas(invoice_no);
CREATE INDEX IF NOT EXISTS idx_fact_stock ON fact_ventas(stock_code);
CREATE INDEX IF NOT EXISTS idx_fact_cliente ON fact_ventas(cliente_id);
CREATE INDEX IF NOT EXISTS idx_fact_tiempo ON fact_ventas(tiempo_id);
CREATE INDEX IF NOT EXISTS idx_fact_tipo ON fact_ventas(tipo_transaccion);
