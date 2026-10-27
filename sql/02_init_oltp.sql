-- 02_init_oltp.sql
-- Caso: DataMart S.A.S.
-- Conexión a la base de datos operacional datamart_oltp y creación de transacciones_raw

\c datamart_oltp;

CREATE TABLE IF NOT EXISTS transacciones_raw (
    invoice_no VARCHAR(50),
    stock_code VARCHAR(50),
    description TEXT,
    quantity INTEGER,
    invoice_date TIMESTAMP,
    unit_price NUMERIC(10, 2),
    customer_id VARCHAR(50),
    country VARCHAR(100),
    category VARCHAR(100),
    tipo_transaccion VARCHAR(20),
    fecha_carga TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_transacciones_invoice ON transacciones_raw(invoice_no);
CREATE INDEX IF NOT EXISTS idx_transacciones_stock ON transacciones_raw(stock_code);
CREATE INDEX IF NOT EXISTS idx_transacciones_fecha ON transacciones_raw(invoice_date);
