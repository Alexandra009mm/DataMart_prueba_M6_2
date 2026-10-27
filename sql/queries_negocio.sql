-- ============================================================================
-- Caso: DataMart S.A.S. - Consultas Analíticas Oficiales de Negocio
-- Base de Datos: datamart_olap
-- ============================================================================

\c datamart_olap;

-- ----------------------------------------------------------------------------
-- CONSULTA 1: Evolución mensual de ventas netas (restando devoluciones)
-- Objetivo: Medir el crecimiento orgánico mes a mes descontando el impacto de devoluciones.
-- ----------------------------------------------------------------------------
SELECT 
    t.anio,
    t.mes,
    TO_CHAR(t.tiempo_id, 'TMMonth YYYY') AS periodo,
    COUNT(CASE WHEN f.tipo_transaccion = 'VENTA' THEN 1 END) AS transacciones_venta,
    COUNT(CASE WHEN f.tipo_transaccion = 'DEVOLUCION' THEN 1 END) AS transacciones_devolucion,
    ROUND(SUM(CASE WHEN f.tipo_transaccion = 'VENTA' THEN f.revenue_bruto ELSE 0 END), 2) AS revenue_ventas,
    ROUND(SUM(CASE WHEN f.tipo_transaccion = 'DEVOLUCION' THEN ABS(f.revenue_bruto) ELSE 0 END), 2) AS revenue_devoluciones,
    ROUND(SUM(f.revenue_neto), 2) AS revenue_neto
FROM fact_ventas f
JOIN dim_tiempo t ON f.tiempo_id = t.tiempo_id
GROUP BY t.anio, t.mes, TO_CHAR(t.tiempo_id, 'TMMonth YYYY')
ORDER BY t.anio, t.mes;


-- ----------------------------------------------------------------------------
-- CONSULTA 2: Revenue bruto y porcentaje de devoluciones por categoría
-- Objetivo: Identificar qué líneas de catálogo tienen mayor fricción o insatisfacción.
-- ----------------------------------------------------------------------------
SELECT 
    p.categoria,
    ROUND(SUM(CASE WHEN f.tipo_transaccion = 'VENTA' THEN f.revenue_bruto ELSE 0 END), 2) AS revenue_ventas_bruto,
    ROUND(SUM(CASE WHEN f.tipo_transaccion = 'DEVOLUCION' THEN ABS(f.revenue_bruto) ELSE 0 END), 2) AS revenue_devoluciones,
    ROUND(SUM(f.revenue_neto), 2) AS revenue_neto,
    ROUND(
        (SUM(CASE WHEN f.tipo_transaccion = 'DEVOLUCION' THEN ABS(f.revenue_bruto) ELSE 0 END)::NUMERIC / 
        NULLIF(SUM(CASE WHEN f.tipo_transaccion = 'VENTA' THEN f.revenue_bruto ELSE 0 END), 0)::NUMERIC) * 100, 
        2
    ) AS pct_tasa_devolucion_monetaria
FROM fact_ventas f
JOIN dim_producto p ON f.stock_code = p.stock_code
GROUP BY p.categoria
ORDER BY revenue_neto DESC;


-- ----------------------------------------------------------------------------
-- CONSULTA 3A: Top 10 productos con mayor revenue neto
-- Objetivo: Descubrir los SKU estrella con mayor aporte positivo a la caja.
-- ----------------------------------------------------------------------------
SELECT 
    f.stock_code,
    p.descripcion_canonica,
    p.categoria,
    SUM(f.cantidad) AS unidades_netas,
    ROUND(SUM(f.revenue_neto), 2) AS total_revenue_neto
FROM fact_ventas f
JOIN dim_producto p ON f.stock_code = p.stock_code
GROUP BY f.stock_code, p.descripcion_canonica, p.categoria
HAVING SUM(f.revenue_neto) > 0
ORDER BY total_revenue_neto DESC
LIMIT 10;


-- ----------------------------------------------------------------------------
-- CONSULTA 3B: Top 10 productos con mayor tasa de devolución (mínimo 50 unidades vendidas)
-- Objetivo: Detectar SKUs problemáticos (posible defecto de fábrica o empaque).
-- ----------------------------------------------------------------------------
SELECT 
    f.stock_code,
    p.descripcion_canonica,
    p.categoria,
    SUM(CASE WHEN f.tipo_transaccion = 'VENTA' THEN f.cantidad ELSE 0 END) AS unidades_vendidas,
    SUM(CASE WHEN f.tipo_transaccion = 'DEVOLUCION' THEN ABS(f.cantidad) ELSE 0 END) AS unidades_devueltas,
    ROUND(
        (SUM(CASE WHEN f.tipo_transaccion = 'DEVOLUCION' THEN ABS(f.cantidad) ELSE 0 END)::NUMERIC / 
        NULLIF(SUM(CASE WHEN f.tipo_transaccion = 'VENTA' THEN f.cantidad ELSE 0 END), 0)::NUMERIC) * 100,
        2
    ) AS pct_tasa_devolucion_unidades,
    ROUND(SUM(CASE WHEN f.tipo_transaccion = 'DEVOLUCION' THEN ABS(f.revenue_bruto) ELSE 0 END), 2) AS impacto_monetario_perdido
FROM fact_ventas f
JOIN dim_producto p ON f.stock_code = p.stock_code
GROUP BY f.stock_code, p.descripcion_canonica, p.categoria
HAVING SUM(CASE WHEN f.tipo_transaccion = 'VENTA' THEN f.cantidad ELSE 0 END) >= 50
ORDER BY pct_tasa_devolucion_unidades DESC
LIMIT 10;


-- ----------------------------------------------------------------------------
-- CONSULTA 4: Ventas y ticket promedio por país
-- Objetivo: Evaluar la rentabilidad geográfica y el tamaño de canasta por mercado.
-- ----------------------------------------------------------------------------
SELECT 
    c.pais,
    COUNT(DISTINCT f.invoice_no) AS total_ordenes,
    COUNT(DISTINCT f.cliente_id) AS total_clientes_unicos,
    ROUND(SUM(f.revenue_neto), 2) AS revenue_neto_total,
    ROUND(SUM(f.revenue_neto) / NULLIF(COUNT(DISTINCT f.invoice_no), 0), 2) AS ticket_promedio_por_orden
FROM fact_ventas f
JOIN dim_cliente c ON f.cliente_id = c.cliente_id
WHERE f.tipo_transaccion = 'VENTA'
GROUP BY c.pais
ORDER BY revenue_neto_total DESC;


-- ----------------------------------------------------------------------------
-- CONSULTA 5: Comparación de comportamiento de compra: Clientes identificados vs anónimos
-- Objetivo: Justificar el valor de la captura de identidad en checkout.
-- ----------------------------------------------------------------------------
SELECT 
    CASE WHEN c.es_anonimo THEN 'Clientes Anónimos' ELSE 'Clientes Identificados' END AS segmento_cliente,
    COUNT(DISTINCT f.invoice_no) AS total_pedidos,
    COUNT(DISTINCT f.cliente_id) AS clientes_unicos,
    SUM(f.cantidad) AS volumen_unidades,
    ROUND(SUM(f.revenue_neto), 2) AS revenue_neto_total,
    ROUND(
        (SUM(f.revenue_neto) / (SELECT SUM(revenue_neto) FROM fact_ventas)) * 100, 
        2
    ) AS pct_participacion_facturacion,
    ROUND(SUM(f.revenue_neto) / NULLIF(COUNT(DISTINCT f.invoice_no), 0), 2) AS ticket_promedio
FROM fact_ventas f
JOIN dim_cliente c ON f.cliente_id = c.cliente_id
GROUP BY c.es_anonimo;


-- ----------------------------------------------------------------------------
-- CONSULTA 6: Productos con descripciones inconsistentes y total de códigos únicos
-- Objetivo: Auditar la calidad del catálogo maestro en la base de datos operacional.
-- ----------------------------------------------------------------------------
\c datamart_oltp;

WITH resumen_skus AS (
    SELECT 
        stock_code,
        COUNT(DISTINCT description) AS variantes
    FROM transacciones_raw
    WHERE description IS NOT NULL
    GROUP BY stock_code
)
SELECT 
    COUNT(*) AS total_skus_unicos_catalogo,
    COUNT(CASE WHEN variantes > 1 THEN 1 END) AS skus_con_descripciones_inconsistentes,
    ROUND(
        (COUNT(CASE WHEN variantes > 1 THEN 1 END)::NUMERIC / NULLIF(COUNT(*), 0)::NUMERIC) * 100,
        2
    ) AS pct_inconsistencia_catalogo
FROM resumen_skus;

-- Regresar el contexto para la consulta 7
\c datamart_olap;


-- ----------------------------------------------------------------------------
-- CONSULTA 7: Consulta agregada que respalda con números una recomendación a Producto
-- Objetivo: Recomendación comercial basada en el cuadrante de Alta Demanda vs Alta Devolución.
-- ----------------------------------------------------------------------------
SELECT 
    p.categoria,
    p.stock_code,
    p.descripcion_canonica,
    SUM(CASE WHEN f.tipo_transaccion = 'VENTA' THEN f.cantidad ELSE 0 END) AS ventas_unidades,
    SUM(CASE WHEN f.tipo_transaccion = 'DEVOLUCION' THEN ABS(f.cantidad) ELSE 0 END) AS devoluciones_unidades,
    ROUND(
        (SUM(CASE WHEN f.tipo_transaccion = 'DEVOLUCION' THEN ABS(f.cantidad) ELSE 0 END)::NUMERIC / 
        NULLIF(SUM(CASE WHEN f.tipo_transaccion = 'VENTA' THEN f.cantidad ELSE 0 END), 0)::NUMERIC) * 100,
        2
    ) AS tasa_devolucion_pct,
    ROUND(SUM(CASE WHEN f.tipo_transaccion = 'DEVOLUCION' THEN ABS(f.revenue_bruto) ELSE 0 END), 2) AS costo_financiero_devolucion
FROM fact_ventas f
JOIN dim_producto p ON f.stock_code = p.stock_code
GROUP BY p.categoria, p.stock_code, p.descripcion_canonica
HAVING SUM(CASE WHEN f.tipo_transaccion = 'VENTA' THEN f.cantidad ELSE 0 END) >= 100
   AND (SUM(CASE WHEN f.tipo_transaccion = 'DEVOLUCION' THEN ABS(f.cantidad) ELSE 0 END)::NUMERIC / 
        NULLIF(SUM(CASE WHEN f.tipo_transaccion = 'VENTA' THEN f.cantidad ELSE 0 END), 0)::NUMERIC) > 0.15
ORDER BY costo_financiero_devolucion DESC
LIMIT 10;
