# Entregables: Arquitectura, Diagrama del Modelo y Consultas SQL Oficiales
## DataMart S.A.S. - Ingenieria de Datos

Este documento reune los insumos tecnicos para la sustentacion y generacion de evidencias del repositorio analitico:

---

## 1. Arquitectura General del Proyecto

```mermaid
flowchart TD
    subgraph S1 ["1. Fuentes de Datos (Raw CSVs)"]
        F1["data/data.csv\n(Transacciones operacionales)"]
        F2["data/online_retail_II.csv\n(Historial consolidado)"]
        F3["data/category_mapping.csv\n(Mapeo oficial 5 categorias)"]
    end

    subgraph S2 ["2. Orquestacion y Pipeline ETL (Airflow 3.1.5)"]
        T1["extract_and_unify\nDeduplicacion keep='last'"]
        T2["transform_and_clean\nFechas UTC, CLIENTE_ANONIMO, Moda canonica"]
        T3["load_staging_parquet\nPersistencia columnar"]
        T4["load_oltp_database\nCarga operacional"]
        T5["load_olap_star_schema\nCarga dimensional en estrella"]

        T1 --> T2 --> T3
        T3 --> T4
        T3 --> T5
    end

    subgraph S3 ["3. Staging Intermedio"]
        P1["data/staging_clean_retail.parquet\n(528,743 filas validadas)"]
        P2["data/staging_rejections_retail.parquet\n(5,042 anomalias segregadas)"]
    end

    subgraph S4 ["4. Repositorio de Datos (PostgreSQL 15 - Puerto 5434)"]
        subgraph OLTP ["datamart_oltp"]
            DB_OLTP[("transacciones_raw")]
        end
        subgraph OLAP ["datamart_olap (Modelo Estrella)"]
            DIM_P[("dim_producto\n(3,827 SKUs)")]
            DIM_C[("dim_cliente\n(4,372 Clientes)")]
            DIM_T[("dim_tiempo\n(21,802 Fechas)")]
            FACT_V[("fact_ventas\n(528,743 Hechos)")]
            RECHAZOS[("tabla_rechazos\n(Auditoria JSONB)")]

            DIM_P --- FACT_V
            DIM_C --- FACT_V
            DIM_T --- FACT_V
        end
    end

    subgraph S5 ["5. Capa de Consumo y Analitica"]
        API["FastAPI Microservice (Puerto 8000)\nPaginacion SQL y Rate Limiting"]
        JUPYTER["Jupyter Notebook\nanalisis_negocio_datamart.ipynb"]
        BI["Power BI Desktop / DBeaver / pgAdmin\n(Puerto 5434)"]
    end

    F1 & F2 & F3 --> T1
    T3 --> P1 & P2
    T4 --> DB_OLTP
    T5 --> OLAP
    DIM_P --> API
    OLAP --> JUPYTER
    OLAP --> BI
```

---

## 2. Diagrama del Modelo Dimensional (`datamart_olap`)

### A. Diagrama Mermaid (ERD)
```mermaid
erDiagram
    dim_cliente ||--o{ fact_ventas : "realiza"
    dim_producto ||--o{ fact_ventas : "contiene"
    dim_tiempo ||--o{ fact_ventas : "ocurre_en"

    dim_cliente {
        varchar cliente_id PK "ID de cliente o CLIENTE_ANONIMO"
        varchar pais "Pais del cliente"
        boolean es_anonimo "Bandera de cliente no registrado"
    }

    dim_producto {
        varchar stock_code PK "Codigo SKU del producto"
        text descripcion_canonica "Descripcion consolidada por moda"
        varchar categoria "Categoria oficial homologada"
    }

    dim_tiempo {
        timestamp tiempo_id PK "Timestamp en UTC"
        int anio "Ano de la transaccion"
        int mes "Mes (1 - 12)"
        int dia "Dia del mes (1 - 31)"
        varchar dia_semana "Nombre del dia de la semana"
    }

    fact_ventas {
        serial fact_id PK "Identificador subrogado"
        varchar invoice_no "Numero de factura (prefijo C para devoluciones)"
        varchar stock_code FK "FK a dim_producto"
        varchar cliente_id FK "FK a dim_cliente"
        timestamp tiempo_id FK "FK a dim_tiempo"
        varchar tipo_transaccion "VENTA o DEVOLUCION"
        int cantidad "Unidades vendidas o devueltas"
        numeric precio_unitario "Precio unitario en GBP"
        numeric revenue_bruto "Monto bruto sin cancelaciones"
        numeric revenue_neto "Monto neto imputable"
    }

    tabla_rechazos {
        serial rechazo_id PK "ID autoincremental"
        varchar fuente_origen "Archivo de origen"
        text motivo "Causa de exclusion"
        jsonb payload_crudo "Registro original en JSON"
        timestamp fecha_rechazo "Fecha y hora de auditoria"
    }
```

### B. Codigo DBML para dbdiagram.io
Copiar y pegar en [https://dbdiagram.io](https://dbdiagram.io):

```dbml
Table dim_cliente {
  cliente_id varchar(50) [pk, note: "ID unico o CLIENTE_ANONIMO"]
  pais varchar(100) [note: "Pais del cliente"]
  es_anonimo boolean [default: false, note: "Flag de cliente anonimo"]
}

Table dim_producto {
  stock_code varchar(50) [pk, note: "SKU unico de producto"]
  descripcion_canonica text [note: "Descripcion mas frecuente"]
  categoria varchar(50) [note: "Categoria oficial asignada"]
}

Table dim_tiempo {
  tiempo_id timestamp [pk, note: "Timestamp en UTC"]
  anio int [note: "Ano"]
  mes int [note: "Mes"]
  dia int [note: "Dia"]
  dia_semana varchar(20) [note: "Dia de la semana"]
}

Table fact_ventas {
  fact_id serial [pk, note: "ID subrogado"]
  invoice_no varchar(50) [note: "Factura (prefijo C para devolucion)"]
  stock_code varchar(50) [ref: > dim_producto.stock_code, note: "FK a dim_producto"]
  cliente_id varchar(50) [ref: > dim_cliente.cliente_id, note: "FK a dim_cliente"]
  tiempo_id timestamp [ref: > dim_tiempo.tiempo_id, note: "FK a dim_tiempo"]
  tipo_transaccion varchar(20) [note: "VENTA o DEVOLUCION"]
  cantidad int [note: "Unidades"]
  precio_unitario numeric(10, 2) [note: "Precio unitario"]
  revenue_bruto numeric(12, 2) [note: "Revenue bruto"]
  revenue_neto numeric(12, 2) [note: "Revenue neto"]
}

Table tabla_rechazos {
  rechazo_id serial [pk, note: "ID del rechazo"]
  fuente_origen varchar(100) [note: "Archivo de origen"]
  motivo text [note: "Causa de rechazo"]
  payload_crudo jsonb [note: "Payload original"]
  fecha_rechazo timestamp [note: "Fecha de rechazo"]
}
```

---

## 3. Consultas SQL de Validacion Oficiales (Seccion 7)

Conexion recomendada: `datamart_olap` en `localhost:5434` (usuario `airflow` o `postgres`).

### Consulta 1: Evolucion mensual de ventas netas
```sql
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
```

### Consulta 2: Revenue bruto y devoluciones por categoria
```sql
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
```

### Consulta 3A: Top 10 productos con mayor revenue neto
```sql
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
```

### Consulta 3B: Top 10 productos con mayor tasa de devolucion (Min. 50 unidades vendidas)
```sql
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
```

### Consulta 4: Paises con mayor volumen y variacion de ticket promedio
```sql
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
```

### Consulta 5: Comportamiento: Clientes identificados vs anonimos
```sql
SELECT 
    CASE WHEN c.es_anonimo THEN 'Clientes Anonimos' ELSE 'Clientes Identificados' END AS segmento_cliente,
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
```

### Consulta 6: Total de codigos unicos y auditoria de catalogo
```sql
-- Ejecutable directamente en datamart_olap
SELECT 
    COUNT(DISTINCT stock_code) AS total_skus_unicos_catalogo,
    COUNT(CASE WHEN categoria = 'Otros' THEN 1 END) AS skus_sin_mapeo_categoria,
    COUNT(CASE WHEN categoria != 'Otros' THEN 1 END) AS skus_con_categoria_oficial
FROM dim_producto;

-- Auditoria en datamart_oltp (ejecutar en base datamart_oltp):
-- SELECT COUNT(DISTINCT stock_code) FROM transacciones_raw;
-- SELECT stock_code, COUNT(DISTINCT description) FROM transacciones_raw GROUP BY stock_code HAVING COUNT(DISTINCT description) > 1;
```

### Consulta 7: Respaldo numerico para recomendacion al equipo de producto
```sql
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
        NULLIF(SUM(CASE WHEN f.tipo_transaccion = 'VENTA' THEN f.cantidad ELSE 0 END), 0)::NUMERIC) >= 0.50
ORDER BY costo_financiero_devolucion DESC
LIMIT 10;
```
