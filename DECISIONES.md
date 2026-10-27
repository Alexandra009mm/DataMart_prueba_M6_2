# Registro de Decisiones de Arquitectura (ADR)
## Caso: DataMart S.A.S.

Este documento formaliza las justificaciones de ingenieria de datos, modelado dimensional y decisiones tecnicas implementadas en la solucion de **DataMart S.A.S.**

---

### 1. Tratamiento e Inclusion de Clientes Anonimos (`CLIENTE_ANONIMO`)

* **Contexto:** En el dataset transaccional original, aproximadamente el 20-25% de los registros carecen de identificador de cliente (`CustomerID IS NULL`).
* **Decision Tecnica:** No descartar estas transacciones. Se imputaron a la entidad `"CLIENTE_ANONIMO"` y se agrego la bandera booleana `es_anonimo = TRUE` en `dim_cliente`.
* **Justificacion de Negocio:**
  - Descartar estas transacciones eliminaria **GBP 1,462,743.10 (15.06% del revenue real)** y distorsionaria los inventarios fisicos.
  - Al modelar `CLIENTE_ANONIMO`, la tabla de hechos `fact_ventas` preserva la integridad referencial del 100% de la facturacion contable.
  - Permite contrastar comportamientos: los clientes anonimos exhiben un ticket promedio de **GBP 908.54**, significativamente superior a los registrados (**GBP 371.77**).

---

### 2. Criterio de Clave Compuesta y Prioridad en Deduplicacion

* **Contexto:** La unificacion de `data.csv` (operacional) y `online_retail_II.csv` (historial extendido) presenta solapamiento de transacciones.
* **Decision Tecnica:** Clave de unicidad compuesta:
  $$\text{Clave Deduplicacion} = [\text{InvoiceNo}, \text{StockCode}, \text{InvoiceDate}]$$
  con regla de precedencia `keep='last'`, asignando prioridad de orden `_source_priority = 2` a `online_retail_II.csv`.
* **Justificacion:**
  - Un numero de factura (`InvoiceNo`) contiene multiples lineas de articulos (`StockCode`).
  - La fecha/hora (`InvoiceDate`) resuelve casos donde un SKU fue facturado en ordenes separadas.
  - `online_retail_II.csv` representa la fuente transaccional central con correcciones operativas posteriores.

---

### 3. Estandarizacion de Catalogo y Modelo de Devoluciones

#### A. Descripcion Canonica (`StockCode` y `Description`)
* **Problema:** Variaciones tipograficas y de formato en descripciones para un mismo SKU (mayusculas/minusculas, espacios, etiquetas operativas).
* **Solucion:**
  - `StockCode` normalizado con `.str.strip().str.upper()`.
  - Calculo de la **Descripcion Canonica** mediante la moda estadistica (la descripcion mas frecuente) para cada SKU.
  - Asignacion de las 5 categorias oficiales mediante `category_mapping.csv` con fallback `"Otros"`.

#### B. Modelo de Devoluciones y Revenue Neto
* **Problema:** Las devoluciones se representan con `Quantity < 0`. Si se eliminan, se sobrestima la facturacion.
* **Solucion:**
  - Columnas explicitas `tipo_transaccion` (`'VENTA'` vs `'DEVOLUCION'`), `revenue_bruto` y `revenue_neto`.
  - Para devoluciones: `revenue_bruto = cantidad * precio_unitario` (negativo).
  - Al ejecutar `SUM(revenue_neto)` en SQL o Power BI, las devoluciones se restan directamente sin requerir logica condicional en la capa de presentacion.

#### C. Auditoria y Segregacion de Rechazos (`tabla_rechazos`)
* Las filas con `UnitPrice <= 0` o `Quantity == 0` no ingresan al Data Warehouse analitico.
* Se persisten en `tabla_rechazos` con payload en formato `JSONB` y motivo de rechazo para auditoria forense.

---

### 4. Idempotencia y Desacoplamiento del Pipeline

* **Staging Intermedio en Parquet:**
  - La transformacion genera `./data/staging_clean_retail.parquet` y `./data/staging_rejections_retail.parquet`.
  - Evita almacenar DataFrames pesados en metadatos de Airflow (XCom), permitiendo a las tareas posteriores leer de forma columnar comprimida.
* **Carga Dimensional Idempotente:**
  - Las dimensiones (`dim_cliente`, `dim_producto`, `dim_tiempo`) emplean:
    ```sql
    INSERT INTO {tabla} ({cols}) VALUES ({placeholders})
    ON CONFLICT ({pk}) DO NOTHING;
    ```
  - Re-ejecutar el DAG no genera duplicados de llaves primarias.
* **Conciliacion Financiera Cruzada:**
  - La tarea `audit_and_quality_check` valida en tiempo real:
    $$\sum \text{revenue\_bruto}_{\text{Transformacion}} = \sum \text{revenue\_bruto}_{\text{PostgreSQL Fact}}$$
    con tolerancia de $\pm 1.0$ GBP, garantizando consistencia antes de la exposicion en BI.

---

### 5. Aislamiento de Catalogos en PostgreSQL y Optimizacion de Consultas

* **Contexto de Bases de Datos:**
  - `datamart_oltp` y `datamart_olap` son bases de datos independientes.
  - Para evitar extensiones complejas (`dblink`/`postgres_fdw`), el script `sql/queries_negocio.sql` implementa conmutaciones explicitas de conexion (`\c datamart_oltp;` y `\c datamart_olap;`).
* **Optimizacion de Consulta de Catalogo (Consulta 6):**
  - Se reescribio la consulta a un CTE unico (`resumen_skus`) que agrupa por `stock_code` y calcula `COUNT(DISTINCT description)`.
  - La agregacion condicional calcula el total de productos y las inconsistencias en un solo escaneo, evitando ordenamientos pesados de cadenas.

---

### 6. Orquestacion y Seguridad en Airflow 3.1.5

* **Modularizacion con TaskGroups:**
  - `validate_inputs` -> `extract_transform_validate` -> `load_staging_parquet` -> `capa_operacional_oltp` -> `capa_analitica_olap` -> `audit_and_quality_check`.
* **Consistencia de Cifrado con Fernet Key:**
  - Se definio `AIRFLOW__CORE__FERNET_KEY` estatica en `.env` para asegurar que las credenciales cifradas por `airflow-init` puedan ser descifradas por `airflow-standalone`.
* **Autenticacion en Airflow 3 (SimpleAuthManager):**
  - Se configuro el archivo `simple_auth_manager_passwords.json` con credenciales `admin` / `admin`, `AIRFLOW__API_AUTH__JWT_SECRET` y `AIRFLOW__CORE__SIMPLE_AUTH_MANAGER_ALL_ADMINS: "true"`, asegurando acceso administrativo directo y estable.

---

### 7. Microservicio REST de Productos (FastAPI)

* **Objetivo:** Exponer el catalogo normalizado de **3,827 productos** directamente desde `datamart_olap.dim_producto`.
* **Componentes:**
  - Contenedor dedicado `products-api` en el puerto **8000**.
  - Paginacion SQL nativa (`skip`, `limit` con maximo de 200 items por llamada).
  - Control de tasa (Rate Limiting) con SlowAPI a 60 peticiones por minuto por IP (respondiendo HTTP 429 en caso de exceso).
