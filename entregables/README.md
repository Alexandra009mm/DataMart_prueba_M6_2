# Carpeta de Entregables y Evidencias de Sustentacion
## Caso: DataMart S.A.S. - Ingenieria de Datos

Esta carpeta esta destinada a almacenar las capturas de pantalla y evidencias requeridas para la entrega y sustentacion del proyecto.

---

### Checklist de Capturas Requeridas:

| Archivo de Captura | Descripcion de la Evidencia | Modulo / Componente |
| :--- | :--- | :--- |
| `01_docker_compose_up.png` | Salida del comando `docker compose ps` mostrando los contenedores activos (`postgres`, `airflow-standalone`, `products-api`). | Infraestructura Docker |
| `02_airflow_connections_variables.png` | Panel de Airflow (`Admin -> Connections` y `Admin -> Variables`) mostrando `postgres_olap`, `retail_currency_default` y `retail_batch_size`. | Configuracion Airflow |
| `03_airflow_dag_success.png` | Grafo de ejecucion del DAG `etl_retail_dag` con todas las tareas en estado `success`. | Orquestacion Airflow |
| `04_calidad_rechazos_db.png` | Consulta SQL sobre `datamart_olap.tabla_rechazos` mostrando registros aislados con motivo y `payload_crudo` en JSONB. | Calidad de Datos |
| `05_modelo_estrella_powerbi.png` | Diagrama del modelo dimensional en **dbdiagram.io** o **Power BI Desktop**, mostrando llaves y relaciones hacia `fact_ventas`. | Modelado Dimensional |
| `06_dashboard_powerbi.png` | Reporte visual en Power BI con graficos de facturacion por categoria y evolucion mensual. | Inteligencia de Negocios |
| `07_consultas_sql_negocio.png` | Ejecucion del script `sql/queries_negocio.sql` respondiendo a las preguntas estrategicas. | Analitica SQL |
| `08_fastapi_products_docs.png` | Documentacion Swagger UI de la API de productos en `http://localhost:8000/docs`. | Microservicios REST |
| `09_jupyter_notebook_visual.png` | Vista del notebook `analisis_negocio_datamart.ipynb` con graficos ejecutados y metricas. | Analitica Visual |
