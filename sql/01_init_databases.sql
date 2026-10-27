-- 01_init_databases.sql
-- Caso: DataMart S.A.S.
-- Inicialización de bases de datos operacional (OLTP) y analítica (OLAP)

CREATE DATABASE datamart_oltp;
GRANT ALL PRIVILEGES ON DATABASE datamart_oltp TO airflow;

CREATE DATABASE datamart_olap;
GRANT ALL PRIVILEGES ON DATABASE datamart_olap TO airflow;
