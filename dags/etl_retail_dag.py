"""
etl_retail_dag.py
-----------------
Pipeline de Ingeniería de Datos para DataMart S.A.S.
Implementado con la TaskFlow API de Apache Airflow (@dag, @task) y TaskGroups.

Estructura del Grafo de Ejecución:
  1. validate_inputs: Verificación de fuentes de datos.
  2. extract_transform_validate: Limpieza, segregación de rechazos y persistencia en Parquet.
  3. TaskGroup 'capa_operacional_oltp' -> load_to_oltp: Ingesta en datamart_oltp.
  4. TaskGroup 'capa_analitica_olap' -> load_to_olap: Ingesta en datamart_olap (Estrella + Rechazos).
  5. audit_and_quality_check: Reconciliación cruzada de integridad y balance de revenue.
"""

import logging
import sys
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict

import pandas as pd
try:
    from airflow.sdk import TaskGroup, dag, task
except ImportError:
    from airflow.decorators import dag, task
    from airflow.utils.task_group import TaskGroup
from sqlalchemy import create_engine, text

# Ajuste de path para importaciones portables
CURRENT_DIR = Path(__file__).resolve().parent
if str(CURRENT_DIR) not in sys.path:
    sys.path.append(str(CURRENT_DIR))

try:
    from modules.config import (
        CATEGORY_MAPPING_PATH,
        DATA_CSV_PATH,
        ONLINE_RETAIL_II_PATH,
        OLAP_CONN_URI,
        OLTP_CONN_URI,
        PRODUCTS_API_URL,
        STAGING_CLEAN_PARQUET,
        STAGING_REJECTIONS_PARQUET,
    )
    from modules.olap import build_dimensional_model
    from modules.transform import load_and_clean_data
except ImportError:
    from dags.modules.config import (
        CATEGORY_MAPPING_PATH,
        DATA_CSV_PATH,
        ONLINE_RETAIL_II_PATH,
        OLAP_CONN_URI,
        OLTP_CONN_URI,
        PRODUCTS_API_URL,
        STAGING_CLEAN_PARQUET,
        STAGING_REJECTIONS_PARQUET,
    )
    from dags.modules.olap import build_dimensional_model
    from dags.modules.transform import load_and_clean_data

logger = logging.getLogger("airflow.task")

default_args = {
    "owner": "data_engineer",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=1),
}


@dag(
    dag_id="etl_retail_oltp_olap",
    default_args=default_args,
    description="Pipeline ETL DataMart S.A.S.: Ciclo de Vida Completo OLTP/OLAP con Parquet Staging",
    schedule="@daily",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["datamart", "etl", "oltp", "olap", "taskflow", "taskgroup"],
)
def etl_retail_dag():
    """
    Definición del pipeline modular con trazabilidad estricta y staging en Parquet.
    """

    @task
    def validate_inputs() -> Dict[str, str]:
        """
        Tarea 1: Valida la presencia física exclusiva de las 2 fuentes transaccionales
        oficiales (data.csv y online_retail_II.csv) y el mapeo de categorías.
        """
        logger.info("=== Tarea 1: Validación de Entradas ===")
        required_files = {
            "data_csv": DATA_CSV_PATH,
            "online_retail_ii": ONLINE_RETAIL_II_PATH,
            "category_mapping": CATEGORY_MAPPING_PATH,
        }

        validated = {}
        for key, filepath in required_files.items():
            if not filepath.exists():
                raise FileNotFoundError(
                    f"Archivo crítico no encontrado en el volumen: {filepath.resolve()}"
                )
            size_mb = filepath.stat().st_size / (1024 * 1024)
            logger.info(f"  ✔ {filepath.name} presente ({size_mb:.2f} MB)")
            validated[key] = str(filepath.resolve())

        return validated

    @task
    def extract_transform_validate(validated_paths: Dict[str, str]) -> Dict[str, Any]:
        """
        Tarea 2: Ejecuta limpieza, deduplicación, clasificación de transacciones y
        persiste datasets en staging Parquet para desacoplar memoria de XCom.
        """
        logger.info("=== Tarea 2: Extracción, Transformación y Calidad de Datos ===")
        df_clean, df_rejections = load_and_clean_data()

        total_clean = len(df_clean)
        total_rejections = len(df_rejections)
        total_gross_revenue = float(df_clean["revenue_bruto"].sum())
        total_net_revenue = float(df_clean["revenue_neto"].sum())

        logger.info(f"Persistiendo staging limpio en: {STAGING_CLEAN_PARQUET}")
        df_clean.to_parquet(STAGING_CLEAN_PARQUET, index=False, engine="pyarrow")

        logger.info(f"Persistiendo staging de rechazos en: {STAGING_REJECTIONS_PARQUET}")
        df_rejections.to_parquet(STAGING_REJECTIONS_PARQUET, index=False, engine="pyarrow")

        metrics = {
            "clean_parquet_path": str(STAGING_CLEAN_PARQUET.resolve()),
            "rejections_parquet_path": str(STAGING_REJECTIONS_PARQUET.resolve()),
            "total_clean_rows": total_clean,
            "total_rejections_rows": total_rejections,
            "total_gross_revenue": round(total_gross_revenue, 2),
            "total_net_revenue": round(total_net_revenue, 2),
        }
        logger.info(f"Métricas de transformación calculadas: {metrics}")
        return metrics

    @task
    def load_to_oltp(metadata: Dict[str, Any]) -> int:
        """
        Inserta las transacciones limpias en datamart_oltp.transacciones_raw.
        """
        logger.info("=== Capa Operacional: Cargando en datamart_oltp ===")
        df_clean = pd.read_parquet(metadata["clean_parquet_path"])

        df_oltp = df_clean[[
            "InvoiceNo",
            "StockCode",
            "Description",
            "Quantity",
            "InvoiceDate",
            "UnitPrice",
            "CustomerID",
            "Country",
            "Category",
            "tipo_transaccion",
        ]].rename(columns={
            "InvoiceNo": "invoice_no",
            "StockCode": "stock_code",
            "Description": "description",
            "Quantity": "quantity",
            "InvoiceDate": "invoice_date",
            "UnitPrice": "unit_price",
            "CustomerID": "customer_id",
            "Country": "country",
            "Category": "category",
        })

        engine_oltp = create_engine(OLTP_CONN_URI, pool_pre_ping=True)
        with engine_oltp.begin() as conn:
            conn.execute(text("TRUNCATE TABLE transacciones_raw;"))

        logger.info(f"Insertando {len(df_oltp):,} registros en 'transacciones_raw'...")
        df_oltp.to_sql(
            name="transacciones_raw",
            con=engine_oltp,
            if_exists="append",
            index=False,
            method="multi",
            chunksize=5000,
        )
        engine_oltp.dispose()
        logger.info("   Carga operacional completada.")
        return len(df_oltp)

    @task
    def load_to_olap(metadata: Dict[str, Any]) -> int:
        """
        Construye el modelo estrella y persiste rechazos en datamart_olap.
        """
        logger.info("=== Capa Analítica: Cargando en datamart_olap ===")
        df_clean = pd.read_parquet(metadata["clean_parquet_path"])
        df_rejections = pd.read_parquet(metadata["rejections_parquet_path"])

        build_dimensional_model(df_clean, df_rejections)
        return len(df_clean)

    @task
    def audit_and_quality_check(
        transform_metrics: Dict[str, Any], oltp_rows: int, olap_rows: int
    ) -> None:
        """
        Tarea 5: Conciliación de integridad cruzada y verificación de revenue.
        """
        logger.info("=== Tarea 5: Auditoría Cruzada y Control de Calidad ===")
        expected_rows = transform_metrics["total_clean_rows"]
        expected_gross_revenue = transform_metrics["total_gross_revenue"]

        logger.info(
            f"Verificando filas: Transformación={expected_rows:,} | OLTP={oltp_rows:,} | OLAP={olap_rows:,}"
        )
        if oltp_rows != expected_rows or olap_rows != expected_rows:
            raise ValueError(
                f"Discrepancia en conteo de filas: Esperadas={expected_rows}, OLTP={oltp_rows}, OLAP={olap_rows}"
            )

        # Validación contable contra el DataMart en PostgreSQL
        engine_olap = create_engine(OLAP_CONN_URI, pool_pre_ping=True)
        with engine_olap.connect() as conn:
            query = text("SELECT ROUND(COALESCE(SUM(revenue_bruto), 0), 2) FROM fact_ventas;")
            db_gross_revenue = float(conn.execute(query).scalar())

            # Validación de rechazos auditados
            query_rechazos = text("SELECT COUNT(*) FROM tabla_rechazos;")
            db_rechazos_count = int(conn.execute(query_rechazos).scalar())

        engine_olap.dispose()

        logger.info(f"Revenue Bruto: Transformación=${expected_gross_revenue:,.2f} | DB Fact=${db_gross_revenue:,.2f}")
        logger.info(f"Rechazos persistidos en tabla_rechazos: {db_rechazos_count:,}")

        # Tolerancia por precisión de redondeo flotante
        diff = abs(expected_gross_revenue - db_gross_revenue)
        if diff > 1.0:
            raise ValueError(
                f"Discrepancia financiera en revenue bruto superior a tolerancia: diff={diff:.2f}"
            )

        logger.info("Auditoria de integridad y conciliacion financiera completada con exito.")

    # -----------------------------------------------------------------------
    # Orquestación de dependencias explícitas con TaskGroups
    # -----------------------------------------------------------------------
    sources_ok = validate_inputs()
    t_metrics = extract_transform_validate(sources_ok)

    with TaskGroup("capa_operacional_oltp") as tg_oltp:
        oltp_res = load_to_oltp(t_metrics)

    with TaskGroup("capa_analitica_olap") as tg_olap:
        olap_res = load_to_olap(t_metrics)

    # La capa analítica depende de la finalización de la capa operacional
    tg_oltp >> tg_olap

    # Auditoría final
    audit_and_quality_check(t_metrics, oltp_res, olap_res)


etl_retail_pipeline = etl_retail_dag()
