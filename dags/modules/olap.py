"""
olap.py
-------
Módulo de construcción y persistencia del modelo dimensional estrella y
auditoría de rechazos en la base de datos analítica 'datamart_olap'.
"""

import logging
from typing import Optional

import pandas as pd
from sqlalchemy import create_engine, text

try:
    from modules.config import OLAP_CONN_URI
except ImportError:
    from dags.modules.config import OLAP_CONN_URI

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)


def _build_dim_tiempo(df: pd.DataFrame) -> pd.DataFrame:
    """
    Construye la dimensión temporal a partir del timestamp normalizado de InvoiceDate.
    Clave primaria: tiempo_id (TIMESTAMP).
    """
    logger.info("Construyendo dimensión tiempo (dim_tiempo)...")
    valid_dates = df["InvoiceDate"].dropna().drop_duplicates()

    dim = pd.DataFrame()
    dim["tiempo_id"] = valid_dates
    dim["anio"] = valid_dates.dt.year.astype(int)
    dim["mes"] = valid_dates.dt.month.astype(int)
    dim["dia"] = valid_dates.dt.day.astype(int)
    dim["dia_semana"] = valid_dates.dt.day_name()

    dim.drop_duplicates(subset=["tiempo_id"], inplace=True)
    return dim[["tiempo_id", "anio", "mes", "dia", "dia_semana"]].reset_index(drop=True)


def _build_dim_cliente(df: pd.DataFrame) -> pd.DataFrame:
    """
    Construye la dimensión cliente identificando clientes registrados y 'CLIENTE_ANONIMO'.
    """
    logger.info("Construyendo dimensión cliente (dim_cliente)...")
    dim = (
        df[["CustomerID", "Country", "es_anonimo"]]
        .drop_duplicates(subset=["CustomerID"])
        .rename(columns={
            "CustomerID": "cliente_id",
            "Country": "pais",
        })
        .reset_index(drop=True)
    )
    return dim


def _build_dim_producto(df: pd.DataFrame) -> pd.DataFrame:
    """
    Construye la dimensión producto con la descripción canónica unificada y categoría.
    """
    logger.info("Construyendo dimensión producto (dim_producto)...")
    dim = (
        df[["StockCode", "descripcion_canonica", "Category"]]
        .drop_duplicates(subset=["StockCode"])
        .rename(columns={
            "StockCode": "stock_code",
            "Category": "categoria",
        })
        .reset_index(drop=True)
    )
    return dim


def _load_dimension(df: pd.DataFrame, table_name: str, engine, pk_col: str) -> None:
    """
    Carga idempotente en tablas dimensionales aplicando ON CONFLICT (pk_col) DO NOTHING.
    """
    if df.empty:
        return

    logger.info(f"Cargando {len(df):,} registros en '{table_name}'...")
    cols = ", ".join(df.columns)
    placeholders = ", ".join([f":{c}" for c in df.columns])
    sql = text(
        f"INSERT INTO {table_name} ({cols}) VALUES ({placeholders}) "
        f"ON CONFLICT ({pk_col}) DO NOTHING"
    )

    with engine.begin() as conn:
        records = df.to_dict(orient="records")
        conn.execute(sql, records)
    logger.info(f"  → Inserción exitosa en '{table_name}'.")


def build_dimensional_model(
    df: pd.DataFrame, df_rejections: Optional[pd.DataFrame] = None
) -> None:
    """
    Puebla el DataMart analítico ('datamart_olap'):
      1. dim_tiempo (grano timestamp)
      2. dim_cliente (clientes identificados y anónimos)
      3. dim_producto (catálogo con descripción canónica)
      4. fact_ventas (ventas brutas, devoluciones y métricas netas)
      5. tabla_rechazos (auditoría forense de transacciones no conformes)
    """
    logger.info(f"Conectando a base de datos analítica DataMart: {OLAP_CONN_URI}")
    engine = create_engine(OLAP_CONN_URI, pool_pre_ping=True)

    # 1. Dimensión Tiempo
    dim_tiempo = _build_dim_tiempo(df)
    _load_dimension(dim_tiempo, "dim_tiempo", engine, pk_col="tiempo_id")

    # 2. Dimensión Cliente
    dim_cliente = _build_dim_cliente(df)
    _load_dimension(dim_cliente, "dim_cliente", engine, pk_col="cliente_id")

    # 3. Dimensión Producto
    dim_producto = _build_dim_producto(df)
    _load_dimension(dim_producto, "dim_producto", engine, pk_col="stock_code")

    # Limpieza idempotente previa de tablas de hechos y auditoría de la corrida actual
    with engine.begin() as conn:
        conn.execute(text("TRUNCATE TABLE fact_ventas, tabla_rechazos;"))

    # 4. Tabla de Hechos: fact_ventas
    logger.info("Preparando tabla de hechos 'fact_ventas'...")
    fact_table = df[[
        "InvoiceNo",
        "StockCode",
        "CustomerID",
        "InvoiceDate",
        "tipo_transaccion",
        "Quantity",
        "UnitPrice",
        "revenue_bruto",
        "revenue_neto",
    ]].rename(columns={
        "InvoiceNo": "invoice_no",
        "StockCode": "stock_code",
        "CustomerID": "cliente_id",
        "InvoiceDate": "tiempo_id",
        "Quantity": "cantidad",
        "UnitPrice": "precio_unitario",
    })

    logger.info(f"Insertando {len(fact_table):,} hechos en 'fact_ventas'...")
    fact_table.to_sql(
        name="fact_ventas",
        con=engine,
        if_exists="append",
        index=False,
        method="multi",
        chunksize=5000,
    )
    logger.info("  → Hechos cargados en 'fact_ventas'.")

    # 5. Tabla de Rechazos (si se proporcionan registros inválidos)
    if df_rejections is not None and not df_rejections.empty:
        logger.info(f"Cargando {len(df_rejections):,} registros en 'tabla_rechazos'...")
        df_rejections.to_sql(
            name="tabla_rechazos",
            con=engine,
            if_exists="append",
            index=False,
            method="multi",
            chunksize=5000,
        )
        logger.info("  → Auditoría de rechazos persistida.")

    engine.dispose()
    logger.info("✅ DataMart analítico OLAP actualizado exitosamente.")
