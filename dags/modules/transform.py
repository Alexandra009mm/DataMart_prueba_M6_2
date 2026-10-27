"""
transform.py
------------
Módulo de transformación y calidad de datos para el Caso: DataMart S.A.S.

Implementa las reglas de negocio canónicas:
  1. Normalización de StockCode (mayúsculas, sin espacios) y Description (espacios estandarizados).
  2. Conversión de fechas a UTC sin zona horaria para almacenamiento homogéneo.
  3. Tratamiento de devoluciones (Quantity < 0 -> 'DEVOLUCION', Quantity > 0 -> 'VENTA').
  4. Segregación de rechazos (UnitPrice <= 0 o Quantity == 0) para auditoría en tabla_rechazos.
  5. Imputación de clientes anónimos a 'CLIENTE_ANONIMO' con indicador booleano es_anonimo = True.
  6. Deduplicación por clave compuesta ['InvoiceNo', 'StockCode', 'InvoiceDate'] priorizando el dataset extendido.
  7. Clasificación por reglas de palabras clave (category_mapping.csv) con fallback 'Otros'.
  8. Cálculo explícito de métricas revenue_bruto y revenue_neto.
"""

import json
import logging
from pathlib import Path
from typing import Dict, Tuple

import pandas as pd

try:
    from modules.config import (
        CATEGORY_MAPPING_PATH,
        DATA_CSV_PATH,
        ONLINE_RETAIL_II_PATH,
    )
except ImportError:
    from dags.modules.config import (
        CATEGORY_MAPPING_PATH,
        DATA_CSV_PATH,
        ONLINE_RETAIL_II_PATH,
    )

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

# Mapeo de homologación de nombres de columnas entre fuentes heterogéneas
COLUMN_RENAME_MAP: Dict[str, str] = {
    "Invoice": "InvoiceNo",
    "Price": "UnitPrice",
    "Customer ID": "CustomerID",
    "TransactionNo": "InvoiceNo",
    "Date": "InvoiceDate",
    "ProductNo": "StockCode",
    "ProductName": "Description",
    "CustomerNo": "CustomerID",
}


def _read_source_file(filepath: Path, source_label: str) -> pd.DataFrame:
    """
    Lee un archivo CSV aplicando codificación segura y normalizando cabeceras.
    """
    logger.info(f"Leyendo fuente: {filepath.name} ({source_label})")
    try:
        df = pd.read_csv(filepath, encoding="utf-8", low_memory=False)
    except UnicodeDecodeError:
        logger.info(f"  Fallback a codificación ISO-8859-1 para {filepath.name}")
        df = pd.read_csv(filepath, encoding="ISO-8859-1", low_memory=False)

    # Renombrar columnas encontradas
    rename_cols = {k: v for k, v in COLUMN_RENAME_MAP.items() if k in df.columns}
    if rename_cols:
        df.rename(columns=rename_cols, inplace=True)

    # Eliminar columna auxiliar de índice si existe
    if "index" in df.columns:
        df.drop(columns=["index"], inplace=True)

    df["_source_label"] = source_label
    logger.info(f"  → Registros leídos de {filepath.name}: {len(df):,}")
    return df


def _assign_categories(descriptions: pd.Series, mapping_df: pd.DataFrame) -> pd.Series:
    """
    Asigna categorías de forma vectorizada basada en keywords sobre la descripción.
    """
    categories = pd.Series("Otros", index=descriptions.index)
    desc_clean = descriptions.fillna("").astype(str).str.lower()

    for _, row in mapping_df.iterrows():
        keyword = str(row["keyword"]).strip().lower()
        cat_name = str(row["category"]).strip()
        if not keyword:
            continue
        mask = (categories == "Otros") & (desc_clean.str.contains(keyword, case=False, regex=False))
        categories[mask] = cat_name

    return categories


def load_and_clean_data() -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Ejecuta el pipeline integral de extracción, limpieza, deduplicación y auditoría.

    Retorna
    -------
    Tuple[pd.DataFrame, pd.DataFrame]
        df_clean : DataFrame depurado con ventas y devoluciones válidas.
        df_rejections : DataFrame con registros rechazados y su motivo para auditoría.
    """
    logger.info("=== Iniciando Transformación Integral: DataMart S.A.S. ===")

    # 1. Lectura de las dos fuentes transaccionales oficiales
    df1 = _read_source_file(DATA_CSV_PATH, "data.csv")
    df2 = _read_source_file(ONLINE_RETAIL_II_PATH, "online_retail_II.csv")

    # Prioridad: archivo II (historial extendido) prevalece estrictamente ante duplicados
    df1["_source_priority"] = 1
    df2["_source_priority"] = 2

    df = pd.concat([df1, df2], ignore_index=True)
    logger.info(f"Total registros combinados iniciales: {len(df):,}")

    # 2. Normalización de formatos y tipos base
    logger.info("Normalizando tipos, fechas en UTC y códigos de producto...")
    # Fechas convertidas a UTC sin información de zona horaria para compatibilidad SQL
    df["InvoiceDate"] = pd.to_datetime(df["InvoiceDate"], errors="coerce", utc=True).dt.tz_localize(None)

    # Conversión numérica con coerción ante caracteres erróneos
    df["Quantity"] = pd.to_numeric(df["Quantity"], errors="coerce").fillna(0).astype(int)
    df["UnitPrice"] = pd.to_numeric(df["UnitPrice"], errors="coerce").fillna(0.0).round(2)

    # StockCode: En mayúsculas y sin espacios laterales
    df["StockCode"] = df["StockCode"].astype(str).str.strip().str.upper()

    # Description: Normalizar espacios múltiples y estandarizar
    df["Description"] = (
        df["Description"]
        .fillna("SIN DESCRIPCION")
        .astype(str)
        .str.replace(r"\s+", " ", regex=True)
        .str.strip()
        .str.upper()
    )
    df.loc[df["Description"] == "", "Description"] = "SIN DESCRIPCION"

    # Clientes: Manejo de nulos como 'CLIENTE_ANONIMO'
    raw_customer = df["CustomerID"].astype(str).str.replace(r"\.0$", "", regex=True).str.strip()
    is_anon = (
        df["CustomerID"].isna()
        | raw_customer.isin(["nan", "None", "", "SIN_ID", "NULL"])
    )
    df["CustomerID"] = raw_customer
    df.loc[is_anon, "CustomerID"] = "CLIENTE_ANONIMO"
    df["es_anonimo"] = is_anon

    # Country
    df["Country"] = df["Country"].fillna("Desconocido").astype(str).str.strip()

    # 3. Identificación y Segregación de Rechazos (Calidad de Datos)
    # Regla: UnitPrice <= 0 o Quantity == 0 o Fecha nula son rechazados
    cond_fecha_invalida = df["InvoiceDate"].isna()
    cond_precio_invalido = df["UnitPrice"] <= 0
    cond_cantidad_cero = df["Quantity"] == 0

    cond_rechazo = cond_fecha_invalida | cond_precio_invalido | cond_cantidad_cero

    df_validos = df[~cond_rechazo].copy()
    df_invalidos = df[cond_rechazo].copy()

    logger.info(f"Registros pre-aprobados: {len(df_validos):,} | Registros rechazados: {len(df_invalidos):,}")

    # Asignar motivo detallado a los rechazos para la tabla_rechazos
    motivos = []
    for idx, row in df_invalidos.iterrows():
        reasons = []
        if pd.isna(row["InvoiceDate"]):
            reasons.append("FECHA_INVALIDA")
        if row["UnitPrice"] <= 0:
            reasons.append(f"PRECIO_INVALIDO ({row['UnitPrice']})")
        if row["Quantity"] == 0:
            reasons.append("CANTIDAD_CERO")
        motivos.append("; ".join(reasons) if reasons else "REGISTRO_CORRUPTO")

    df_invalidos["motivo"] = motivos
    
    # Construir payload JSON serializado para auditoría forense
    df_invalidos["payload_crudo"] = df_invalidos[[
        "InvoiceNo", "StockCode", "Description", "Quantity", "InvoiceDate", "UnitPrice", "CustomerID", "Country"
    ]].apply(lambda r: json.dumps(r.to_dict(), default=str), axis=1)

    df_rejections = df_invalidos[[
        "_source_label", "motivo", "payload_crudo"
    ]].rename(columns={"_source_label": "fuente_origen"}).copy()
    df_rejections["fecha_rechazo"] = pd.Timestamp.utcnow().tz_localize(None)

    # 4. Deduplicación estricta por clave compuesta sobre válidos
    logger.info("Aplicando deduplicación por ['InvoiceNo', 'StockCode', 'InvoiceDate'] con prioridad al archivo II...")
    dedup_cols = ["InvoiceNo", "StockCode", "InvoiceDate"]
    df_validos.sort_values(by=["_source_priority"], inplace=True)
    df_validos.drop_duplicates(subset=dedup_cols, keep="last", inplace=True)
    df_validos.drop(columns=["_source_priority", "_source_label"], inplace=True)
    logger.info(f"Registros tras deduplicación: {len(df_validos):,}")

    # 5. Clasificación de tipo de transacción y cálculo de revenue
    # Quantity > 0 -> 'VENTA', Quantity < 0 -> 'DEVOLUCION'
    df_validos["tipo_transaccion"] = "VENTA"
    df_validos.loc[df_validos["Quantity"] < 0, "tipo_transaccion"] = "DEVOLUCION"

    # revenue_bruto: positivo en ventas, negativo en devoluciones
    df_validos["revenue_bruto"] = (df_validos["Quantity"] * df_validos["UnitPrice"]).round(2)
    # revenue_neto: refleja el impacto neto en la facturación
    df_validos["revenue_neto"] = df_validos["revenue_bruto"]

    # 6. Categorización mediante reglas de catálogo
    logger.info(f"Cargando reglas de categorías desde: {CATEGORY_MAPPING_PATH}")
    mapping_df = pd.read_csv(CATEGORY_MAPPING_PATH)
    df_validos["Category"] = _assign_categories(df_validos["Description"], mapping_df)

    # Generar descripción canónica unívoca por stock_code
    canonical_desc = (
        df_validos.groupby("StockCode")["Description"]
        .agg(lambda s: s.mode().iloc[0] if not s.mode().empty else s.iloc[0])
        .to_dict()
    )
    df_validos["descripcion_canonica"] = df_validos["StockCode"].map(canonical_desc).fillna(df_validos["Description"])

    resumen_ventas = df_validos["tipo_transaccion"].value_counts().to_dict()
    logger.info(f"Composición de transacciones válidas: {resumen_ventas}")
    logger.info(f"Revenue neto total calculado: {df_validos['revenue_neto'].sum():,.2f}")

    return df_validos.reset_index(drop=True), df_rejections.reset_index(drop=True)
