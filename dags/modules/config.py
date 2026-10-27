"""
config.py
---------
Configuración centralizada para el pipeline ETL de DataMart S.A.S.
Define las rutas canónicas del lago de datos local y las cadenas de conexión
SQLAlchemy hacia PostgreSQL para las bases de datos 'datamart_oltp' y 'datamart_olap'.
"""

import os
from pathlib import Path

# ---------------------------------------------------------------------------
# Rutas base del proyecto
# ---------------------------------------------------------------------------

# Directorio raíz del proyecto (2 niveles arriba de dags/modules/)
BASE_DIR = Path(__file__).resolve().parents[2]

# Directorio de datos (volumen compartido /data)
DATA_DIR = Path(os.getenv("DATA_DIR", BASE_DIR / "data"))

# Fuentes oficiales del Caso DataMart S.A.S. (2 transaccionales + 1 mapeo)
# 1. data/data.csv (Transacciones diarias operacionales)
# 2. data/online_retail_II.csv (Historial transaccional extendido)
# 3. data/category_mapping.csv (Mapeo de categorías)
DATA_CSV_PATH = DATA_DIR / "data.csv"
ONLINE_RETAIL_II_PATH = DATA_DIR / "online_retail_II.csv"
if not ONLINE_RETAIL_II_PATH.exists() and (DATA_DIR / "online_retail.csv").exists():
    ONLINE_RETAIL_II_PATH = DATA_DIR / "online_retail.csv"
CATEGORY_MAPPING_PATH = DATA_DIR / "category_mapping.csv"

# Alias para compatibilidad
FILE_PART_1 = DATA_CSV_PATH
FILE_PART_2 = ONLINE_RETAIL_II_PATH
FILE_MAPPING = CATEGORY_MAPPING_PATH

# Archivos de staging intermedio en formato columnar (Parquet)
STAGING_CLEAN_PARQUET = DATA_DIR / "staging_clean_retail.parquet"
STAGING_REJECTIONS_PARQUET = DATA_DIR / "staging_rejections_retail.parquet"

# URL de la API interna de productos (microservicio FastAPI)
PRODUCTS_API_URL = os.getenv("PRODUCTS_API_URL", "http://products-api:8000")

# ---------------------------------------------------------------------------
# Cadenas de conexión SQLAlchemy hacia PostgreSQL
# ---------------------------------------------------------------------------

_PG_USER = os.getenv("POSTGRES_USER", "airflow")
_PG_PASSWORD = os.getenv("POSTGRES_PASSWORD", "airflow")
_PG_HOST = os.getenv("POSTGRES_HOST", "postgres")
_PG_PORT = os.getenv("POSTGRES_PORT", "5432")

_DB_OLTP = os.getenv("DB_OLTP_NAME", "datamart_oltp")
_DB_OLAP = os.getenv("DB_OLAP_NAME", "datamart_olap")

# URI para la capa operacional (OLTP)
OLTP_CONN_URI = (
    f"postgresql+psycopg2://{_PG_USER}:{_PG_PASSWORD}"
    f"@{_PG_HOST}:{_PG_PORT}/{_DB_OLTP}"
)

# URI para la capa analítica (OLAP / DataMart)
OLAP_CONN_URI = (
    f"postgresql+psycopg2://{_PG_USER}:{_PG_PASSWORD}"
    f"@{_PG_HOST}:{_PG_PORT}/{_DB_OLAP}"
)
