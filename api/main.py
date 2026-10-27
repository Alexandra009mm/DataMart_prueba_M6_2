"""
main.py
-------
Microservicio REST interno de catálogo de productos para DataMart S.A.S.
Conexión directa a PostgreSQL (datamart_olap.dim_producto), paginación nativa SQL
y control de tasa de llamadas (Rate Limiting).
"""

import os
from math import ceil
from typing import List, Optional

from fastapi import Depends, FastAPI, HTTPException, Query, Request, status
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field
from slowapi import Limiter, _rate_limit_exceeded_handler
from slowapi.errors import RateLimitExceeded
from slowapi.util import get_remote_address
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import declarative_base

# ---------------------------------------------------------------------------
# Configuración de Entorno y Base de Datos (datamart_olap)
# ---------------------------------------------------------------------------
PG_USER = os.getenv("POSTGRES_USER", "airflow")
PG_PASSWORD = os.getenv("POSTGRES_PASSWORD", "airflow")
PG_HOST = os.getenv("POSTGRES_HOST", "postgres")
PG_PORT = os.getenv("POSTGRES_PORT", "5432")
DB_NAME = os.getenv("DB_OLAP_NAME", "datamart_olap")

DATABASE_URL = (
    f"postgresql+psycopg2://{PG_USER}:{PG_PASSWORD}@{PG_HOST}:{PG_PORT}/{DB_NAME}"
)

# Límite de llamadas por defecto (configurable por variable de entorno)
RATE_LIMIT = os.getenv("RATE_LIMIT_PER_MINUTE", "60") + "/minute"

# ---------------------------------------------------------------------------
# Inicialización de Limiter (Rate Limiting) y FastAPI
# ---------------------------------------------------------------------------
limiter = Limiter(key_func=get_remote_address)

app = FastAPI(
    title="DataMart S.A.S. - Products Catalog API (PostgreSQL)",
    description=(
        "API REST oficial conectada a la base de datos analítica (datamart_olap.dim_producto). "
        "Expone el catálogo maestro de 3,827 productos con paginación nativa SQL y protección por Rate Limiting."
    ),
    version="2.0.0",
)

app.state.limiter = limiter


@app.exception_handler(RateLimitExceeded)
def custom_rate_limit_exceeded_handler(request: Request, exc: RateLimitExceeded):
    """Manejador personalizado cuando un cliente excede la cuota de llamadas."""
    return JSONResponse(
        status_code=status.HTTP_429_TOO_MANY_REQUESTS,
        content={
            "error": "Too Many Requests",
            "detail": f"Límite de llamadas excedido ({RATE_LIMIT}). Por favor espere antes de enviar nuevas peticiones.",
            "client_ip": get_remote_address(request),
        },
        headers={"Retry-After": "60"},
    )


# Pool de conexiones a PostgreSQL
engine = create_engine(
    DATABASE_URL,
    pool_pre_ping=True,
    pool_size=5,
    max_overflow=10,
    connect_args={"connect_timeout": 5},
)

# ---------------------------------------------------------------------------
# Esquemas Pydantic
# ---------------------------------------------------------------------------
class Product(BaseModel):
    stock_code: str = Field(..., description="Código SKU alfanumérico único normalizado")
    nombre: str = Field(..., description="Descripción canónica comercial del producto")
    categoria: str = Field(..., description="Categoría asignada (Electrónica, Hogar, Ropa, Deportes, Papelería, Otros)")
    activo: bool = Field(True, description="Indicador comercial de disponibilidad")


class PaginatedProductsResponse(BaseModel):
    total: int = Field(..., description="Total de productos disponibles que coinciden con los filtros")
    skip: int = Field(..., description="Número de registros omitidos")
    limit: int = Field(..., description="Límite de registros solicitados por página")
    total_pages: int = Field(..., description="Total de páginas calculadas")
    current_page: int = Field(..., description="Página actual calculada (1-indexed)")
    items: List[Product] = Field(..., description="Lista de productos en la página actual")


# ---------------------------------------------------------------------------
# Endpoints de la API
# ---------------------------------------------------------------------------
@app.get("/health", tags=["Salud"])
def health_check():
    """
    Verifica el estado del servicio y la conectividad viva con la base de datos PostgreSQL.
    """
    try:
        with engine.connect() as conn:
            total_db = conn.execute(text("SELECT COUNT(*) FROM dim_producto")).scalar()
            return {
                "status": "healthy",
                "database": "connected",
                "database_name": DB_NAME,
                "total_productos_en_db": total_db,
                "rate_limit_policy": RATE_LIMIT,
            }
    except SQLAlchemyError as e:
        return JSONResponse(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            content={
                "status": "degraded",
                "database": "disconnected",
                "error": str(e),
            },
        )


@app.get("/products", response_model=PaginatedProductsResponse, tags=["Catálogo"])
@limiter.limit(RATE_LIMIT)
def list_products(
    request: Request,
    skip: int = Query(0, ge=0, description="Número de registros a omitir para paginación"),
    limit: int = Query(50, ge=1, le=200, description="Cantidad máxima de registros a retornar por página (máx: 200)"),
    categoria: Optional[str] = Query(None, description="Filtro opcional por categoría exacta (case-insensitive)"),
):
    """
    Lista productos directamente desde PostgreSQL (`datamart_olap.dim_producto`)
    con paginación eficiente en SQL y control de cuota de llamadas por cliente.
    """
    where_clauses = []
    params = {"skip": skip, "limit": limit}

    if categoria:
        where_clauses.append("LOWER(categoria) = LOWER(:categoria)")
        params["categoria"] = categoria.strip()

    where_sql = f"WHERE {' AND '.join(where_clauses)}" if where_clauses else ""

    count_query = text(f"SELECT COUNT(*) FROM dim_producto {where_sql}")
    data_query = text(f"""
        SELECT 
            stock_code,
            descripcion_canonica,
            categoria
        FROM dim_producto
        {where_sql}
        ORDER BY stock_code ASC
        LIMIT :limit OFFSET :skip
    """)

    try:
        with engine.connect() as conn:
            total = conn.execute(count_query, params).scalar() or 0
            rows = conn.execute(data_query, params).mappings().fetchall()

            items = [
                Product(
                    stock_code=r["stock_code"],
                    nombre=r["descripcion_canonica"],
                    categoria=r["categoria"],
                    activo=True,
                )
                for r in rows
            ]

            total_pages = ceil(total / limit) if total > 0 else 1
            current_page = (skip // limit) + 1

            return PaginatedProductsResponse(
                total=total,
                skip=skip,
                limit=limit,
                total_pages=total_pages,
                current_page=current_page,
                items=items,
            )
    except SQLAlchemyError as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error consultando la base de datos PostgreSQL: {str(e)}",
        )


@app.get("/products/{stock_code}", response_model=Product, tags=["Catálogo"])
@limiter.limit(RATE_LIMIT)
def get_product_by_code(request: Request, stock_code: str):
    """
    Busca un producto individual en PostgreSQL a partir de su StockCode normalizado.
    """
    normalized_code = stock_code.strip().upper()
    query = text("""
        SELECT 
            stock_code,
            descripcion_canonica,
            categoria
        FROM dim_producto
        WHERE stock_code = :code
        LIMIT 1
    """)

    try:
        with engine.connect() as conn:
            row = conn.execute(query, {"code": normalized_code}).mappings().fetchone()

            if not row:
                raise HTTPException(
                    status_code=status.HTTP_404_NOT_FOUND,
                    detail=f"Producto con stock_code '{stock_code}' no existe en la dimensión dim_producto.",
                )

            return Product(
                stock_code=row["stock_code"],
                nombre=row["descripcion_canonica"],
                categoria=row["categoria"],
                activo=True,
            )
    except SQLAlchemyError as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error consultando la base de datos PostgreSQL: {str(e)}",
        )
