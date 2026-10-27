FROM python:3.10-slim

WORKDIR /app

# Instalar dependencias esenciales para FastAPI, PostgreSQL y Rate Limiting
RUN pip install --no-cache-dir fastapi uvicorn sqlalchemy psycopg2-binary slowapi

# Copiar script de la aplicación
COPY main.py /app/

EXPOSE 8000

CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]

