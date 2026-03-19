FROM python:3.11-slim

LABEL maintainer="Castor ETL Pipeline"
LABEL description="Pipeline ETL para extracción financiera desde MSSQL/Oracle a Postgres"

# Dependencias del sistema para drivers de BD
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    libpq-dev \
    unixodbc-dev \
    curl \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Instalar dependencias Python primero (caché de Docker)
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copiar código fuente
COPY . .

# Usuario no-root por seguridad
RUN useradd --create-home appuser
USER appuser

# Health check
HEALTHCHECK --interval=30s --timeout=10s --retries=3 \
    CMD python -c "print('ok')" || exit 1

CMD ["python", "-m", "src.pipeline"]
