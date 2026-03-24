# Castor ETL Pipeline

Pipeline ETL robusto para extracción incremental de información financiera desde sistemas legados (MSSQL/Oracle) hacia PostgreSQL para consumo de Power BI.

## Arquitectura

```
┌─────────────────┐    ┌──────────────┐    ┌───────────────┐    ┌──────────────┐
│   MSSQL/Oracle  │───▶│  Extractor   │───▶│  Validator &  │───▶│   Loader     │───▶ PostgreSQL
│  (Sistema       │    │ (Incremental │    │  Transformer  │    │ (Batch +     │    (Power BI)
│   Legado)       │    │  + Chunking) │    │  (Pydantic)   │    │  Paralelo)   │
└─────────────────┘    └──────────────┘    └───────────────┘    └──────────────┘
```

## Decisiones de Diseño

### Principios SOLID
- **Single Responsibility**: Cada módulo tiene una única responsabilidad (extracción, transformación, validación, carga).
- **Open/Closed**: Los extractores y transformadores son extensibles vía composición sin modificar el código existente.
- **Dependency Inversion**: Los componentes dependen de abstracciones (Engine, Config dataclasses), no de implementaciones concretas.

### Patrones Implementados
- **Connection Pool** (SQLAlchemy): Reutilización de conexiones para evitar overhead de apertura/cierre por operación.
- **Exponential Backoff con Jitter**: Reintentos inteligentes ante fallos transitorios de red.
- **Chunked Processing**: Extracción y carga por lotes de 5,000 registros para optimizar IOPS.
- **Upsert (Idempotencia)**: `INSERT ON CONFLICT UPDATE` garantiza que reintentos no dupliquen datos.
- **Facade**: `ETLPipeline` orquesta todo el flujo como punto de entrada simplificado.

### Tecnologías
- **Python 3.11** + **SQLAlchemy 2.x** para ORM y connection pooling.
- **Pydantic v2** para validación de esquemas con mensajes de error detallados.
- **pytest** con mocks para tests unitarios e integración.
- **Docker Compose** para levantar el entorno completo.

## Estructura del Proyecto

```
castor-etl/
├── config/
│   ├── __init__.py
│   └── settings.py          # Configuración centralizada (env vars)
├── src/
│   ├── extractors/
│   │   └── incremental_extractor.py  # Extracción incremental por watermark
│   ├── transformers/
│   │   └── data_transformer.py       # Transformación y mapeo de datos
│   ├── validators/
│   │   └── schema_validator.py       # Validación con Pydantic
│   ├── loaders/
│   │   └── batch_loader.py           # Carga por lotes con paralelismo
│   ├── utils/
│   │   ├── connections.py            # Connection pooling (Singleton)
│   │   ├── retry.py                  # Exponential Backoff
│   │   └── logging_config.py         # Logging estructurado
│   └── pipeline.py                   # Orquestador principal (Facade)
├── tests/
│   ├── conftest.py                   # Fixtures compartidos
│   ├── test_schema_validator.py      # Tests de validación
│   ├── test_retry.py                 # Tests de reintentos
│   ├── test_transformer.py           # Tests de transformación
│   ├── test_connections.py           # Tests de connection manager
│   └── test_integration.py           # Tests de integración (fallos simulados)
├── scripts/
│   └── init_postgres.sql             # DDL del esquema destino
├── docker-compose.yml                # Entorno completo containerizado
├── Dockerfile
├── azure-pipelines.yml               # CI/CD para Azure DevOps
├── requirements.txt
├── requirements-dev.txt
├── .env.example
├── .gitignore
└── README.md
```

## Inicio Rápido

### Prerrequisitos

- Python 3.11+
- Docker Desktop (con Docker Compose)
- ODBC Driver 18 for SQL Server ([descargar aquí](https://learn.microsoft.com/en-us/sql/connect/odbc/download-odbc-driver-for-sql-server))

### 1. Clonar y configurar

```bash
git clone https://github.com/SebastianDiazJ/castor-etl.git
cd castor-etl
```

Crear el archivo `.env` en la raíz del proyecto con este contenido:

```
SOURCE_DB_HOST=localhost
SOURCE_DB_PORT=1433
SOURCE_DB_NAME=legacy_financial
SOURCE_DB_USER=sa
SOURCE_DB_PASSWORD=Str0ngP@ssw0rd!
SOURCE_DB_DRIVER=mssql+pyodbc
SOURCE_DB_ODBC_DRIVER=ODBC Driver 18 for SQL Server
SOURCE_DB_POOL_SIZE=5
SOURCE_DB_MAX_OVERFLOW=10
TARGET_DB_HOST=localhost
TARGET_DB_PORT=5433
TARGET_DB_NAME=analytics
TARGET_DB_USER=postgres
TARGET_DB_PASSWORD=postgres_secret
TARGET_DB_POOL_SIZE=5
TARGET_DB_MAX_OVERFLOW=10
ETL_CHUNK_SIZE=5000
ETL_MAX_WORKERS=4
ETL_RETRY_MAX_ATTEMPTS=5
ETL_RETRY_BASE_DELAY=1.0
ETL_RETRY_MAX_DELAY=60.0
ETL_LOG_LEVEL=INFO
```

> **Nota:** El puerto de Postgres es `5433` (no `5432`) para evitar conflictos con instalaciones locales de PostgreSQL.

### 2. Levantar las bases de datos con Docker Compose

```bash
docker-compose up -d source-db target-db
```

Esperar ~30 segundos a que MSSQL esté listo. Verificar que esté healthy:

```bash
docker ps
```

### 3. Crear la base de datos y datos de ejemplo en MSSQL

Ejecutar estos comandos uno por uno:

```bash
docker exec castor-source-mssql /opt/mssql-tools18/bin/sqlcmd -S localhost -U sa -P "Str0ngP@ssw0rd!" -C -N -Q "IF NOT EXISTS (SELECT name FROM sys.databases WHERE name = 'legacy_financial') CREATE DATABASE legacy_financial;"
```

```bash
docker exec castor-source-mssql /opt/mssql-tools18/bin/sqlcmd -S localhost -U sa -P "Str0ngP@ssw0rd!" -C -N -d legacy_financial -Q "IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='financial_transactions' AND xtype='U') CREATE TABLE financial_transactions (transaction_id BIGINT PRIMARY KEY IDENTITY(1,1), account_id BIGINT NOT NULL, amount DECIMAL(18,2) NOT NULL, currency CHAR(3) NOT NULL DEFAULT 'USD', transaction_date DATETIME NOT NULL, description NVARCHAR(500) NULL, category NVARCHAR(100) NULL, status NVARCHAR(20) NOT NULL, created_at DATETIME NOT NULL DEFAULT GETDATE(), updated_at DATETIME NOT NULL DEFAULT GETDATE());"
```

```bash
docker exec castor-source-mssql /opt/mssql-tools18/bin/sqlcmd -S localhost -U sa -P "Str0ngP@ssw0rd!" -C -N -d legacy_financial -Q "DECLARE @i INT=1; WHILE @i<=50 BEGIN INSERT INTO financial_transactions(account_id,amount,currency,transaction_date,description,category,status,created_at,updated_at) VALUES(1000+(@i%10),CAST(RAND(CHECKSUM(NEWID()))*10000 AS DECIMAL(18,2)),CASE @i%3 WHEN 0 THEN 'USD' WHEN 1 THEN 'EUR' ELSE 'GBP' END,DATEADD(DAY,-@i,GETDATE()),CONCAT('Transaction #',@i),CASE @i%4 WHEN 0 THEN 'transfer' WHEN 1 THEN 'payment' WHEN 2 THEN 'deposit' ELSE 'withdrawal' END,CASE @i%3 WHEN 0 THEN 'completed' WHEN 1 THEN 'pending' ELSE 'completed' END,DATEADD(DAY,-@i,GETDATE()),DATEADD(DAY,-@i,GETDATE())); SET @i=@i+1; END;"
```

Verificar que hay datos:

```bash
docker exec castor-source-mssql /opt/mssql-tools18/bin/sqlcmd -S localhost -U sa -P "Str0ngP@ssw0rd!" -C -N -d legacy_financial -Q "SELECT COUNT(*) AS total FROM financial_transactions;"
```

Debe mostrar `50`.

### 4. Instalar dependencias Python

```bash
python -m venv venv
# Windows:
venv\Scripts\activate
# Linux/Mac:
source venv/bin/activate

pip install -r requirements.txt -r requirements-dev.txt
```

### 5. Cargar variables de entorno y ejecutar el pipeline

**En PowerShell (Windows):**

> **Importante:** Las variables de entorno deben cargarse con comillas simples para evitar problemas con caracteres especiales como `!` y `@`.

```powershell
$env:SOURCE_DB_HOST='localhost'
$env:SOURCE_DB_PORT='1433'
$env:SOURCE_DB_NAME='legacy_financial'
$env:SOURCE_DB_USER='sa'
$env:SOURCE_DB_PASSWORD='Str0ngP@ssw0rd!'
$env:SOURCE_DB_DRIVER='mssql+pyodbc'
$env:SOURCE_DB_ODBC_DRIVER='ODBC Driver 18 for SQL Server'
$env:TARGET_DB_HOST='localhost'
$env:TARGET_DB_PORT='5433'
$env:TARGET_DB_NAME='analytics'
$env:TARGET_DB_USER='postgres'
$env:TARGET_DB_PASSWORD='postgres_secret'
$env:ETL_CHUNK_SIZE='5000'
$env:ETL_LOG_LEVEL='INFO'

python -m src.pipeline
```

**En Bash (Linux/Mac):**

```bash
export $(grep -v '^#' .env | xargs)
python -m src.pipeline
```

### 6. Verificar los datos en Postgres

```bash
docker exec castor-target-postgres psql -U postgres -d analytics -c "SELECT COUNT(*) AS total FROM financial_transactions;"
```

Debe mostrar `50` — los mismos registros que estaban en MSSQL.

### 7. Ejecutar tests

```bash
pytest tests/ -v --cov=src --cov-report=term-missing
```

Los tests no requieren bases de datos reales (usan mocks).

## GitFlow

El repositorio sigue el modelo GitFlow:

| Rama | Propósito |
|------|-----------|
| `main` | Código en producción |
| `develop` | Rama de integración |
| `feature/*` | Nuevas funcionalidades |
| `release/*` | Preparación de releases |
| `hotfix/*` | Correcciones urgentes en producción |

## CI/CD (Azure Pipelines)

El archivo `azure-pipelines.yml` ejecuta automáticamente en cada Pull Request:
1. **Lint**: Verificación de estilo con flake8.
2. **Tests**: Ejecución completa de pytest con reporte de cobertura.
3. **Build**: Construcción de la imagen Docker (solo en PRs).