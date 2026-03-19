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

### 1. Clonar y configurar
```bash
git clone https://github.com/tu-usuario/castor-etl.git
cd castor-etl
cp .env .env
# Editar .env con las credenciales reales
```

### 2. Levantar con Docker Compose
```bash
docker-compose up -d
```
Esto levanta MSSQL (fuente), PostgreSQL (destino) y el pipeline ETL.

### 3. Ejecutar localmente (desarrollo)
```bash
python -m venv venv
venv\Scripts\Activate
pip install -r requirements.txt -r requirements-dev.txt
python -m src.pipeline
```

### 4. Ejecutar tests
```bash
pytest tests/ -v --cov=src --cov-report=term-missing
```

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
