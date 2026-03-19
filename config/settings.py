"""
Configuración centralizada del pipeline ETL.
Todas las credenciales se leen de variables de entorno (nunca hardcoded).
"""

import os
from dataclasses import dataclass, field


@dataclass(frozen=True)
class DatabaseConfig:
    """Configuración de conexión a base de datos."""
    host: str
    port: int
    database: str
    username: str
    password: str
    driver: str = ""
    odbc_driver: str = ""  # Ej: "ODBC Driver 18 for SQL Server"
    pool_size: int = 5
    max_overflow: int = 10
    pool_timeout: int = 30
    pool_recycle: int = 1800

    @property
    def connection_url(self) -> str:
        from urllib.parse import quote_plus

        if self.driver and "mssql" in self.driver:
            # MSSQL requiere el driver ODBC en la query string
            odbc = self.odbc_driver or "ODBC Driver 18 for SQL Server"
            conn_str = (
                f"DRIVER={{{odbc}}};"
                f"SERVER={self.host},{self.port};"
                f"DATABASE={self.database};"
                f"UID={self.username};"
                f"PWD={self.password};"
                f"TrustServerCertificate=yes;"
            )
            return f"{self.driver}:///?odbc_connect={quote_plus(conn_str)}"
        if self.driver:
            # Oracle u otro driver genérico
            return (
                f"{self.driver}://{quote_plus(self.username)}:{quote_plus(self.password)}"
                f"@{self.host}:{self.port}/{self.database}?client_encoding=utf8"
            )
        return (
            f"postgresql://{quote_plus(self.username)}:{quote_plus(self.password)}"
            f"@{self.host}:{self.port}/{self.database}?client_encoding=utf8"
        )


@dataclass(frozen=True)
class ETLConfig:
    """Configuración global del pipeline ETL."""
    chunk_size: int = 5000
    max_workers: int = 4
    retry_max_attempts: int = 5
    retry_base_delay: float = 1.0
    retry_max_delay: float = 60.0
    retry_exponential_base: float = 2.0
    log_level: str = "INFO"


def get_source_db_config() -> DatabaseConfig:
    """Obtiene la configuración de la base de datos fuente (MSSQL/Oracle)."""
    return DatabaseConfig(
        host=os.environ.get("SOURCE_DB_HOST", "localhost"),
        port=int(os.environ.get("SOURCE_DB_PORT", "1433")),
        database=os.environ.get("SOURCE_DB_NAME", "legacy_financial"),
        username=os.environ.get("SOURCE_DB_USER", "sa"),
        password=os.environ.get("SOURCE_DB_PASSWORD", ""),
        driver=os.environ.get("SOURCE_DB_DRIVER", "mssql+pyodbc"),
        odbc_driver=os.environ.get("SOURCE_DB_ODBC_DRIVER", "ODBC Driver 18 for SQL Server"),
        pool_size=int(os.environ.get("SOURCE_DB_POOL_SIZE", "5")),
        max_overflow=int(os.environ.get("SOURCE_DB_MAX_OVERFLOW", "10")),
    )


def get_target_db_config() -> DatabaseConfig:
    """Obtiene la configuración de la base de datos destino (Postgres)."""
    return DatabaseConfig(
        host=os.environ.get("TARGET_DB_HOST", "localhost"),
        port=int(os.environ.get("TARGET_DB_PORT", "5432")),
        database=os.environ.get("TARGET_DB_NAME", "analytics"),
        username=os.environ.get("TARGET_DB_USER", "postgres"),
        password=os.environ.get("TARGET_DB_PASSWORD", ""),
        pool_size=int(os.environ.get("TARGET_DB_POOL_SIZE", "5")),
        max_overflow=int(os.environ.get("TARGET_DB_MAX_OVERFLOW", "10")),
    )


def get_etl_config() -> ETLConfig:
    """Obtiene la configuración del pipeline ETL."""
    return ETLConfig(
        chunk_size=int(os.environ.get("ETL_CHUNK_SIZE", "5000")),
        max_workers=int(os.environ.get("ETL_MAX_WORKERS", "4")),
        retry_max_attempts=int(os.environ.get("ETL_RETRY_MAX_ATTEMPTS", "5")),
        retry_base_delay=float(os.environ.get("ETL_RETRY_BASE_DELAY", "1.0")),
        retry_max_delay=float(os.environ.get("ETL_RETRY_MAX_DELAY", "60.0")),
        log_level=os.environ.get("ETL_LOG_LEVEL", "INFO"),
    )