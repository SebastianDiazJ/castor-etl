"""
Gestor de conexiones a bases de datos con Connection Pooling.
Implementa el patrón Singleton para reutilizar engines/pools.
"""

import logging
from typing import Dict, Optional

from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
from sqlalchemy.orm import sessionmaker, Session

from config.settings import DatabaseConfig

logger = logging.getLogger(__name__)


class ConnectionManager:
    """
    Administra Connection Pools para múltiples bases de datos.
    Usa SQLAlchemy Engine que internamente gestiona el pool de conexiones,
    evitando abrir/cerrar conexiones por cada operación.
    """

    _engines: Dict[str, Engine] = {}

    @classmethod
    def get_engine(cls, config: DatabaseConfig, alias: str = "default") -> Engine:
        """
        Obtiene o crea un Engine con pool de conexiones configurado.

        Args:
            config: Configuración de la base de datos.
            alias: Identificador único para esta conexión.

        Returns:
            SQLAlchemy Engine con connection pool activo.
        """
        if alias not in cls._engines:
            engine = create_engine(
                config.connection_url,
                pool_size=config.pool_size,
                max_overflow=config.max_overflow,
                pool_timeout=config.pool_timeout,
                pool_recycle=config.pool_recycle,
                pool_pre_ping=True,  # Verifica conexión antes de usarla
                echo=False,
            )
            cls._engines[alias] = engine
            logger.info(
                "Engine creado para '%s' con pool_size=%d, max_overflow=%d",
                alias,
                config.pool_size,
                config.max_overflow,
            )

        return cls._engines[alias]

    @classmethod
    def get_session(cls, config: DatabaseConfig, alias: str = "default") -> Session:
        """Crea una sesión a partir del engine pooled."""
        engine = cls.get_engine(config, alias)
        session_factory = sessionmaker(bind=engine)
        return session_factory()

    @classmethod
    def dispose(cls, alias: Optional[str] = None) -> None:
        """Libera conexiones del pool."""
        if alias:
            if alias in cls._engines:
                cls._engines[alias].dispose()
                del cls._engines[alias]
                logger.info("Engine '%s' eliminado.", alias)
        else:
            for name, engine in cls._engines.items():
                engine.dispose()
                logger.info("Engine '%s' eliminado.", name)
            cls._engines.clear()

    @classmethod
    def health_check(cls, config: DatabaseConfig, alias: str = "default") -> bool:
        """Verifica conectividad con la base de datos."""
        try:
            engine = cls.get_engine(config, alias)
            with engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            logger.info("Health check OK para '%s'.", alias)
            return True
        except Exception as exc:
            logger.error("Health check FALLIDO para '%s': %s", alias, exc)
            return False
