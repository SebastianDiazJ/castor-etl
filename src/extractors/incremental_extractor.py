"""
Extractor Incremental: Lee solo registros nuevos/modificados desde la fuente
usando una columna de timestamp o rowversion como marca de agua (watermark).

Principios SOLID aplicados:
- Single Responsibility: Solo se encarga de la extracción.
- Open/Closed: Extensible vía herencia para distintas fuentes.
- Dependency Inversion: Depende de abstracciones (Engine), no de implementaciones concretas.
"""

import logging
from datetime import datetime
from typing import Any, Dict, Generator, List, Optional

from sqlalchemy import text
from sqlalchemy.engine import Engine

from config.settings import DatabaseConfig, ETLConfig
from src.utils.connections import ConnectionManager
from src.utils.retry import with_retry

logger = logging.getLogger(__name__)


class IncrementalExtractor:
    """
    Extrae registros incrementalmente desde una base de datos fuente.
    Usa una columna de watermark (timestamp/rowversion) para determinar
    qué registros son nuevos desde la última ejecución.
    """

    def __init__(
        self,
        source_config: DatabaseConfig,
        etl_config: ETLConfig,
        table_name: str,
        watermark_column: str = "updated_at",
    ):
        self.source_config = source_config
        self.etl_config = etl_config
        self.table_name = table_name
        self.watermark_column = watermark_column
        self._engine: Optional[Engine] = None

    @property
    def engine(self) -> Engine:
        """Lazy initialization del engine con connection pool."""
        if self._engine is None:
            self._engine = ConnectionManager.get_engine(
                self.source_config, alias="source"
            )
        return self._engine

    @with_retry(max_attempts=5, base_delay=1.0, max_delay=60.0)
    def get_max_watermark(self) -> Optional[datetime]:
        """Obtiene el valor máximo actual del watermark en la fuente."""
        query = text(f"SELECT MAX({self.watermark_column}) FROM {self.table_name}")
        with self.engine.connect() as conn:
            result = conn.execute(query).scalar()
        logger.info(
            "Watermark máximo en '%s': %s", self.table_name, result
        )
        return result

    @with_retry(max_attempts=5, base_delay=1.0, max_delay=60.0)
    def _fetch_chunk(
        self, last_watermark: Optional[datetime], offset: int, limit: int
    ) -> List[Dict[str, Any]]:
        """
        Obtiene un chunk de registros desde la fuente.
        Usa OFFSET/LIMIT para paginar y no cargar todo en memoria.
        """
        if last_watermark:
            query = text(
                f"SELECT * FROM {self.table_name} "
                f"WHERE {self.watermark_column} > :watermark "
                f"ORDER BY {self.watermark_column} ASC "
                f"OFFSET :offset ROWS FETCH NEXT :limit ROWS ONLY"
            )
            params = {
                "watermark": last_watermark,
                "offset": offset,
                "limit": limit,
            }
        else:
            # Primera ejecución: extraer todo
            query = text(
                f"SELECT * FROM {self.table_name} "
                f"ORDER BY {self.watermark_column} ASC "
                f"OFFSET :offset ROWS FETCH NEXT :limit ROWS ONLY"
            )
            params = {"offset": offset, "limit": limit}

        with self.engine.connect() as conn:
            result = conn.execute(query, params)
            columns = result.keys()
            rows = [dict(zip(columns, row)) for row in result.fetchall()]

        logger.debug(
            "Chunk extraído: offset=%d, registros=%d", offset, len(rows)
        )
        return rows

    def extract(
        self, last_watermark: Optional[datetime] = None
    ) -> Generator[List[Dict[str, Any]], None, None]:
        """
        Generador que extrae registros en chunks del tamaño configurado.
        Yield de cada chunk permite procesamiento en streaming sin
        cargar todo el dataset en memoria.

        Args:
            last_watermark: Último valor procesado. None = carga completa.

        Yields:
            Lista de diccionarios con los registros del chunk.
        """
        chunk_size = self.etl_config.chunk_size
        offset = 0
        total_extracted = 0

        logger.info(
            "Iniciando extracción incremental de '%s' "
            "(watermark_column='%s', last_watermark=%s, chunk_size=%d)",
            self.table_name,
            self.watermark_column,
            last_watermark,
            chunk_size,
        )

        while True:
            chunk = self._fetch_chunk(last_watermark, offset, chunk_size)

            if not chunk:
                logger.info(
                    "Extracción completada: %d registros en total de '%s'.",
                    total_extracted,
                    self.table_name,
                )
                break

            total_extracted += len(chunk)
            offset += chunk_size

            logger.info(
                "Progreso extracción '%s': %d registros extraídos...",
                self.table_name,
                total_extracted,
            )

            yield chunk

    @with_retry(max_attempts=5, base_delay=1.0, max_delay=60.0)
    def get_record_count(
        self, last_watermark: Optional[datetime] = None
    ) -> int:
        """Obtiene el conteo de registros a extraer (para progreso)."""
        if last_watermark:
            query = text(
                f"SELECT COUNT(*) FROM {self.table_name} "
                f"WHERE {self.watermark_column} > :watermark"
            )
            params = {"watermark": last_watermark}
        else:
            query = text(f"SELECT COUNT(*) FROM {self.table_name}")
            params = {}

        with self.engine.connect() as conn:
            count = conn.execute(query, params).scalar()

        logger.info("Registros a extraer de '%s': %d", self.table_name, count)
        return count or 0
