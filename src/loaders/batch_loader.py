"""
Loader: Carga datos validados y transformados a Postgres en lotes (batches).
Usa paralelismo controlado con ThreadPoolExecutor para aprovechar I/O
sin saturar el pool de conexiones.

Principio Single Responsibility: Solo se encarga de persistir datos.
"""

import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any, Dict, List, Optional

from sqlalchemy import text, MetaData, Table
from sqlalchemy.engine import Engine
from sqlalchemy.dialects.postgresql import insert as pg_insert

from config.settings import DatabaseConfig, ETLConfig
from src.utils.connections import ConnectionManager
from src.utils.retry import with_retry

logger = logging.getLogger(__name__)


class BatchLoader:
    """
    Carga datos a Postgres en lotes con paralelismo controlado.
    Soporta upsert (INSERT ON CONFLICT UPDATE) para idempotencia.
    """

    def __init__(
        self,
        target_config: DatabaseConfig,
        etl_config: ETLConfig,
        table_name: str,
        primary_key: str = "transaction_id",
    ):
        self.target_config = target_config
        self.etl_config = etl_config
        self.table_name = table_name
        self.primary_key = primary_key
        self._engine: Optional[Engine] = None
        self._loaded_count = 0
        self._failed_count = 0

    @property
    def engine(self) -> Engine:
        """Lazy initialization del engine con connection pool."""
        if self._engine is None:
            self._engine = ConnectionManager.get_engine(
                self.target_config, alias="target"
            )
        return self._engine

    @with_retry(max_attempts=5, base_delay=1.0, max_delay=60.0)
    def _load_batch(self, batch: List[Dict[str, Any]], batch_number: int) -> int:
        """
        Carga un lote individual usando INSERT con ON CONFLICT (upsert).
        Esto garantiza idempotencia en caso de reintentos.

        Returns:
            Número de registros insertados/actualizados.
        """
        if not batch:
            return 0

        metadata = MetaData()
        table = Table(self.table_name, metadata, autoload_with=self.engine)

        stmt = pg_insert(table).values(batch)

        # Upsert: si el PK ya existe, actualizar todos los campos
        update_columns = {
            col.name: stmt.excluded[col.name]
            for col in table.columns
            if col.name != self.primary_key
        }
        upsert_stmt = stmt.on_conflict_do_update(
            index_elements=[self.primary_key],
            set_=update_columns,
        )

        with self.engine.begin() as conn:
            conn.execute(upsert_stmt)

        logger.info(
            "Batch #%d cargado: %d registros en '%s'.",
            batch_number,
            len(batch),
            self.table_name,
        )
        return len(batch)

    def _split_into_batches(
        self, records: List[Dict[str, Any]]
    ) -> List[List[Dict[str, Any]]]:
        """Divide registros en sub-lotes del tamaño configurado."""
        chunk_size = self.etl_config.chunk_size
        return [
            records[i: i + chunk_size]
            for i in range(0, len(records), chunk_size)
        ]

    def load(self, records: List[Dict[str, Any]]) -> Dict[str, int]:
        """
        Carga registros con paralelismo controlado.

        El número de workers no excede max_workers NI el pool_size
        de la base de datos para evitar saturación.

        Returns:
            Dict con estadísticas de carga (loaded, failed).
        """
        batches = self._split_into_batches(records)
        max_workers = min(
            self.etl_config.max_workers,
            self.target_config.pool_size,
            len(batches),
        )

        logger.info(
            "Cargando %d registros en %d batches con %d workers paralelos.",
            len(records),
            len(batches),
            max_workers,
        )

        loaded = 0
        failed = 0

        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = {
                executor.submit(self._load_batch, batch, i + 1): i
                for i, batch in enumerate(batches)
            }

            for future in as_completed(futures):
                batch_idx = futures[future]
                try:
                    count = future.result()
                    loaded += count
                except Exception as exc:
                    failed += len(batches[batch_idx])
                    logger.error(
                        "Error cargando batch #%d: %s - %s",
                        batch_idx + 1,
                        type(exc).__name__,
                        str(exc),
                    )

        self._loaded_count += loaded
        self._failed_count += failed

        stats = {"loaded": loaded, "failed": failed, "total": len(records)}
        logger.info("Resultado de carga: %s", stats)
        return stats

    @property
    def stats(self) -> Dict[str, int]:
        return {
            "total_loaded": self._loaded_count,
            "total_failed": self._failed_count,
        }
