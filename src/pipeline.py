"""
Orquestador principal del pipeline ETL.
Coordina Extracción → Validación → Transformación → Carga.

Este módulo actúa como Facade (patrón de diseño) que simplifica
la interacción con los subsistemas internos.
"""

import logging
from datetime import datetime
from typing import Any, Dict, Optional

from config.settings import get_source_db_config, get_target_db_config, get_etl_config
from src.extractors.incremental_extractor import IncrementalExtractor
from src.transformers.data_transformer import DataTransformer
from src.validators.schema_validator import SchemaValidator
from src.loaders.batch_loader import BatchLoader
from src.utils.logging_config import setup_logging
from src.utils.connections import ConnectionManager

logger = logging.getLogger(__name__)


class ETLPipeline:
    """
    Pipeline ETL completo que orquesta:
    1. Extracción incremental desde MSSQL/Oracle
    2. Validación de esquemas con Pydantic
    3. Transformación de datos
    4. Carga por lotes a Postgres
    """

    def __init__(
        self,
        table_name: str,
        watermark_column: str = "updated_at",
        primary_key: str = "transaction_id",
        transformer: Optional[DataTransformer] = None,
    ):
        self.source_config = get_source_db_config()
        self.target_config = get_target_db_config()
        self.etl_config = get_etl_config()

        self.extractor = IncrementalExtractor(
            source_config=self.source_config,
            etl_config=self.etl_config,
            table_name=table_name,
            watermark_column=watermark_column,
        )

        self.validator = SchemaValidator(table_name=table_name)

        self.transformer = transformer or DataTransformer()

        self.loader = BatchLoader(
            target_config=self.target_config,
            etl_config=self.etl_config,
            table_name=table_name,
            primary_key=primary_key,
        )

        self._stats: Dict[str, Any] = {
            "started_at": None,
            "finished_at": None,
            "chunks_processed": 0,
            "total_extracted": 0,
            "total_valid": 0,
            "total_invalid": 0,
            "total_loaded": 0,
            "total_failed": 0,
        }

    def run(self, last_watermark: Optional[datetime] = None) -> Dict[str, Any]:
        """
        Ejecuta el pipeline completo.

        Args:
            last_watermark: Marca de agua de la última ejecución exitosa.
                            None = carga completa (full load).

        Returns:
            Dict con estadísticas completas de la ejecución.
        """
        self._stats["started_at"] = datetime.utcnow().isoformat()
        logger.info("=" * 60)
        logger.info("PIPELINE ETL INICIADO")
        logger.info("=" * 60)

        try:
            for chunk in self.extractor.extract(last_watermark):
                self._stats["chunks_processed"] += 1
                self._stats["total_extracted"] += len(chunk)

                # Paso 1: Validar contra esquema destino
                valid_records, invalid_records = self.validator.validate_batch(chunk)
                self._stats["total_valid"] += len(valid_records)
                self._stats["total_invalid"] += len(invalid_records)

                if not valid_records:
                    logger.warning(
                        "Chunk #%d sin registros válidos, saltando carga.",
                        self._stats["chunks_processed"],
                    )
                    continue

                # Paso 2: Transformar datos
                transformed = self.transformer.transform_batch(valid_records)

                # Paso 3: Cargar a destino
                load_result = self.loader.load(transformed)
                self._stats["total_loaded"] += load_result["loaded"]
                self._stats["total_failed"] += load_result["failed"]

        except Exception as exc:
            logger.error("ERROR CRÍTICO en pipeline: %s - %s", type(exc).__name__, exc)
            self._stats["error"] = str(exc)
            raise
        finally:
            self._stats["finished_at"] = datetime.utcnow().isoformat()
            logger.info("=" * 60)
            logger.info("PIPELINE ETL FINALIZADO")
            logger.info("Estadísticas: %s", self._stats)
            logger.info("=" * 60)

        return self._stats


# ─── Entry point ─────────────────────────────────────────────────────────────


def main():
    """Punto de entrada principal del pipeline."""
    setup_logging(level="INFO")

    pipeline = ETLPipeline(
        table_name="financial_transactions",
        watermark_column="updated_at",
        primary_key="transaction_id",
    )

    # En producción, last_watermark se leería de una tabla de control
    # o un archivo de estado para mantener la marca de agua entre ejecuciones.
    stats = pipeline.run(last_watermark=None)
    logger.info("Pipeline completado con stats: %s", stats)


if __name__ == "__main__":
    main()
