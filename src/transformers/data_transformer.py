"""
Transformador de datos: Aplica transformaciones necesarias entre el esquema
fuente (MSSQL/Oracle) y el esquema destino (Postgres).

Principio Single Responsibility: Este módulo SOLO transforma datos.
No extrae ni persiste.
"""

import logging
from datetime import datetime
from decimal import Decimal
from typing import Any, Callable, Dict, List, Optional

logger = logging.getLogger(__name__)


class DataTransformer:
    """
    Aplica transformaciones configurables a los registros extraídos.
    Soporta mapeo de columnas, conversión de tipos y transformaciones custom.
    """

    def __init__(
            self,
            column_mapping: Optional[Dict[str, str]] = None,
            type_converters: Optional[Dict[str, Callable]] = None,
            drop_columns: Optional[List[str]] = None,
    ):
        """
        Args:
            column_mapping: Mapeo fuente->destino de nombres de columnas.
            type_converters: Funciones de conversión por columna.
            drop_columns: Columnas a eliminar del resultado.
        """
        self.column_mapping = column_mapping or {}
        self.type_converters = type_converters or {}
        self.drop_columns = set(drop_columns or [])

    def transform_record(self, record: Dict[str, Any]) -> Dict[str, Any]:
        """Transforma un registro individual."""
        result = {}

        for key, value in record.items():
            # Eliminar columnas no deseadas
            if key in self.drop_columns:
                continue

            # Aplicar mapeo de columnas
            target_key = self.column_mapping.get(key, key)

            # Sanitizar strings: MSSQL puede devolver bytes en Windows-1252/latin1
            # que Postgres (UTF-8) no acepta
            if isinstance(value, bytes):
                try:
                    value = value.decode("utf-8")
                except UnicodeDecodeError:
                    value = value.decode("latin-1", errors="replace")
            elif isinstance(value, str):
                # Re-encode para limpiar caracteres problemáticos
                value = value.encode("utf-8", errors="replace").decode("utf-8")

            # Aplicar conversión de tipo
            if target_key in self.type_converters:
                try:
                    value = self.type_converters[target_key](value)
                except (ValueError, TypeError) as exc:
                    logger.warning(
                        "Error convirtiendo columna '%s' valor '%s': %s",
                        target_key,
                        value,
                        exc,
                    )

            result[target_key] = value

        return result

    def transform_batch(
            self, records: List[Dict[str, Any]]
    ) -> List[Dict[str, Any]]:
        """Transforma un lote completo de registros."""
        transformed = [self.transform_record(r) for r in records]
        logger.info("Batch transformado: %d registros.", len(transformed))
        return transformed


# ─── Transformadores predefinidos ────────────────────────────────────────────


def normalize_currency(value: Any) -> str:
    """Normaliza código de moneda a uppercase."""
    return str(value).strip().upper() if value else "USD"


def safe_decimal(value: Any) -> Optional[Decimal]:
    """Convierte a Decimal de forma segura."""
    if value is None:
        return None
    try:
        return Decimal(str(value))
    except Exception:
        return Decimal("0")


def parse_datetime(value: Any) -> Optional[datetime]:
    """Parsea datetime desde múltiples formatos."""
    if value is None:
        return None
    if isinstance(value, datetime):
        return value
    formats = [
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%dT%H:%M:%S",
        "%Y-%m-%d %H:%M:%S.%f",
        "%d/%m/%Y %H:%M:%S",
    ]
    for fmt in formats:
        try:
            return datetime.strptime(str(value), fmt)
        except ValueError:
            continue
    logger.warning("No se pudo parsear datetime: '%s'", value)
    return None