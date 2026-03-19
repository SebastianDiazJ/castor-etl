"""
Capa de validación de esquemas.
Valida que los datos de entrada coincidan con el esquema destino
ANTES de iniciar la carga, evitando escrituras parciales o corruptas.
"""

import logging
from datetime import datetime, date
from decimal import Decimal
from typing import Any, Dict, List, Optional, Type

from pydantic import BaseModel, ValidationError, field_validator, ConfigDict

logger = logging.getLogger(__name__)


# ─── Esquemas de ejemplo para tablas financieras ─────────────────────────────


class FinancialTransactionSchema(BaseModel):
    """Esquema de validación para la tabla de transacciones financieras."""

    model_config = ConfigDict(strict=False, coerce_numbers_to_str=False)

    transaction_id: int
    account_id: int
    amount: Decimal
    currency: str
    transaction_date: datetime
    description: Optional[str] = None
    category: Optional[str] = None
    status: str
    created_at: datetime
    updated_at: datetime

    @field_validator("currency")
    @classmethod
    def currency_must_be_valid(cls, v: str) -> str:
        if len(v) != 3:
            raise ValueError(f"Código de moneda debe ser ISO 4217 (3 chars), recibido: '{v}'")
        return v.upper()

    @field_validator("status")
    @classmethod
    def status_must_be_valid(cls, v: str) -> str:
        valid = {"pending", "completed", "failed", "cancelled"}
        if v.lower() not in valid:
            raise ValueError(f"Status inválido: '{v}'. Válidos: {valid}")
        return v.lower()

    @field_validator("amount")
    @classmethod
    def amount_must_be_positive(cls, v: Decimal) -> Decimal:
        if v < 0:
            raise ValueError(f"Amount no puede ser negativo: {v}")
        return v


class AccountSchema(BaseModel):
    """Esquema de validación para la tabla de cuentas."""

    account_id: int
    account_number: str
    account_type: str
    owner_name: str
    balance: Decimal
    is_active: bool
    created_at: datetime
    updated_at: datetime


# ─── Registro de esquemas ────────────────────────────────────────────────────

SCHEMA_REGISTRY: Dict[str, Type[BaseModel]] = {
    "financial_transactions": FinancialTransactionSchema,
    "accounts": AccountSchema,
}


# ─── Validador principal ─────────────────────────────────────────────────────


class SchemaValidator:
    """
    Valida lotes de registros contra un esquema Pydantic registrado.
    Separa registros válidos de inválidos para no bloquear la carga completa.
    """

    def __init__(self, table_name: str):
        if table_name not in SCHEMA_REGISTRY:
            raise ValueError(
                f"Esquema no registrado para tabla '{table_name}'. "
                f"Disponibles: {list(SCHEMA_REGISTRY.keys())}"
            )
        self.table_name = table_name
        self.schema_class = SCHEMA_REGISTRY[table_name]
        self._error_count = 0
        self._valid_count = 0

    def validate_record(self, record: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """
        Valida un registro individual contra el esquema.

        Returns:
            Dict con datos validados si es correcto, None si falla validación.
        """
        try:
            validated = self.schema_class.model_validate(record)
            self._valid_count += 1
            return validated.model_dump()
        except ValidationError as exc:
            self._error_count += 1
            logger.warning(
                "Registro inválido en '%s': %s | Datos: %s",
                self.table_name,
                exc.errors(),
                {k: str(v)[:50] for k, v in record.items()},  # Truncar para log
            )
            return None

    def validate_batch(
        self, records: List[Dict[str, Any]]
    ) -> tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
        """
        Valida un lote completo de registros.

        Returns:
            Tupla de (registros_válidos, registros_inválidos).
        """
        valid_records = []
        invalid_records = []

        for record in records:
            result = self.validate_record(record)
            if result is not None:
                valid_records.append(result)
            else:
                invalid_records.append(record)

        logger.info(
            "Validación '%s': %d válidos, %d inválidos de %d totales.",
            self.table_name,
            len(valid_records),
            len(invalid_records),
            len(records),
        )

        return valid_records, invalid_records

    @property
    def stats(self) -> Dict[str, int]:
        return {
            "valid": self._valid_count,
            "invalid": self._error_count,
            "total": self._valid_count + self._error_count,
        }
