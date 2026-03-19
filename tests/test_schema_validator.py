"""
Tests unitarios para el validador de esquemas.
Verifica que la capa de validación detecte datos inválidos
antes de intentar la carga a Postgres.
"""

import pytest
from datetime import datetime
from decimal import Decimal

from src.validators.schema_validator import SchemaValidator, SCHEMA_REGISTRY


class TestSchemaValidator:
    """Tests para SchemaValidator."""

    def test_valid_record_passes(self, sample_valid_record):
        """Un registro con todos los campos correctos debe pasar validación."""
        validator = SchemaValidator("financial_transactions")
        result = validator.validate_record(sample_valid_record)

        assert result is not None
        assert result["transaction_id"] == 1
        assert result["currency"] == "USD"
        assert result["status"] == "completed"

    def test_invalid_currency_rejected(self, sample_valid_record):
        """Un código de moneda con longitud != 3 debe ser rechazado."""
        record = sample_valid_record.copy()
        record["currency"] = "INVALID"  # 7 chars, no es ISO 4217
        validator = SchemaValidator("financial_transactions")
        result = validator.validate_record(record)

        assert result is None
        assert validator.stats["invalid"] == 1

    def test_invalid_status_rejected(self, sample_valid_record):
        """Un status no reconocido debe ser rechazado."""
        record = sample_valid_record.copy()
        record["status"] = "nonexistent_status"
        validator = SchemaValidator("financial_transactions")
        result = validator.validate_record(record)

        assert result is None

    def test_negative_amount_rejected(self, sample_valid_record):
        """Un monto negativo debe ser rechazado."""
        record = sample_valid_record.copy()
        record["amount"] = Decimal("-500.00")
        validator = SchemaValidator("financial_transactions")
        result = validator.validate_record(record)

        assert result is None

    def test_missing_required_field_rejected(self, sample_valid_record):
        """Un registro sin campo obligatorio debe ser rechazado."""
        record = sample_valid_record.copy()
        del record["transaction_id"]
        validator = SchemaValidator("financial_transactions")
        result = validator.validate_record(record)

        assert result is None

    def test_batch_validation_separates_valid_and_invalid(self):
        """validate_batch debe separar correctamente válidos de inválidos."""
        records = [
            {
                "transaction_id": 1,
                "account_id": 100,
                "amount": Decimal("500.00"),
                "currency": "USD",
                "transaction_date": datetime.now(),
                "status": "completed",
                "created_at": datetime.now(),
                "updated_at": datetime.now(),
            },
            {
                "transaction_id": 2,
                "account_id": 101,
                "amount": Decimal("-10.00"),  # Inválido
                "currency": "EUR",
                "transaction_date": datetime.now(),
                "status": "completed",
                "created_at": datetime.now(),
                "updated_at": datetime.now(),
            },
            {
                "transaction_id": 3,
                "account_id": 102,
                "amount": Decimal("200.00"),
                "currency": "TOOLONG",  # Inválido
                "transaction_date": datetime.now(),
                "status": "completed",
                "created_at": datetime.now(),
                "updated_at": datetime.now(),
            },
        ]

        validator = SchemaValidator("financial_transactions")
        valid, invalid = validator.validate_batch(records)

        assert len(valid) == 1
        assert len(invalid) == 2
        assert valid[0]["transaction_id"] == 1

    def test_unregistered_table_raises_error(self):
        """Intentar validar una tabla no registrada debe lanzar error."""
        with pytest.raises(ValueError, match="Esquema no registrado"):
            SchemaValidator("tabla_inexistente")

    def test_stats_tracking(self, sample_valid_record):
        """Las estadísticas de validación deben actualizarse correctamente."""
        validator = SchemaValidator("financial_transactions")

        validator.validate_record(sample_valid_record)
        invalid = sample_valid_record.copy()
        invalid["currency"] = "XXXXX"
        validator.validate_record(invalid)

        assert validator.stats["valid"] == 1
        assert validator.stats["invalid"] == 1
        assert validator.stats["total"] == 2

    def test_optional_fields_can_be_none(self, sample_valid_record):
        """Campos opcionales (description, category) pueden ser None."""
        record = sample_valid_record.copy()
        record["description"] = None
        record["category"] = None
        validator = SchemaValidator("financial_transactions")
        result = validator.validate_record(record)

        assert result is not None
        assert result["description"] is None

    def test_currency_normalized_to_uppercase(self, sample_valid_record):
        """El código de moneda debe normalizarse a mayúsculas."""
        record = sample_valid_record.copy()
        record["currency"] = "eur"
        validator = SchemaValidator("financial_transactions")
        result = validator.validate_record(record)

        assert result is not None
        assert result["currency"] == "EUR"
