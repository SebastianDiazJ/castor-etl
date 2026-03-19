"""
Tests unitarios para el transformador de datos.
"""

import pytest
from datetime import datetime
from decimal import Decimal

from src.transformers.data_transformer import (
    DataTransformer,
    normalize_currency,
    safe_decimal,
    parse_datetime,
)


class TestDataTransformer:
    """Tests para DataTransformer."""

    def test_identity_transform(self, sample_valid_record):
        """Sin configuración, el transformador debe devolver el registro intacto."""
        transformer = DataTransformer()
        result = transformer.transform_record(sample_valid_record)
        assert result == sample_valid_record

    def test_column_mapping(self):
        """Debe renombrar columnas según el mapeo configurado."""
        transformer = DataTransformer(
            column_mapping={"old_name": "new_name", "fecha": "date"}
        )
        record = {"old_name": "value1", "fecha": "2024-01-01", "other": "keep"}
        result = transformer.transform_record(record)

        assert "new_name" in result
        assert "date" in result
        assert "other" in result
        assert "old_name" not in result
        assert "fecha" not in result

    def test_drop_columns(self):
        """Debe eliminar columnas marcadas para eliminación."""
        transformer = DataTransformer(drop_columns=["internal_id", "temp_flag"])
        record = {"id": 1, "name": "test", "internal_id": 999, "temp_flag": True}
        result = transformer.transform_record(record)

        assert "id" in result
        assert "name" in result
        assert "internal_id" not in result
        assert "temp_flag" not in result

    def test_type_converters(self):
        """Debe aplicar conversiones de tipo configuradas."""
        transformer = DataTransformer(
            type_converters={
                "amount": lambda v: Decimal(str(v)),
                "name": lambda v: v.upper(),
            }
        )
        record = {"amount": "1500.50", "name": "test"}
        result = transformer.transform_record(record)

        assert result["amount"] == Decimal("1500.50")
        assert result["name"] == "TEST"

    def test_batch_transform(self, sample_batch):
        """Debe transformar un lote completo."""
        transformer = DataTransformer()
        results = transformer.transform_batch(sample_batch)
        assert len(results) == len(sample_batch)

    def test_combined_operations(self):
        """Debe aplicar mapeo, conversión y eliminación simultáneamente."""
        transformer = DataTransformer(
            column_mapping={"src_amount": "amount"},
            type_converters={"amount": safe_decimal},
            drop_columns=["_internal"],
        )
        record = {"src_amount": "999.99", "_internal": "drop_me", "status": "ok"}
        result = transformer.transform_record(record)

        assert result["amount"] == Decimal("999.99")
        assert "_internal" not in result
        assert result["status"] == "ok"


class TestHelperFunctions:
    """Tests para funciones auxiliares de transformación."""

    def test_normalize_currency_uppercase(self):
        assert normalize_currency("eur") == "EUR"
        assert normalize_currency("  usd  ") == "USD"

    def test_normalize_currency_default(self):
        assert normalize_currency(None) == "USD"
        assert normalize_currency("") == "USD"

    def test_safe_decimal_valid(self):
        assert safe_decimal("123.45") == Decimal("123.45")
        assert safe_decimal(100) == Decimal("100")

    def test_safe_decimal_none(self):
        assert safe_decimal(None) is None

    def test_safe_decimal_invalid(self):
        assert safe_decimal("not_a_number") == Decimal("0")

    def test_parse_datetime_standard(self):
        result = parse_datetime("2024-01-15 10:30:00")
        assert result == datetime(2024, 1, 15, 10, 30, 0)

    def test_parse_datetime_iso(self):
        result = parse_datetime("2024-01-15T10:30:00")
        assert result == datetime(2024, 1, 15, 10, 30, 0)

    def test_parse_datetime_passthrough(self):
        dt = datetime(2024, 6, 1)
        assert parse_datetime(dt) == dt

    def test_parse_datetime_none(self):
        assert parse_datetime(None) is None

    def test_parse_datetime_invalid(self):
        assert parse_datetime("not_a_date") is None
