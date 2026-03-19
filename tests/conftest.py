"""
Fixtures compartidos para todas las pruebas.
"""

import os
import sys
from datetime import datetime
from decimal import Decimal
from unittest.mock import MagicMock

import pytest

# Agregar el directorio raíz al path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config.settings import DatabaseConfig, ETLConfig


@pytest.fixture
def source_db_config():
    """Configuración de prueba para la base de datos fuente."""
    return DatabaseConfig(
        host="localhost",
        port=1433,
        database="test_legacy",
        username="test_user",
        password="test_pass",
        driver="mssql+pyodbc",
        pool_size=3,
        max_overflow=5,
    )


@pytest.fixture
def target_db_config():
    """Configuración de prueba para la base de datos destino."""
    return DatabaseConfig(
        host="localhost",
        port=5432,
        database="test_analytics",
        username="test_user",
        password="test_pass",
        pool_size=3,
        max_overflow=5,
    )


@pytest.fixture
def etl_config():
    """Configuración de prueba para el pipeline ETL."""
    return ETLConfig(
        chunk_size=100,
        max_workers=2,
        retry_max_attempts=3,
        retry_base_delay=0.01,  # Delays pequeños para tests rápidos
        retry_max_delay=0.1,
    )


@pytest.fixture
def sample_valid_record():
    """Registro financiero válido de ejemplo."""
    return {
        "transaction_id": 1,
        "account_id": 1001,
        "amount": Decimal("1500.50"),
        "currency": "USD",
        "transaction_date": datetime(2024, 1, 15, 10, 30, 0),
        "description": "Wire transfer",
        "category": "transfer",
        "status": "completed",
        "created_at": datetime(2024, 1, 15, 10, 30, 0),
        "updated_at": datetime(2024, 1, 15, 10, 30, 0),
    }


@pytest.fixture
def sample_invalid_record():
    """Registro financiero con datos inválidos."""
    return {
        "transaction_id": 2,
        "account_id": 1002,
        "amount": Decimal("-100.00"),  # Negativo → inválido
        "currency": "INVALID",  # Más de 3 chars → inválido
        "transaction_date": datetime(2024, 1, 15, 10, 30, 0),
        "description": "Bad record",
        "category": "test",
        "status": "unknown_status",  # Status no reconocido → inválido
        "created_at": datetime(2024, 1, 15, 10, 30, 0),
        "updated_at": datetime(2024, 1, 15, 10, 30, 0),
    }


@pytest.fixture
def sample_batch(sample_valid_record):
    """Lote de registros válidos para pruebas de carga."""
    records = []
    for i in range(10):
        record = sample_valid_record.copy()
        record["transaction_id"] = i + 1
        record["amount"] = Decimal(f"{(i + 1) * 100}.00")
        records.append(record)
    return records


@pytest.fixture
def mock_engine():
    """Engine mock para pruebas sin conexión real."""
    return MagicMock()
