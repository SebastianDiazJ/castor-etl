"""
Tests unitarios para el gestor de conexiones.
"""

import pytest
from unittest.mock import patch, MagicMock
from config.settings import DatabaseConfig
from src.utils.connections import ConnectionManager


class TestConnectionManager:
    """Tests para ConnectionManager."""

    def setup_method(self):
        """Limpiar engines entre tests."""
        ConnectionManager._engines.clear()

    def test_connection_url_postgres(self, target_db_config):
        """Debe generar URL correcta para Postgres."""
        url = target_db_config.connection_url
        assert "postgresql://" in url
        assert "test_user" in url
        assert "5432" in url

    def test_connection_url_mssql(self, source_db_config):
        """Debe generar URL correcta para MSSQL."""
        url = source_db_config.connection_url
        assert "mssql+pyodbc://" in url
        assert "1433" in url

    @patch("src.utils.connections.create_engine")
    def test_engine_reuse(self, mock_create_engine, target_db_config):
        """get_engine debe reutilizar engines existentes (Singleton)."""
        mock_engine = MagicMock()
        mock_create_engine.return_value = mock_engine

        engine1 = ConnectionManager.get_engine(target_db_config, "test")
        engine2 = ConnectionManager.get_engine(target_db_config, "test")

        assert engine1 is engine2
        assert mock_create_engine.call_count == 1  # Solo 1 creación

    @patch("src.utils.connections.create_engine")
    def test_different_aliases_create_different_engines(
        self, mock_create_engine, target_db_config, source_db_config
    ):
        """Aliases distintos deben crear engines separados."""
        mock_create_engine.return_value = MagicMock()

        ConnectionManager.get_engine(target_db_config, "target")
        ConnectionManager.get_engine(source_db_config, "source")

        assert mock_create_engine.call_count == 2

    @patch("src.utils.connections.create_engine")
    def test_dispose_specific_alias(self, mock_create_engine, target_db_config):
        """dispose con alias debe eliminar solo ese engine."""
        mock_engine = MagicMock()
        mock_create_engine.return_value = mock_engine

        ConnectionManager.get_engine(target_db_config, "to_remove")
        ConnectionManager.dispose("to_remove")

        assert "to_remove" not in ConnectionManager._engines
        mock_engine.dispose.assert_called_once()

    @patch("src.utils.connections.create_engine")
    def test_dispose_all(self, mock_create_engine, target_db_config, source_db_config):
        """dispose sin alias debe eliminar todos los engines."""
        mock_create_engine.return_value = MagicMock()

        ConnectionManager.get_engine(target_db_config, "a")
        ConnectionManager.get_engine(source_db_config, "b")
        ConnectionManager.dispose()

        assert len(ConnectionManager._engines) == 0

    @patch("src.utils.connections.create_engine")
    def test_pool_pre_ping_enabled(self, mock_create_engine, target_db_config):
        """El engine debe crearse con pool_pre_ping=True."""
        mock_create_engine.return_value = MagicMock()
        ConnectionManager.get_engine(target_db_config, "ping_test")

        call_kwargs = mock_create_engine.call_args[1]
        assert call_kwargs["pool_pre_ping"] is True
