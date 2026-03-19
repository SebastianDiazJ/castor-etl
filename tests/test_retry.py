"""
Tests unitarios para el mecanismo de reintentos (Exponential Backoff).
Simula fallos transitorios de red/base de datos.
"""

import pytest
from unittest.mock import MagicMock, patch
from sqlalchemy.exc import OperationalError, InterfaceError

from src.utils.retry import with_retry


class TestRetryMechanism:
    """Tests para el decorador with_retry."""

    def test_success_on_first_attempt(self):
        """Función exitosa en el primer intento no debe reintentar."""
        call_count = 0

        @with_retry(max_attempts=3, base_delay=0.001)
        def successful_function():
            nonlocal call_count
            call_count += 1
            return "success"

        result = successful_function()
        assert result == "success"
        assert call_count == 1

    def test_success_after_transient_failure(self):
        """Debe reintentar y tener éxito tras fallos transitorios."""
        call_count = 0

        @with_retry(max_attempts=3, base_delay=0.001)
        def flaky_function():
            nonlocal call_count
            call_count += 1
            if call_count < 3:
                raise OperationalError("connection lost", {}, None)
            return "recovered"

        result = flaky_function()
        assert result == "recovered"
        assert call_count == 3

    def test_raises_after_max_attempts(self):
        """Debe lanzar excepción tras agotar todos los intentos."""

        @with_retry(max_attempts=3, base_delay=0.001)
        def always_fails():
            raise OperationalError("db down", {}, None)

        with pytest.raises(OperationalError):
            always_fails()

    def test_non_transient_exception_not_retried(self):
        """Excepciones no transitorias no deben generar reintentos."""
        call_count = 0

        @with_retry(max_attempts=3, base_delay=0.001)
        def raises_value_error():
            nonlocal call_count
            call_count += 1
            raise ValueError("not a transient error")

        with pytest.raises(ValueError):
            raises_value_error()

        assert call_count == 1  # Solo 1 intento, sin reintentos

    def test_interface_error_triggers_retry(self):
        """InterfaceError (desconexión) debe disparar reintento."""
        call_count = 0

        @with_retry(max_attempts=3, base_delay=0.001)
        def interface_error_func():
            nonlocal call_count
            call_count += 1
            if call_count < 2:
                raise InterfaceError("interface lost", {}, None)
            return "ok"

        result = interface_error_func()
        assert result == "ok"
        assert call_count == 2

    def test_connection_error_triggers_retry(self):
        """ConnectionError nativo de Python debe disparar reintento."""
        call_count = 0

        @with_retry(max_attempts=3, base_delay=0.001)
        def network_error_func():
            nonlocal call_count
            call_count += 1
            if call_count < 2:
                raise ConnectionError("network unreachable")
            return "reconnected"

        result = network_error_func()
        assert result == "reconnected"

    @patch("src.utils.retry.time.sleep")
    def test_exponential_backoff_delay(self, mock_sleep):
        """El delay debe crecer exponencialmente entre reintentos."""
        call_count = 0

        @with_retry(max_attempts=4, base_delay=1.0, exponential_base=2.0)
        def fails_three_times():
            nonlocal call_count
            call_count += 1
            if call_count < 4:
                raise OperationalError("retry me", {}, None)
            return "done"

        result = fails_three_times()
        assert result == "done"
        assert mock_sleep.call_count == 3

        # Verificar que los delays respetan el rango exponencial
        for i, call in enumerate(mock_sleep.call_args_list):
            delay = call[0][0]
            max_expected = 1.0 * (2.0 ** i)  # 1, 2, 4
            assert 0 <= delay <= max_expected

    def test_timeout_error_triggers_retry(self):
        """TimeoutError debe disparar reintento."""
        call_count = 0

        @with_retry(max_attempts=2, base_delay=0.001)
        def timeout_func():
            nonlocal call_count
            call_count += 1
            if call_count < 2:
                raise TimeoutError("query timeout")
            return "completed"

        result = timeout_func()
        assert result == "completed"
