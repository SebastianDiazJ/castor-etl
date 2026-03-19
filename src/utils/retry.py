"""
Módulo de resiliencia: Implementa Exponential Backoff con jitter
para reintentos ante fallos transitorios de red o base de datos.
"""

import logging
import random
import time
from functools import wraps
from typing import Callable, Tuple, Type

from sqlalchemy.exc import (
    DBAPIError,
    DisconnectionError,
    OperationalError,
    InterfaceError,
)

logger = logging.getLogger(__name__)

# Excepciones consideradas transitorias (reintentables)
TRANSIENT_EXCEPTIONS: Tuple[Type[Exception], ...] = (
    OperationalError,
    DisconnectionError,
    InterfaceError,
    DBAPIError,
    ConnectionError,
    TimeoutError,
)


def with_retry(
    max_attempts: int = 5,
    base_delay: float = 1.0,
    max_delay: float = 60.0,
    exponential_base: float = 2.0,
    transient_exceptions: Tuple[Type[Exception], ...] = TRANSIENT_EXCEPTIONS,
):
    """
    Decorador que aplica Exponential Backoff con jitter.

    Parámetros:
        max_attempts: Número máximo de intentos.
        base_delay: Delay inicial en segundos.
        max_delay: Delay máximo en segundos (cap).
        exponential_base: Base para el crecimiento exponencial.
        transient_exceptions: Tupla de excepciones que disparan reintento.
    """

    def decorator(func: Callable) -> Callable:
        @wraps(func)
        def wrapper(*args, **kwargs):
            last_exception = None

            for attempt in range(1, max_attempts + 1):
                try:
                    return func(*args, **kwargs)
                except transient_exceptions as exc:
                    last_exception = exc
                    if attempt == max_attempts:
                        logger.error(
                            "FALLO PERMANENTE en '%s' tras %d intentos. "
                            "Última excepción: %s - %s",
                            func.__name__,
                            max_attempts,
                            type(exc).__name__,
                            str(exc),
                        )
                        raise

                    # Exponential backoff con full jitter
                    delay = min(
                        base_delay * (exponential_base ** (attempt - 1)),
                        max_delay,
                    )
                    jittered_delay = random.uniform(0, delay)

                    logger.warning(
                        "Intento %d/%d fallido en '%s': %s - %s. "
                        "Reintentando en %.2fs...",
                        attempt,
                        max_attempts,
                        func.__name__,
                        type(exc).__name__,
                        str(exc),
                        jittered_delay,
                    )
                    time.sleep(jittered_delay)

            raise last_exception  # type: ignore

        return wrapper

    return decorator
