import logging
import logging.config
import os
import sys
from datetime import datetime
from logging.handlers import (
    QueueHandler,
    QueueListener,
)
from multiprocessing import Queue
from pathlib import Path
from platform import platform
from types import TracebackType
from typing import (
    Callable,
    Type,
)

from forecasting_platform import master_config
from forecasting_platform.static import LOG_DEFAULT_ACCOUNT_CONTEXT

_account_log_context: str = LOG_DEFAULT_ACCOUNT_CONTEXT

logger = logging.getLogger("initialize")


def initialize_logging(command_name: str, log_queue: "Queue[logging.LogRecord]") -> Callable[[], None]:
    """Initialize logging handlers and configuration.

    Args:
        command_name: Name of the command that is being logged.
        log_queue: Logging queue to collect log messages from sub-processes.

    Returns:
        Callback to stop the log queue listener when shutting down the platform.

    """
    _configure_logging(command_name)
    _log_unhandled_exceptions()

    log_listener = QueueListener(log_queue, *logging.getLogger().handlers, respect_handler_level=True)
    log_listener.start()

    def cleanup_callback() -> None:
        log_listener.stop()

    logger.debug(
        f"Initialized logging for main process (pid={os.getpid()}, parent={os.getppid()}, platform={platform()})"
    )

    return cleanup_callback


def initialize_subprocess_logging(log_queue: "Queue[logging.LogRecord]") -> None:
    """Initialize logging for a sub-process to allow consistent single file logging via the main process.

    Args:
        log_queue: Queue to send log messages to the main process.

    Note:
        https://docs.python.org/3/howto/logging-cookbook.html#logging-to-a-single-file-from-multiple-processes

    """
    logging_config = {
        "version": 1,
        "disable_existing_loggers": False,
        "handlers": {"queue": {"()": SubProcessQueueHandler, "queue": log_queue,}},
        "root": {"handlers": ["queue"], "level": logging.NOTSET},
    }

    logging.config.dictConfig(logging_config)

    _reduce_noise_from_library_loggers()

    logger.debug(f"Initialized logging for sub-process (pid={os.getpid()}, parent={os.getppid()})")


class SubProcessQueueHandler(QueueHandler):
    """Custom QueueHandler which includes the account log-context for each sub-process."""

    def prepare(self, record: logging.LogRecord) -> logging.LogRecord:
        """Override method to include account log-context from sub-process."""
        if not hasattr(record, "account"):
            record.account = _account_log_context  # type: ignore

        return super().prepare(record)  # type: ignore


def set_logging_context(context_name: str) -> None:
    """Set current logging context, otherwise ``LOG_DEFAULT_SCOPE`` is used.

    Args:
        context_name: Name to set for logging.

    """
    global _account_log_context
    _account_log_context = context_name


def reset_logging_context() -> None:
    """Set current logging context to ``LOG_DEFAULT_SCOPE``."""
    global _account_log_context
    _account_log_context = LOG_DEFAULT_ACCOUNT_CONTEXT


def _log_unhandled_exceptions() -> None:
    def _excepthook(exctype: Type[BaseException], exc: BaseException, tb: TracebackType) -> None:
        logging.exception("Exiting due to unhandled exception.", exc_info=(exctype, exc, tb))

    sys.excepthook = _excepthook


class _LogAccountFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        if not hasattr(record, "account"):
            record.account = _account_log_context  # type: ignore

        result = logging.Formatter.format(self, record)

        return result


def _configure_logging(command_name: str) -> None:
    logging_config = {
        "version": 1,
        "disable_existing_loggers": False,
        "formatters": {
            "default": {
                "()": _LogAccountFormatter,
                "format": master_config.logging_format,
                "datefmt": master_config.logging_timestamp_format,
            },
        },
        "handlers": {
            "file": {
                "class": "logging.FileHandler",
                "filename": _get_log_filename(command_name),
                "mode": "w",
                "encoding": "utf-8",
                "formatter": "default",
                "level": master_config.log_level_file,
            },
            "console": {
                "class": "logging.StreamHandler",
                "formatter": "default",
                "level": master_config.log_level_console,
            },
        },
        "root": {"handlers": ["console", "file"], "level": logging.NOTSET},
    }

    logging.config.dictConfig(logging_config)

    _reduce_noise_from_library_loggers()


def _reduce_noise_from_library_loggers() -> None:
    logging.getLogger("urllib3").setLevel(logging.INFO)
    logging.getLogger("urllib3.connectionpool").setLevel(logging.INFO)


def _get_log_filename(command_name: str) -> Path:
    log_directory = Path(master_config.log_output_location)
    log_directory.mkdir(parents=True, exist_ok=True)

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"{timestamp}_forecasting_platform_{command_name}.log"

    return log_directory / filename
