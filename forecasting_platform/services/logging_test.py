import logging
import re
from contextlib import contextmanager
from pathlib import Path
from typing import Iterator

import pytest
from _pytest.capture import CaptureFixture
from _pytest.monkeypatch import MonkeyPatch
from forecasting_platform import master_config

from .logging import (
    initialize_logging,
    reset_logging_context,
    set_logging_context,
)
from .multiprocessing import initialize_multiprocessing


@contextmanager
def logging_initialized() -> Iterator[None]:
    multiprocessing_context = initialize_multiprocessing()
    log_queue = multiprocessing_context.Queue(-1)
    clean_up = initialize_logging("info", log_queue)

    yield

    clean_up()
    log_queue.close()


@pytest.mark.testlogging
class TestLogging:
    def test_any_logger_logs_account_context(self, capsys: CaptureFixture) -> None:
        with logging_initialized():
            logger = logging.getLogger("account_logger")
            set_logging_context("test_default_account")

            logger.info("Message in default context")
            captured = capsys.readouterr()
            assert re.search("INFO::account_logger::test_default_account::Message in default context\n", captured.err)

            set_logging_context("Account 1")
            logger.info("Overwriting previously set context")
            captured = capsys.readouterr()
            assert re.search("INFO::account_logger::Account 1::Overwriting previously set context\n", captured.err)

            reset_logging_context()
            logger.info("Reset default context")
            captured = capsys.readouterr()
            assert re.search("INFO::account_logger::global::Reset default context\n", captured.err)

    def test_any_logger_logs_global_context_as_default(self, capsys: CaptureFixture) -> None:
        with logging_initialized():
            logger = logging.getLogger("account_logger")

            logger.info("Message in global context")
            captured = capsys.readouterr()
            assert re.search("INFO::account_logger::global::Message in global context\n", captured.err)

    def test_root_logger_logs_global_context_as_default(self, capsys: CaptureFixture) -> None:
        with logging_initialized():
            logger = logging.getLogger("logging_test")
            logger.info("Message in global context")
            captured = capsys.readouterr()
            assert re.search("INFO::logging_test::global::Message in global context\n", captured.err)

    def test_existing_loggers_use_fallback_format(self, capsys: CaptureFixture) -> None:
        existing_logger = logging.getLogger("existing_logger")

        with logging_initialized():
            existing_logger.info("Message from existing logger")
            captured = capsys.readouterr()
            assert re.search("INFO::existing_logger::global::Message from existing logger\n", captured.err)

    def test_log_level_is_configurable_via_master_config(
        self, capsys: CaptureFixture, monkeypatch: MonkeyPatch
    ) -> None:
        monkeypatch.setattr(master_config, "log_level_console", logging.WARNING)

        with logging_initialized():
            logger = logging.getLogger("logging_test")
            logger.debug("Debug message will be ignored")
            logger.warning("Warning message will be logged")
            logger.critical("Critical message will be logged")
            captured = capsys.readouterr()

            assert re.search("CRITICAL::logging_test::global::Critical message will be logged\n", captured.err)
            assert re.search("WARNING::logging_test::global::Warning message will be logged\n", captured.err)
            assert not re.search("DEBUG", captured.err)

    @pytest.mark.freeze_time("2000-12-31-17-00")  # type: ignore
    def test_logs_are_saved_to_file(self, tmp_path: Path, monkeypatch: MonkeyPatch) -> None:
        monkeypatch.setattr(master_config, "log_output_location", tmp_path.resolve())

        with logging_initialized():
            logger = logging.getLogger("logging_test")
            logger.debug("Debug Message in global context")

            expected_log_file = Path(tmp_path / "20001231_170000_forecasting_platform_info.log")
            assert expected_log_file.exists()
            assert "DEBUG::logging_test::global::Debug Message in global context" in expected_log_file.read_text()
