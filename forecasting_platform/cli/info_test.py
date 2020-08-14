import logging
import re

import pytest
from _pytest.monkeypatch import MonkeyPatch
from click.testing import CliRunner
from forecasting_platform import master_config

from .info import info


@pytest.mark.testlogging  # type: ignore
def test_info(cli_runner: CliRunner, monkeypatch: MonkeyPatch) -> None:
    monkeypatch.setattr(master_config, "log_level_console", logging.DEBUG)

    result = cli_runner.invoke(info)

    assert result.exit_code == 0
    assert re.search("DEBUG::initialize::global::Invoked cli command 'info' with parameters {}\n", result.output)
    assert re.search("H2O cluster uptime", result.output)
    assert re.search("INFO::info::global::Successfully connected to internal database and H2O server.", result.output)
