import re
from unittest.mock import Mock

import pytest
from _pytest.logging import LogCaptureFixture
from _pytest.monkeypatch import MonkeyPatch
from click.testing import CliRunner
from forecasting_platform import master_config
from forecasting_platform.dsx_write_schema import DsxWriteSchemaBase
from forecasting_platform.internal_schema import InternalSchemaBase
from forecasting_platform.static import DatabaseType

from .setup_database import setup_database

CONNECT_SUCCESS_LOG = "Successfully connected to internal database"
DROP_TABLES_LOG = (
    "Dropping own database tables: "
    "['ml_internal.cleaned_data',"
    " 'ml_internal.exogenous_feature',"
    " 'ml_internal.forecast_data',"
    " 'ml_internal.forecast_model_run',"
    " 'ml_internal.forecast_run']"
)
EXISTING_TABLES_PATTERN = "Found existing internal database tables"
NEW_TABLES_PATTERN = (
    "Now existing tables:.*"
    "'ml_internal.cleaned_data',"
    " 'ml_internal.exogenous_feature',"
    " 'ml_internal.forecast_data',"
    " 'ml_internal.forecast_model_run',"
    " 'ml_internal.forecast_run'"
)
EXISTING_SCHEMA_LOG = "Found schema: ml_internal in internal database"


@pytest.fixture()  # type: ignore
def mock_drop_all(monkeypatch: MonkeyPatch) -> Mock:
    mock = Mock()
    monkeypatch.setattr(InternalSchemaBase.metadata, "drop_all", mock)
    monkeypatch.setattr(DsxWriteSchemaBase.metadata, "drop_all", mock)
    return mock


def test_setup_database_internal(cli_runner: CliRunner, caplog: LogCaptureFixture, mock_drop_all: Mock) -> None:
    result = cli_runner.invoke(setup_database, ["internal"])
    assert result.exit_code == 0
    assert CONNECT_SUCCESS_LOG in caplog.messages
    assert EXISTING_SCHEMA_LOG in caplog.messages
    assert any(re.search(EXISTING_TABLES_PATTERN, message) for message in caplog.messages)
    assert any(re.search(NEW_TABLES_PATTERN, message) for message in caplog.messages)

    assert DROP_TABLES_LOG not in caplog.messages
    mock_drop_all.assert_not_called()


def test_setup_database_drop_tables_internal(
    cli_runner: CliRunner, caplog: LogCaptureFixture, mock_drop_all: Mock
) -> None:
    result = cli_runner.invoke(setup_database, ["internal", "--drop-tables"])
    assert result.exit_code == 0
    assert CONNECT_SUCCESS_LOG in caplog.messages
    assert EXISTING_SCHEMA_LOG in caplog.messages
    assert any(re.search(EXISTING_TABLES_PATTERN, message) for message in caplog.messages)
    assert any(re.search(NEW_TABLES_PATTERN, message) for message in caplog.messages)

    assert DROP_TABLES_LOG in caplog.messages
    mock_drop_all.assert_called_once()


def test_setup_database_dsx_write(cli_runner: CliRunner, caplog: LogCaptureFixture, mock_drop_all: Mock) -> None:
    result = cli_runner.invoke(setup_database, ["dsx-write"])
    assert result.exit_code == 0
    assert "Successfully connected to dsx_write database" in caplog.messages
    assert "Found schema: ml_dsx_write in dsx_write database" in caplog.messages
    assert "Found existing dsx_write database tables: {'ml_dsx_write.STG_Import_Periodic_ML'}" in caplog.messages
    assert "Now existing tables: ['ml_dsx_write.STG_Import_Periodic_ML']" in caplog.messages

    assert "Dropping own database tables: ['ml_dsx_write.STG_Import_Periodic_ML']" not in caplog.messages
    mock_drop_all.assert_not_called()


def test_setup_database_drop_tables_dsx_write(
    cli_runner: CliRunner, caplog: LogCaptureFixture, mock_drop_all: Mock, monkeypatch: MonkeyPatch
) -> None:
    monkeypatch.setitem(master_config.database_dsn, DatabaseType.dsx_write, "ML_Internal_DB")

    result = cli_runner.invoke(setup_database, ["dsx-write", "--drop-tables"])
    assert result.exit_code == 0
    assert "Successfully connected to dsx_write database" in caplog.messages
    assert "Found schema: ml_dsx_write in dsx_write database" in caplog.messages
    assert "Found existing dsx_write database tables: {'ml_dsx_write.STG_Import_Periodic_ML'}" in caplog.messages
    assert "Now existing tables: ['ml_dsx_write.STG_Import_Periodic_ML']" in caplog.messages

    assert "Dropping own database tables: ['ml_dsx_write.STG_Import_Periodic_ML']" in caplog.messages
    mock_drop_all.assert_called_once()


def test_setup_database_drop_tables_dsx_write_not_on_production(
    cli_runner: CliRunner, caplog: LogCaptureFixture, mock_drop_all: Mock, monkeypatch: MonkeyPatch
) -> None:
    monkeypatch.setitem(master_config.database_dsn, DatabaseType.dsx_write, "THIS_MIGHT_BE_PRODUCTION")

    result = cli_runner.invoke(setup_database, ["dsx-write", "--drop-tables"])
    assert result.exit_code == 1
    assert "Cannot drop tables on external production dsx_write database." == str(result.exception)

    mock_drop_all.assert_not_called()
