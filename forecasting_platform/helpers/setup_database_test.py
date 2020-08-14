import logging
from typing import (
    Optional,
    cast,
)
from unittest.mock import Mock

import pytest
from _pytest.logging import LogCaptureFixture
from _pytest.monkeypatch import MonkeyPatch
from forecasting_platform import master_config
from forecasting_platform.services import Database
from forecasting_platform.static import DatabaseType
from sqlalchemy.dialects.mssql.pyodbc import MSDialect_pyodbc
from sqlalchemy.engine import Connection
from sqlalchemy.sql.base import Executable
from sqlalchemy.sql.ddl import CreateSchema

from . import (
    drop_known_tables,
    ensure_schema_exists,
    ensure_tables_exist,
)


@pytest.mark.parametrize("database_type", [DatabaseType.internal, DatabaseType.dsx_write])
class TestSetupDatabase:
    def test_no_drop_tables_when_disabled(
        self, database_type: DatabaseType, monkeypatch: MonkeyPatch, caplog: LogCaptureFixture
    ) -> None:
        monkeypatch.setattr(master_config, "db_connection_attempts", 0)

        database = Database(database_type)

        mock_drop_all = Mock()
        monkeypatch.setattr(database.schema_base_class.metadata, "drop_all", mock_drop_all)

        caplog.clear()
        with caplog.at_level(logging.INFO):
            drop_known_tables(database)
            assert f"Cannot drop tables, because {database} connection is not available" in caplog.messages

        mock_drop_all.assert_not_called()

    def test_drop_tables(self, database_type: DatabaseType, monkeypatch: MonkeyPatch) -> None:
        mock_drop_all = Mock()
        database = Database(database_type)
        monkeypatch.setattr(database.schema_base_class.metadata, "drop_all", mock_drop_all)

        drop_known_tables(database)

        mock_drop_all.assert_called_once()

    def test_no_ensure_tables_when_disabled(
        self, database_type: DatabaseType, monkeypatch: MonkeyPatch, caplog: LogCaptureFixture
    ) -> None:
        monkeypatch.setattr(master_config, "db_connection_attempts", 0)

        database = Database(database_type)

        mock_create_all = Mock()
        monkeypatch.setattr(database.schema_base_class.metadata, "create_all", mock_create_all)

        caplog.clear()
        with caplog.at_level(logging.INFO):
            ensure_tables_exist(database)
            assert f"Cannot setup tables, because {database} connection is not available" in caplog.messages

        mock_create_all.assert_not_called()

    def test_ensure_tables(self, database_type: DatabaseType, monkeypatch: MonkeyPatch) -> None:
        database = Database(database_type)

        mock_create_all = Mock()
        monkeypatch.setattr(database.schema_base_class.metadata, "create_all", mock_create_all)

        ensure_tables_exist(database)

        mock_create_all.assert_called_once()


def test_defined_tables_internal_database() -> None:
    assert Database(DatabaseType.internal).get_defined_table_names() == [
        "ml_internal.cleaned_data",
        "ml_internal.exogenous_feature",
        "ml_internal.forecast_data",
        "ml_internal.forecast_model_run",
        "ml_internal.forecast_run",
    ]


def test_defined_tables_dsx_write_database() -> None:
    assert Database(DatabaseType.dsx_write).get_defined_table_names() == [
        "ml_dsx_write.STG_Import_Periodic_ML",
    ]


def test_ensure_schema_exists(monkeypatch: MonkeyPatch, caplog: LogCaptureFixture) -> None:
    existing_schema = "existing_schema"
    mock_get_schema_names = Mock(return_value=[existing_schema])
    monkeypatch.setattr(MSDialect_pyodbc, "get_schema_names", mock_get_schema_names)

    internal_database = Database(DatabaseType.internal)
    internal_database._database_schema = existing_schema

    with caplog.at_level(logging.INFO):
        ensure_schema_exists(internal_database)
        assert f"Found schema: {existing_schema} in internal database" in caplog.messages

    mock_get_schema_names.assert_called_once()


def test_ensure_schema_exists_creates_missing_schema(monkeypatch: MonkeyPatch, caplog: LogCaptureFixture) -> None:
    new_schema = "new_schema"
    mock_get_schema_names = Mock(return_value=["old_schema"])

    internal_database = Database(DatabaseType.internal)
    internal_database._database_schema = new_schema

    with internal_database.transaction_context() as session:
        original_connection = session.connection()

        def mock_execute_create_schema(connection: Connection, statement: Executable) -> Optional[Connection]:
            if isinstance(statement, CreateSchema):
                assert statement.element == new_schema
                return None

            return cast(Connection, original_connection(connection, statement))

    monkeypatch.setattr(MSDialect_pyodbc, "get_schema_names", mock_get_schema_names)
    monkeypatch.setattr(Connection, "execute", mock_execute_create_schema)

    with caplog.at_level(logging.INFO):
        ensure_schema_exists(internal_database)
        assert f"Creating schema: {new_schema} in internal database" in caplog.messages

    mock_get_schema_names.assert_called_once()
