import logging
from typing import (
    Iterator,
    List,
    Tuple,
)
from unittest.mock import Mock

import pyodbc

import pandas as pd
import pytest
from _pytest.logging import LogCaptureFixture
from _pytest.monkeypatch import MonkeyPatch
from forecasting_platform import master_config
from forecasting_platform.internal_schema import InternalSchemaBase
from forecasting_platform.static import (
    DatabaseConnectionFailure,
    DatabaseType,
)
from sqlalchemy import (
    Column,
    Integer,
    Table,
)
from sqlalchemy.orm import Session

from .database import (
    Database,
    retry_database_read_errors,
)


@pytest.mark.parametrize("database_type", [DatabaseType.internal, DatabaseType.dsx_write])
class TestDatabase:
    @pytest.mark.parametrize("attempts,is_disabled", [(-1, True), (0, True), (1, False), (10, False)])  # type: ignore
    def test_can_be_disabled(
        self, database_type: DatabaseType, attempts: int, is_disabled: bool, monkeypatch: MonkeyPatch
    ) -> None:
        monkeypatch.setattr(master_config, "db_connection_attempts", attempts)
        assert Database(database_type).is_disabled() == is_disabled

    def test_no_connection_when_disabled(
        self, database_type: DatabaseType, monkeypatch: MonkeyPatch, caplog: LogCaptureFixture
    ) -> None:
        monkeypatch.setattr(master_config, "db_connection_attempts", 0)

        with caplog.at_level(logging.INFO):
            database = Database(database_type)
            assert caplog.messages == [f"Skipping {database} connection due to db_connection_attempts==0"]

    def test_database_type_defines_the_connection_string(self, database_type: DatabaseType) -> None:
        database = Database(database_type)
        assert database._get_connection_string() == "DSN=ML_Internal_DB;UID=sa;PWD=Password1;"

    def test_retry_and_fail(
        self, database_type: DatabaseType, monkeypatch: MonkeyPatch, caplog: LogCaptureFixture
    ) -> None:
        monkeypatch.setattr(master_config, "db_connection_attempts", 3)
        monkeypatch.setattr(master_config, "db_connection_retry_sleep_seconds", 0.01)

        monkeypatch.setattr(pyodbc, "connect", Mock(side_effect=pyodbc.OperationalError("Always fails!")))

        with pytest.raises(DatabaseConnectionFailure):
            with caplog.at_level(logging.DEBUG):
                database = Database(database_type)
                database.get_existing_table_names()  # Run something that explicitly connects to the database

        assert caplog.messages == [
            f"Trying to connect to {database_type.name} database, attempt #1",
            f"Unsuccessful attempt #1 to connect to {database_type.name} database: Always fails!",
            "Waiting 0.01 seconds before next connection attempt",
            f"Trying to connect to {database_type.name} database, attempt #2",
            f"Unsuccessful attempt #2 to connect to {database_type.name} database: Always fails!",
            "Waiting 0.01 seconds before next connection attempt",
            f"Trying to connect to {database_type.name} database, attempt #3",
            f"Unsuccessful attempt #3 to connect to {database_type.name} database: Always fails!",
            f"Unable to connect to {database_type.name} database after 3 attempt(s).",
        ]

    def test_retry_and_succeed(
        self, database_type: DatabaseType, monkeypatch: MonkeyPatch, caplog: LogCaptureFixture
    ) -> None:
        real_connect = pyodbc.connect
        attempt = 0

        def fail_twice_then_succeed(connection_string: str, timeout: float) -> pyodbc.Connection:
            nonlocal attempt
            if attempt < 2:
                attempt += 1
                raise pyodbc.OperationalError(f"Test Fail {attempt}")
            return real_connect(connection_string, timeout=timeout)

        monkeypatch.setattr(master_config, "db_connection_retry_sleep_seconds", 0.01)
        monkeypatch.setattr(pyodbc, "connect", fail_twice_then_succeed)

        with caplog.at_level(logging.DEBUG):
            database = Database(database_type)
            database.get_existing_table_names()  # Run something that explicitly connects to the database

        assert caplog.messages == [
            f"Trying to connect to {database}, attempt #1",
            f"Unsuccessful attempt #1 to connect to {database}: Test Fail 1",
            "Waiting 0.01 seconds before next connection attempt",
            f"Trying to connect to {database}, attempt #2",
            f"Unsuccessful attempt #2 to connect to {database}: Test Fail 2",
            "Waiting 0.01 seconds before next connection attempt",
            f"Trying to connect to {database}, attempt #3",
            f"Successfully connected to {database}",
        ]

    def test_retry_database_read_errors_can_be_disabled(
        self, database_type: DatabaseType, caplog: LogCaptureFixture, monkeypatch: MonkeyPatch
    ) -> None:
        caplog.set_level(logging.DEBUG)
        database = Database(database_type)

        mocked_query = Mock(side_effect=[pyodbc.OperationalError("Error 1")])
        monkeypatch.setattr(Session, "query", mocked_query)
        monkeypatch.setattr(master_config, "db_read_retries", 0)

        @retry_database_read_errors
        def query_with_retry() -> bool:
            with database.transaction_context() as session:
                return session.query()  # type: ignore

        with pytest.raises(pyodbc.OperationalError, match="Error 1"):
            assert query_with_retry()

        assert mocked_query.call_count == 1
        assert not (
            "Error 'Error 1' occurred while running query_with_retry. Trying again 1 more time(s) in 1 second(s)."
            in caplog.messages
        )
        assert f"Error during {database} operation, transaction was rolled-back: Error 1" in caplog.messages

    def test_retry_database_read_errors_fails_after_retries(
        self, database_type: DatabaseType, caplog: LogCaptureFixture, monkeypatch: MonkeyPatch
    ) -> None:
        caplog.set_level(logging.DEBUG)
        database = Database(database_type)

        mocked_query = Mock(side_effect=[pyodbc.OperationalError("Error 1"), pyodbc.OperationalError("Error 2")])
        monkeypatch.setattr(Session, "query", mocked_query)
        monkeypatch.setattr(master_config, "db_read_retries", 1)

        @retry_database_read_errors
        def query_with_retry() -> bool:
            with database.transaction_context() as session:
                return session.query()  # type: ignore

        with pytest.raises(pyodbc.OperationalError, match="Error 2"):
            assert query_with_retry()

        assert mocked_query.call_count == 2
        assert (
            "Error 'Error 1' occurred while running query_with_retry. Trying again 1 more time(s) in 1 second(s)."
            in caplog.messages
        )
        assert f"Error during {database} operation, transaction was rolled-back: Error 1" in caplog.messages
        assert f"Error during {database} operation, transaction was rolled-back: Error 2" in caplog.messages

    def test_retry_database_read_errors(
        self, database_type: DatabaseType, caplog: LogCaptureFixture, monkeypatch: MonkeyPatch
    ) -> None:
        caplog.set_level(logging.DEBUG)
        database = Database(database_type)

        mocked_query = Mock(side_effect=[pyodbc.OperationalError("Error 1"), pyodbc.OperationalError("Error 2"), True])
        monkeypatch.setattr(Session, "query", mocked_query)

        @retry_database_read_errors
        def query_with_retry() -> bool:
            with database.transaction_context() as session:
                return session.query()  # type: ignore

        assert query_with_retry()
        assert mocked_query.call_count == 3
        assert (
            "Error 'Error 1' occurred while running query_with_retry. Trying again 2 more time(s) in 1 second(s)."
            in caplog.messages
        )
        assert f"Error during {database} operation, transaction was rolled-back: Error 1" in caplog.messages
        assert (
            "Error 'Error 2' occurred while running query_with_retry. Trying again 1 more time(s) in 1 second(s)."
            in caplog.messages
        )
        assert f"Error during {database} operation, transaction was rolled-back: Error 2" in caplog.messages


def test_existing_tables_in_internal_database() -> None:
    database = Database(DatabaseType.internal)

    existing_tables = database.get_existing_table_names()

    assert len(existing_tables) > 0
    assert all(table in existing_tables for table in database.get_defined_table_names())


@pytest.fixture()  # type: ignore
def tmp_internal_table() -> Iterator[Tuple[Database, Table]]:
    internal_database = Database(DatabaseType.internal)

    TmpInternalTable = Table(
        "test-tmp-internal-database", InternalSchemaBase.metadata, Column("test_numbers", Integer, nullable=False)
    )
    TmpInternalTable.create()

    yield internal_database, TmpInternalTable

    TmpInternalTable.drop()
    InternalSchemaBase.metadata.remove(TmpInternalTable)


@pytest.mark.parametrize(
    "row_count, expected_logs",
    [
        (0, ["Inserting 0 rows to test-tmp-internal-database table of internal database"]),
        (
            1,
            [
                "Inserting 1 rows to test-tmp-internal-database table of internal database",
                "Inserting chunk 1 with shape (1, 1) to database table test-tmp-internal-database",
            ],
        ),
        (
            123,
            [
                "Inserting 123 rows to test-tmp-internal-database table of internal database",
                "Inserting chunk 1 with shape (123, 1) to database table test-tmp-internal-database",
            ],
        ),
        (
            23456,
            [
                "Inserting 23456 rows to test-tmp-internal-database table of internal database",
                "Inserting chunk 1 with shape (10000, 1) to database table test-tmp-internal-database",
                "Inserting chunk 2 with shape (10000, 1) to database table test-tmp-internal-database",
                "Inserting chunk 3 with shape (3456, 1) to database table test-tmp-internal-database",
            ],
        ),
    ],
)  # type: ignore
def test_internal_database_insert_data_frame_chunking(
    tmp_internal_table: Tuple[Database, Table], caplog: LogCaptureFixture, row_count: int, expected_logs: List[str],
) -> None:
    internal_database, table = tmp_internal_table

    caplog.set_level(logging.DEBUG)

    with internal_database.transaction_context() as session:
        assert session.query(table).count() == 0  # type: ignore

    df = pd.DataFrame({"test_numbers": range(row_count)})
    internal_database.insert_data_frame(df, table.name)

    with internal_database.transaction_context() as session:
        assert session.query(table).count() == row_count  # type: ignore

    assert caplog.messages == expected_logs
