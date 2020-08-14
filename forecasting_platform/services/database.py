from __future__ import annotations

import logging
import os
import platform
from contextlib import contextmanager
from functools import wraps
from time import sleep
from typing import (
    Any,
    Callable,
    Iterator,
    List,
    Optional,
    Type,
    TypeVar,
    Union,
    cast,
)

import pyodbc

import numpy as np
import pandas as pd
from forecasting_platform import master_config
from forecasting_platform.dsx_read_schema import DsxReadSchemaBase
from forecasting_platform.dsx_write_schema import DsxWriteSchemaBase
from forecasting_platform.internal_schema import InternalSchemaBase
from forecasting_platform.static import (
    MS_SQL_DATABASE_INFOS,
    DatabaseConnectionFailure,
    DatabaseType,
)
from sqlalchemy import (
    create_engine,
    inspect,
)
from sqlalchemy.engine import Engine
from sqlalchemy.exc import NoSuchTableError
from sqlalchemy.ext.declarative import DeferredReflection
from sqlalchemy.orm import sessionmaker
from sqlalchemy.orm.session import Session

F = TypeVar("F", bound=Callable[..., Any])

logger = logging.getLogger("database")


class Database:
    """Database service provides access to a Microsoft SQL Server database.

    Multiple instances of this service can be created to access different databases.

    Args:
        database_type: Select which database to connect to based on master_config.
        ignore_missing_tables: Ignore DatabaseConnectionFailure in case of expected tables not existing.
    """

    def __init__(self, database_type: DatabaseType, ignore_missing_tables: bool = False) -> None:
        self._is_disabled = master_config.db_connection_attempts < 1
        self._ignore_missing_tables = ignore_missing_tables
        self._database_type = database_type
        self._database_schema = master_config.database_schema[self._database_type]
        self._optional_engine = self._initialize_engine()

    def __str__(self) -> str:
        return f"{self._database_type.name} database"

    @property
    def _engine(self) -> Engine:
        assert self._optional_engine, "Use Database.is_disabled() to check if database is available"
        return self._optional_engine

    @property
    def schema_base_class(self) -> Union[Type[InternalSchemaBase], Type[DsxReadSchemaBase], Type[DsxWriteSchemaBase]]:
        """Base class for configured database schema."""
        if self._database_type is DatabaseType.internal:
            return InternalSchemaBase
        if self._database_type is DatabaseType.dsx_read:
            return DsxReadSchemaBase
        if self._database_type is DatabaseType.dsx_write:
            return DsxWriteSchemaBase

        raise NotImplementedError(f"A base class defining tables of the {self} needs to be referenced here")

    def is_disabled(self) -> bool:
        """Check if database has been disabled."""
        return self._is_disabled

    def has_table(self, table_name: str) -> bool:
        """Check if table exists in a database."""
        return cast(bool, self._engine.has_table(table_name=table_name, schema=self._database_schema))

    def _get_connection_string(self) -> str:
        db_dsn = master_config.database_dsn[self._database_type]
        connection_string = f"DSN={db_dsn};"

        if platform.system() in {"Linux", "Darwin"} or os.getenv("ML_SQL_DOCKER_FOR_TESTING", "false") == "true":
            # For development and testing, we run SQL Server in a local Docker container with dummy user authentication.
            # Currently it looks like it is not possible to specify a plain username and password via ``odbc.ini``.
            connection_string += "UID=sa;PWD=Password1;"

        return connection_string

    def _initialize_connection(self) -> pyodbc.Connection:
        assert master_config.db_connection_attempts > 0

        connection_string = self._get_connection_string()
        for attempt in range(1, master_config.db_connection_attempts + 1):
            logger.debug(f"Trying to connect to {self}, attempt #{attempt}")
            try:
                connection = pyodbc.connect(connection_string, timeout=master_config.db_connection_timeout_seconds,)
                logger.debug(f"Successfully connected to {self}")
                return connection
            except (pyodbc.OperationalError, pyodbc.ProgrammingError, pyodbc.InterfaceError) as error:
                logger.warning(f"Unsuccessful attempt #{attempt} to connect to {self}: {error}")

                if attempt >= master_config.db_connection_attempts:
                    logger.warning(f"Unable to connect to {self} after {attempt} attempt(s).")
                    raise DatabaseConnectionFailure(str(error)) from error

                sleep_seconds = master_config.db_connection_retry_sleep_seconds
                logger.info(f"Waiting {sleep_seconds} seconds before next connection attempt")
                sleep(sleep_seconds)

    def _initialize_engine(self) -> Optional[Engine]:
        if self.is_disabled():
            logger.info(
                f"Skipping {self} connection due to db_connection_attempts=={master_config.db_connection_attempts}"
            )
            return None

        engine = create_engine(
            "mssql+pyodbc://",
            creator=self._initialize_connection,
            echo=False,  # Disable verbose logging of all SQL queries
            fast_executemany=True,  # Improve insert performance,
            # see also https://github.com/mkleehammer/pyodbc/wiki/Features-beyond-the-DB-API#fast_executemany
            pool_pre_ping=True,  # Always check status of database connections before using them,
            # see also https://docs.sqlalchemy.org/en/13/core/pooling.html#disconnect-handling-pessimistic
        )

        try:
            DeferredReflection.prepare(engine)  # type: ignore
        except NoSuchTableError as error:
            message = (
                f"Could not find expected table in {self}: {error}. "
                f"Did you run the 'setup-database {self._database_type.name}' command to setup {self}?"
            )
            if self._ignore_missing_tables:
                logger.debug(message)
            else:
                raise DatabaseConnectionFailure(message) from error

        self.schema_base_class.metadata.bind = engine

        return engine

    @contextmanager
    def transaction_context(self) -> Iterator[Session]:
        """Provide a contextmanager to execute an operation on the database within a transaction.

        Based on the SQLAlchemy recommendation:
            https://docs.sqlalchemy.org/en/13/orm/session_basics.html#session-faq-whentocreate
        """
        session_class = sessionmaker(bind=self._engine)
        session = cast(Session, session_class())

        try:
            yield session
            session.commit()  # type: ignore
        except Exception as error:
            session.rollback()  # type: ignore
            logger.error(f"Error during {self} operation, transaction was rolled-back: {error}")
            raise error
        finally:
            session.close()  # type: ignore

    def log_database_status(self) -> None:
        """Log database status, version, existing tables, and configuration."""
        if self.is_disabled():
            logger.warning(f"Connection to {self} is not initialized")
            return

        try:
            with self._engine.connect() as connection:

                for info in MS_SQL_DATABASE_INFOS:
                    logger.debug(f"{info}={connection.connection.getinfo(getattr(pyodbc, info))}")

                for info in connection.execute("SELECT @@VERSION").fetchall():
                    assert len(info) == 1
                    for line in info[0].splitlines():
                        logger.debug(line)

                existing_tables = set(self.get_existing_table_names())
                defined_tables = set(self.get_defined_table_names())
                logger.info(f"Found existing {self} tables: {existing_tables}")
                if not defined_tables.issubset(existing_tables):
                    logger.warning(f"Missing tables in {self}: {defined_tables - existing_tables}")
        except DatabaseConnectionFailure as error:
            logger.warning(f"Could not get status of {self}: {error}")

    def get_defined_table_names(self) -> List[str]:
        """List database tables, which are defined in the platform code.

        These tables are defined as subclasses of :py:attr:`~forecasting_platform.database.Database.schema_base_class`.
        """
        return list(self.schema_base_class.metadata.tables.keys())

    def get_existing_table_names(self) -> List[str]:
        """List all existing database tables, including the schema."""
        if self.is_disabled():
            logger.warning(f"Could not get existing tables, because {self} connection is not available.")
            return []

        return [
            f"{self._database_schema}.{name}" for name in inspect(self._engine).get_table_names(self._database_schema)
        ]

    def insert_data_frame(self, df: pd.DataFrame, table_name: str) -> None:
        """Insert given ``df`` to database. Inserts are done in fixed-size batches to improve stability.

        In case of an error only the currently inserted chunk will be rolled-back. Previous chunks remain in the DB.

        Args:
            df: :class:`~pandas.DataFrame` to insert to the given table of the database.
            table_name: Table of database to insert given :class:`~pandas.DataFrame`.
        """
        total_size = len(df)
        chunk_size = min(10 ** 4, total_size)

        logger.info(f"Inserting {total_size} rows to {table_name} table of {self}",)

        for counter, chunk in df.groupby(np.arange(total_size) // chunk_size):
            logger.debug(f"Inserting chunk {counter + 1} with shape {chunk.shape} to database table {table_name}")
            with self.transaction_context() as session:
                chunk.to_sql(
                    table_name, session.connection(), schema=self._database_schema, if_exists="append", index=False
                )


def retry_database_read_errors(function: F) -> F:
    """Retry a database read operation, if a :class:`~pyodbc.OperationalError` has been encountered.

    Only intended for decorating functions, that do not manipulate data.
    Retry configuration is defined in :mod:`~forecasting_platform.master_config`.

    Args:
        function: Function to decorate.

    Returns:
        Decorated function.

    """

    @wraps(function)
    def wrapper(*args, **kwargs):  # type: ignore
        retries = master_config.db_read_retries
        sleep_seconds = master_config.db_read_retry_sleep_seconds
        while retries > 0:
            try:
                return function(*args, **kwargs)
            except pyodbc.OperationalError as e:
                logger.warning(
                    f"Error '{e}' occurred while running {function.__name__}. "
                    f"Trying again {retries} more time(s) in {sleep_seconds} second(s)."
                )
                retries -= 1
                sleep(sleep_seconds)
        return function(*args, **kwargs)

    return cast(F, wrapper)
