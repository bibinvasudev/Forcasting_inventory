from __future__ import annotations

import logging
from typing import TYPE_CHECKING

from sqlalchemy import schema

logger = logging.getLogger("setup_database")

if TYPE_CHECKING:
    from forecasting_platform.services import Database


def ensure_tables_exist(database: Database) -> None:
    """Ensure tables defined in the platform code exist in the database and create them if they are missing.

    These tables are defined as subclasses of :class:`~forecasting_platform.internal_schema.InternalSchemaBase` or
    :class:`~forecasting_platform.dsx_schema.DsxWriteSchemaBase`.
    This will not update existing tables, e.g. when new columns are added.
    """
    if database.is_disabled():
        logger.error(f"Cannot setup tables, because {database} connection is not available")
        return

    logger.info(f"Previously existing tables: {database.get_existing_table_names()}")

    database.schema_base_class.metadata.create_all()

    logger.info(f"Now existing tables: {database.get_existing_table_names()}")


def ensure_schema_exists(database: Database) -> None:
    """Ensure database schema configured to be used with database exists and create it if missing."""
    if database.is_disabled():
        logger.error(f"Cannot setup schema, because {database} connection is not available")
        return

    with database.transaction_context() as session:
        connection = session.connection()
        schema_name = database._database_schema
        existing_schemas = connection.dialect.get_schema_names(connection)

        if schema_name in existing_schemas:
            logger.info(f"Found schema: {schema_name} in {database}")
            return

        logger.info(f"Creating schema: {schema_name} in {database}")
        connection.execute(schema.CreateSchema(schema_name))


def drop_known_tables(database: Database) -> None:
    """Permanently delete tables defined in the platform code and remove and all their data from database.

    These tables are defined as subclasses of :class:`~forecasting_platform.internal_schema.InternalSchemaBase` or
    :class:`~forecasting_platform.dsx_schema.DsxWriteSchemaBase`.
    This will not delete unknown tables, e.g. tables used by other programs or removed from the platform.
    """
    if database.is_disabled():
        logger.error(f"Cannot drop tables, because {database} connection is not available")
        return

    defined_table_names = database.get_defined_table_names()
    logger.info(f"Dropping own database tables: {defined_table_names}")

    previous_table_names = database.get_existing_table_names()
    logger.info(f"Previously existing tables: {previous_table_names}")

    database.schema_base_class.metadata.drop_all()

    new_table_names = database.get_existing_table_names()
    logger.info(f"Now existing tables: {new_table_names}")

    assert all(name in defined_table_names for name in set(previous_table_names) - set(new_table_names))
