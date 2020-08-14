import logging

import click
from forecasting_platform import master_config
from forecasting_platform.dsx_write_schema import DsxWriteSchemaBase
from forecasting_platform.helpers import (
    drop_known_tables,
    ensure_schema_exists,
    ensure_tables_exist,
)
from forecasting_platform.internal_schema import InternalSchemaBase
from forecasting_platform.services import initialize
from forecasting_platform.static import (
    ConfigurationException,
    DatabaseType,
    EngineRunType,
)

logger = logging.getLogger("setup_database")


@click.group()
def setup_database() -> None:
    """Setup internal or dsx_write database."""
    pass


@setup_database.command()
@click.option(
    "--drop-tables",
    is_flag=True,
    default=False,
    show_default=True,
    help=f"Remove these tables and delete all their data from the internal database: "
    f"{list(InternalSchemaBase.metadata.tables.keys())}. Other tables will not be modified.",
)
def internal(drop_tables: bool) -> None:
    """Create internal database tables.

    .. warning::

        The "--drop-tables" option is only intended to be used for automated end-to-end testing. A future platform
        update will add an additional confirmation to prevent accidental use.

    """
    with initialize(EngineRunType.development, ignore_missing_tables=True) as services:
        services.internal_database.log_database_status()
        ensure_schema_exists(services.internal_database)

        if drop_tables:
            drop_known_tables(services.internal_database)

        ensure_tables_exist(services.internal_database)
        logger.info("Successfully set up internal database.")


@setup_database.command()
@click.option(
    "--drop-tables",
    is_flag=True,
    default=False,
    show_default=True,
    help=f"Remove these tables and delete all their data from the dsx_write database: "
    f"{list(DsxWriteSchemaBase.metadata.tables.keys())}. Other tables will not be modified.",
)
def dsx_write(drop_tables: bool) -> None:
    """Create dsx_write database tables.

    .. warning::

        The "--drop-tables" option is only intended to be used for automated end-to-end testing.
        There is a check in place to ensure this can only be used to drop tables from the internal database.
        A future platform update will add an additional confirmation to prevent accidental use.

    """
    if drop_tables and master_config.database_dsn[DatabaseType.dsx_write] != "ML_Internal_DB":
        raise ConfigurationException("Cannot drop tables on external production dsx_write database.")

    with initialize(EngineRunType.development, ignore_missing_tables=True) as services:
        services.dsx_write_database.log_database_status()
        ensure_schema_exists(services.dsx_write_database)

        if drop_tables:
            assert master_config.database_dsn[DatabaseType.dsx_write] == "ML_Internal_DB", "Can only drop internal DB"
            drop_known_tables(services.dsx_write_database)

        ensure_tables_exist(services.dsx_write_database)
        logger.info("Successfully set up dsx_write database.")
