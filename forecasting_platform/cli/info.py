import logging

import click
from forecasting_platform.services import initialize
from forecasting_platform.static import EngineRunType

logger = logging.getLogger("info")


@click.command()
def info() -> None:
    """Show database and H2O setup information (default option)."""
    with initialize(EngineRunType.development, ignore_missing_tables=True) as services:
        services.h2o_connection.cluster.show_status(detailed=True)
        services.internal_database.log_database_status()
        services.dsx_read_database.log_database_status()
        services.dsx_write_database.log_database_status()
        logger.info("Successfully connected to internal database and H2O server.")
