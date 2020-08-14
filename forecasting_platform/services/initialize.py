from __future__ import annotations

import logging
from contextlib import contextmanager
from typing import (
    TYPE_CHECKING,
    Iterator,
    NamedTuple,
    Optional,
    Type,
    Union,
)

import click
import pandas as pd
from forecasting_platform import master_config
from forecasting_platform.static import (
    DatabaseType,
    EngineRunType,
    OutputFormat,
)
from h2o.backend import H2OConnection

from .data_loader import DataLoader
from .data_output import DataOutput
from .database import Database
from .h2o import initialize_h2o_connection
from .logging import (
    initialize_logging,
    reset_logging_context,
    set_logging_context,
)
from .multiprocessing import initialize_multiprocessing
from .orchestrator import Orchestrator
from .random_seed import initialize_random_seed
from .runtime_config import RuntimeConfig
from .signal_handler import initialize_faulthandler
from .tempdir import initialize_tempdir
from .warnings import initialize_warnings

logger = logging.getLogger("initialize")

if TYPE_CHECKING:
    from forecasting_platform.model_config_scripts import BaseModelConfig


class Services(NamedTuple):
    """Provide references to the currently valid services for this run."""

    h2o_connection: H2OConnection  #: Current :py:class:`h2o.backend.H2OConnection` instance
    internal_database: Database  #: Current internal :py:class:`Database` instance
    dsx_read_database: Database  #: Current dsx_read :py:class:`Database` instance
    dsx_write_database: Database  #: Current dsx_write :py:class:`Database` instance
    orchestrator: Orchestrator  #: Current :py:class:`Orchestrator` instance
    runtime_config: RuntimeConfig  #: Current :py:class:`RuntimeConfig` instance
    data_loader: DataLoader  #: Current :py:class:`DataLoader` instance


class SubProcessServices(NamedTuple):
    """Provide references to the currently valid services for this sub-process."""

    h2o_connection: H2OConnection  #: Current :py:class:`h2o.backend.H2OConnection` instance
    internal_database: Database  #: Current internal :py:class:`Database` instance
    data_loader: DataLoader  #: Current :py:class:`DataLoader` instance
    data_output: DataOutput  #: Current :py:class:`DataOutput` instance


@contextmanager
def initialize(
    engine_run_type: EngineRunType,
    forecast_periods: int = master_config.default_forecast_periods,
    output_location: str = master_config.default_output_location,
    prediction_start_month: pd.Timestamp = master_config.default_prediction_start_month,
    output_format: OutputFormat = master_config.default_output_format,
    optimize_hyperparameters: bool = False,
    force_reload: bool = False,
    only_model_config: Optional[Union[Type[BaseModelConfig], str]] = None,
    exclude_model_config: Optional[str] = None,
    ignore_missing_tables: bool = False,
) -> Iterator[Services]:
    """Initialize and provide :class:`Services` context.

    Args:
        engine_run_type: Type of engine run (i.e. backward or forward) and type of data output (file vs. database).
        forecast_periods: Number of periods to predict.
        output_location: Location to write the outputs to.
        prediction_start_month: Number to start the prediction (forward) or end the prediction (backward).
        output_format: File format for outputting files.
        optimize_hyperparameters: Activate hyper-parameter optimization during training.
        force_reload: Force re-loading of data from DSX.
        only_model_config: Only run forecast for given model config.
        exclude_model_config: Exclude given model config from forecasting.
        ignore_missing_tables: Ignore missing table (warning instead of error).

    Returns:
        Initialized services for the current forecasting run, valid within the :func:`~contextlib.contextmanager`.

    """
    initialize_tempdir()
    initialize_faulthandler()
    initialize_random_seed()
    initialize_warnings()

    multiprocessing_context = initialize_multiprocessing()
    log_queue = multiprocessing_context.Queue(-1)

    click_context = click.get_current_context(silent=True)
    command_name = str(click_context.info_name) if click_context else engine_run_type.value

    logging_cleanup = initialize_logging(command_name, log_queue)

    if click_context:
        logger.debug(f"Invoked cli command '{click_context.info_name}' with parameters {click_context.params}")

    runtime_config = RuntimeConfig(
        engine_run_type=engine_run_type,
        forecast_periods=forecast_periods,
        output_location=output_location,
        prediction_month=prediction_start_month,
        output_format=output_format,
        optimize_hyperparameters=optimize_hyperparameters,
        force_reload=force_reload,
        only_model_config=only_model_config,
        exclude_model_config=exclude_model_config,
    )
    h2o_connection = initialize_h2o_connection(master_config.h2o_urls, master_config.fallback_h2o_port)
    internal_database = Database(DatabaseType.internal, ignore_missing_tables)
    dsx_read_database = Database(DatabaseType.dsx_read, ignore_missing_tables)
    dsx_write_database = Database(DatabaseType.dsx_write, ignore_missing_tables)
    data_loader = DataLoader(internal_database, dsx_read_database)
    data_output = DataOutput(runtime_config, internal_database, dsx_write_database)
    orchestrator = Orchestrator(
        runtime_config,
        data_loader,
        data_output,
        internal_database,
        log_queue,
        multiprocessing_context,
        initialize_subprocess,
    )

    logger.debug("Services initialized successfully")

    try:
        yield Services(
            h2o_connection,
            internal_database,
            dsx_read_database,
            dsx_write_database,
            orchestrator,
            runtime_config,
            data_loader,
        )
    finally:
        h2o_connection.close()
        logging_cleanup()


_subprocess_services: Optional[SubProcessServices] = None


@contextmanager
def initialize_subprocess(runtime_config: RuntimeConfig, log_context: str) -> Iterator[SubProcessServices]:
    """Initialize a new sub-process.

    It is safe to call this multiple times within the same process, while avoiding re-initialization issues.

    Args:
        runtime_config: Config of sub-process to start.
        log_context: Logging context for the sub-process.

    Returns:
        Initialized sub-process services, valid within the :func:`~contextlib.contextmanager` for each sub-process.

    """
    global _subprocess_services
    try:
        set_logging_context(log_context)

        if _subprocess_services:
            yield _subprocess_services
            return

        initialize_tempdir()
        initialize_faulthandler()
        initialize_random_seed()
        initialize_warnings()

        # H2O needs to be initialized for each process, because the connection pool cannot be serialized.
        h2o_connection = initialize_h2o_connection(master_config.h2o_urls, master_config.fallback_h2o_port)

        # Database connections have to be initialized for each process, because the connection pool cannot be serialized
        # See: https://docs.sqlalchemy.org/en/13/core/pooling.html#using-connection-pools-with-multiprocessing
        internal_database = Database(DatabaseType.internal)
        dsx_read_database = Database(DatabaseType.dsx_read)
        dsx_write_database = Database(DatabaseType.dsx_write)
        data_output = DataOutput(runtime_config, internal_database, dsx_write_database)
        data_loader = DataLoader(internal_database, dsx_read_database)

        logger.debug("Sub-process services initialized successfully")

        _subprocess_services = SubProcessServices(h2o_connection, internal_database, data_loader, data_output)
        yield _subprocess_services
    finally:
        reset_logging_context()
