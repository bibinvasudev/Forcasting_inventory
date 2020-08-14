"""The ``master_config`` script contains static configuration and default values used in all environments."""
import logging
from typing import List

import pandas as pd

from .static import (
    DatabaseType,
    OutputFormat,
)

#: List of H2O server URLs to connect to, first successful connection will be used.
h2o_urls = [
    "http://localhost:54321",
    "http://h2o-server:54321",
]

#: If none of the connections to :py:attr:`h2o_urls` are successful,
#: a local H2O server will by started on this fallback port
fallback_h2o_port: int = 55555
#: If local H2O server is started, use this as the maximum memory limit for the H2O server.
fallback_h2o_max_mem_size_GB: int = 24

#: The Data Source Name (DSN) of the databases used by the platform.
#: This needs to match the configuration of the Operating System running the platform.
database_dsn = {
    DatabaseType.internal: "ML_Internal_DB",
    DatabaseType.dsx_read: "ML_Internal_DB",
    DatabaseType.dsx_write: "ML_Internal_DB",
}
#: Configuration of database schemas used by the platform.
database_schema = {
    DatabaseType.internal: "ml_internal",
    DatabaseType.dsx_read: "ml_dsx_read",
    DatabaseType.dsx_write: "ml_dsx_write",
}

#: Maximum number of database connection attempts in case of connection failure.
#: Set to ``0`` to disable database access.
db_connection_attempts = 5
#: Database connection timeout for each connection attempt.
db_connection_timeout_seconds = 3
#: Seconds to wait after a failed database connection attempt before trying again.
db_connection_retry_sleep_seconds = 3
#: Maximum number of database read retries in case of an :class:`~pyodbc.OperationalError`.
#: Set to ``0`` disable retries i.e only one read attempt will be performed.
db_read_retries = 2
#: Seconds to wait after a failed database read attempt before trying again.
db_read_retry_sleep_seconds = 1

# Forecasting platform default configuration
#: Default number of months to predict.
default_forecast_periods = 13
#: Default location for loading files with :class:`~forecasting_platform.services.DataLoader`.
default_data_loader_location = "."
#: Default output location for storing forecast result files with :class:`~forecasting_platform.services.DataOutput`.
default_output_location = "."
#: Default output format for storing forecast result files with :class:`~forecasting_platform.services.DataOutput`.
default_output_format = OutputFormat.csv
#: Default prediction start is the current month.
default_prediction_start_month = pd.Timestamp.today().replace(day=1)

#: Logging output directory name.
log_output_location = "logs"
#: Log level for file logging.
log_level_file = logging.DEBUG
#: Log level for console logging.
log_level_console = logging.INFO
#: Logging format as defined in :class:`~logging.Formatter`.
logging_format = "%(asctime)s::%(levelname)s::%(name)s::%(account)s::%(message)s"

#: Logging timestamp format as defined in :class:`~logging.Formatter`.
logging_timestamp_format = "%Y-%m-%d %H:%M:%S"

#: Default location for loading DSX input data with :class:`~forecasting_platform.services.DataLoader`.
dsx_input_data_path = "01 Raw data/DSX_anonymized_input.csv.gz"
#: Default location for loading DSX input data for exogenous features.
dsx_exogenous_data_path = "01 Raw data/DSX_exogenous_features.csv"
#: Default month for loading processed account data with :class:`~forecasting_platform.services.DataLoader`.
account_processed_data_month = "202002"
#: Default location for loading processed account data with :class:`~forecasting_platform.services.DataLoader`.
account_processed_data_path = f"03 Processed data/monthly_process/{account_processed_data_month}/anonymized_data_dsx"

#: Number of models to process in parallel.
max_parallel_models = 8

#: List of model configurations to forecast.
#: To enable efficient parallelism of the platform, make sure to put long running accounts at the beginning of the list.
model_configs: List[str] = [
    "ModelConfigAccount1",
    "ModelConfigAccount2",
    "ModelConfigAccount3",
    "ModelConfigAccount5",
    "ModelConfigAccount6",
    "ModelConfigAccount7",
    "ModelConfigAccount8",
    "ModelConfigAccount9",
    "ModelConfigAccount10",
    "ModelConfigAccount11",
    "ModelConfigAccount12",
    "ModelConfigAccount13",
    "ModelConfigAccount14",
    "ModelConfigAccount15",
    "ModelConfigAccount16",
    "ModelConfigAccount17",
    "ModelConfigAccount20",
    "ModelConfigAccount29",
    "ModelConfigAccount38",
    "ModelConfigAccount39",
    "ModelConfigAccount44",
    "ModelConfigAccount45",
    "ModelConfigAccount54",
    "ModelConfigAccount466",
    "ModelConfigAccount468",
]
