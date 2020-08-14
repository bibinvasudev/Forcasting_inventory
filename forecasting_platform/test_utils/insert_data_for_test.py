from contextlib import contextmanager
from pathlib import Path
from typing import (
    Iterator,
    Type,
)

import pandas as pd
from forecasting_platform import master_config
from forecasting_platform.forecasting import SALES_COLUMNS
from forecasting_platform.internal_schema import CleanedData
from forecasting_platform.model_config_scripts import BaseModelConfig
from forecasting_platform.services import Database
from forecasting_platform.static import (
    CLEANED_DATA_TABLE,
    DatabaseType,
)
from sqlalchemy.orm import Query

from .delete_test_data import delete_test_data


@contextmanager
def insert_cleaned_data_for_database_test(
    model_config_class: Type[BaseModelConfig], test_run_id: int, disable_internal_database: bool = False
) -> Iterator[None]:
    """Insert cleaned data for integration tests by loading a matching CSV file from anonymized_data_dsx.zip.

    Parameters
    ----------
    model_config_class
        Model config class determines with file will be loaded from anonymized_data_dsx.zip
    test_run_id
        Integer ID which should uniquely identify the cleaned data for a specific test.
    disable_internal_database
        If True will skip this setup step.
    """
    if disable_internal_database:
        yield  # We can skip this function for tests that do not use the real database
        return

    # Cleanup potentially left-over data from previously failed run
    delete_test_data(Query(CleanedData).filter(CleanedData.c.run_id == test_run_id))  # type: ignore

    # Insert fresh cleaned data for this test
    for contract in model_config_class.CONTRACTS:  # type: ignore
        file_path = (
            Path(master_config.default_data_loader_location)
            / master_config.account_processed_data_path
            / f"DSX_{contract}_Data.csv.gz"
        )
        cleaned_data = pd.read_csv(file_path)
        cleaned_data = cleaned_data[SALES_COLUMNS].assign(run_id=test_run_id)
        Database(DatabaseType.internal).insert_data_frame(cleaned_data, CLEANED_DATA_TABLE)

    yield

    # Remove test data from database
    delete_test_data(Query(CleanedData).filter(CleanedData.c.run_id == test_run_id))  # type: ignore
