from __future__ import annotations

from logging import getLogger
from pathlib import Path
from typing import (
    TYPE_CHECKING,
    Any,
    List,
)

import numpy as np
import pandas as pd
from forecasting_platform import master_config
from forecasting_platform.dsx_read_schema import (
    DsxExogenousFeature,
    DsxInput,
)
from forecasting_platform.forecasting.preprocess import DSX_INPUT_COLUMN_MAPPING
from forecasting_platform.helpers import (
    AccountID,
    ContractID,
)
from forecasting_platform.internal_schema import (
    CleanedData,
    ExogenousFeature,
)
from forecasting_platform.static import (
    DSX_EXOGENOUS_FEATURE_TABLE,
    DSX_INPUT_TABLE,
    DataException,
)

from .database import retry_database_read_errors

if TYPE_CHECKING:
    from .database import Database
    from forecasting_platform.model_config_scripts import BaseModelConfig

logger = getLogger("data_loader")

DSX_INPUT_DTYPES = {
    "lowlevelcust": "string",  # TODO FSC-371 Remove lowlevelcust again (should no longer be needed)
    "projectid": "string",
    "contractid": "string",
    "shortid": "int32",
    "perioddate": "object",
    "Cost": "float64",
    "Adjusted History": "float64",
    "masterpart": "string",
}

assert DSX_INPUT_COLUMN_MAPPING.keys() == DSX_INPUT_DTYPES.keys(), "Inconsistent DSX input definitions"


DSX_EXOGENOUS_DATA_DTYPES = {
    "projectid": "object",
    "contractid": "object",
    "airframe": "object",
    "periodicdatastream": "object",
    "perioddate": "object",
    "value": "float64",
}

DSX_EXOGENOUS_DATA_COLUMN_MAPPING = {
    "periodicdatastream": "Periodic_Data_Stream",
    "airframe": "Airframe",
    "contractid": "Contract_ID",
    "projectid": "Project_ID",
    "perioddate": "Date",
    "value": "Value",
}

assert (
    DSX_EXOGENOUS_DATA_COLUMN_MAPPING.keys() == DSX_EXOGENOUS_DATA_DTYPES.keys()
), "Inconsistent DSX exogenous data input definitions"


class DataLoader:
    """Load input data and files.

    Args:
        internal_database: Database to load data from.

    """

    def __init__(self, internal_database: Database, dsx_read_database: Database) -> None:
        self._internal_database = internal_database
        self._dsx_read_database = dsx_read_database

    def load_account_data(self, model_config: BaseModelConfig, cleaned_data_run_id: int) -> pd.DataFrame:
        """Load, map and aggregate data from account specific data sources.

        Data is primarily queried from the internal database. If the database connection was disabled
        via :data:`~forecasting_platform.master_config.db_connection_attempts` data will be loaded from csv file,
        from path defined in :data:`~forecasting_platform.master_config.account_processed_data_path`.

        Args:
            model_config: Account specific configuration object.
            cleaned_data_run_id: ID of the previous run that created the cleaned data set.

        Returns:
            Loaded data as :class:`pandas.DataFrame`.

        Raises:
            DataException: if data was loaded successfully but is empty.
        """
        if self._internal_database.is_disabled():
            logger.warning(
                "Internal database connection disabled in master_config. Loading account data from csv file."
            )
            account_data = self._load_account_data_from_csv(model_config)
        else:
            account_data = self._load_account_data_from_database(model_config, cleaned_data_run_id)

        if len(account_data) < 1:
            raise DataException(f"Account data for {model_config.MODEL_NAME} is empty")
        return account_data

    @retry_database_read_errors
    def _load_account_data_from_database(self, model_config: BaseModelConfig, cleaned_data_run_id: int) -> pd.DataFrame:
        with self._internal_database.transaction_context() as session:
            account_data = pd.read_sql(
                session.query(CleanedData)  # type: ignore
                .filter(CleanedData.c.run_id == cleaned_data_run_id)
                .filter(model_config.model_data_query)
                .statement,
                session.bind,
            )
            account_data.drop(columns=["run_id"], inplace=True)
            logger.info(f"Loaded account data with {len(account_data)} rows from internal database")
        return account_data

    def _load_account_data_from_csv(self, model_config: BaseModelConfig) -> pd.DataFrame:
        data_directory = Path(master_config.account_processed_data_path)
        source_paths = [data_directory / f"DSX_{contract}_Data.csv.gz" for contract in model_config.CONTRACTS]

        appended_data: List[pd.DataFrame] = []

        for data_path in source_paths:
            data = self.load_csv(data_path, parse_dates=["Date"])
            appended_data.append(data)
            logger.info(f'Loaded account data with {len(data.index)} lines from "{data_path}"',)

        account_data = pd.concat(appended_data)
        account_data["Item_ID"] = account_data["Item_ID"].astype(np.int32)
        return account_data.reset_index(drop=True)

    def load_cleaning_input_data(self) -> pd.DataFrame:
        """Load cleaning input data from DSX.

        Data is primarily queried from the DSX database. If the database connection was disabled
        via :data:`~forecasting_platform.master_config.db_connection_attempts`
        or the defined table does not exist, data will be loaded from CSV file,
        from path defined in :data:`~forecasting_platform.master_config.account_processed_data_path`.

        Returns:
            Loaded data as :class:`pandas.DataFrame`.

        Raises:
            DataException: if data was loaded successfully but is empty.
        """
        if self._dsx_read_database.is_disabled():
            logger.warning("DSX database connection disabled in master_config. Loading cleaning input from csv file.")
            raw_dsx_input = self._load_cleaning_input_data_from_csv()
        elif not self._dsx_read_database.has_table(DSX_INPUT_TABLE):
            logger.warning(
                f"Table {DSX_INPUT_TABLE} does not exist in {self._dsx_read_database}. "
                f"Loading cleaning input data from csv file."
            )
            raw_dsx_input = self._load_cleaning_input_data_from_csv()
        else:
            raw_dsx_input = self._load_cleaning_input_data_from_database()

        if len(raw_dsx_input) < 1:
            raise DataException("Loaded cleaning input data is empty")

        # Temporary code to cleanup invalid dates, until this is fixed in upcoming DSX data update.
        raw_dsx_input_len = len(raw_dsx_input)
        raw_dsx_input = raw_dsx_input.loc[raw_dsx_input["perioddate"].dt.day == 1]  # TODO FSC-318 remove this block
        if len(raw_dsx_input) < raw_dsx_input_len:
            logger.warning(f"Removed {raw_dsx_input_len - len(raw_dsx_input)} rows with invalid dates.")

        # Temporary code to filter Account_6 data, until this is done by DSX as expected.
        raw_dsx_input_len = len(raw_dsx_input)
        tmp_filter_mask = raw_dsx_input["lowlevelcust"].isin(
            [
                AccountID("Account 18"),
                AccountID("Account 293"),
                AccountID("Account 408"),
                AccountID("Account 50"),
                AccountID("Account 47"),
            ]
        )
        tmp_filter_mask &= raw_dsx_input["contractid"] == ContractID("Contract_730")
        raw_dsx_input = raw_dsx_input.loc[~tmp_filter_mask]  # TODO FSC-371 remove this block
        if len(raw_dsx_input) < raw_dsx_input_len:
            logger.warning(f"Removed {raw_dsx_input_len - len(raw_dsx_input)} unexpected rows of adhoc contract.")

        return raw_dsx_input

    def _load_cleaning_input_data_from_csv(self) -> pd.DataFrame:
        dsx_data = self.load_csv(
            Path(master_config.dsx_input_data_path),
            usecols=list(DSX_INPUT_DTYPES.keys()),
            dtype=DSX_INPUT_DTYPES,
            parse_dates=["perioddate"],
        )

        logger.info(f"Loaded cleaning input data with {len(dsx_data)} rows from {master_config.dsx_input_data_path}")
        return dsx_data

    @retry_database_read_errors
    def _load_cleaning_input_data_from_database(self) -> pd.DataFrame:
        with self._dsx_read_database.transaction_context() as session:
            dsx_data = pd.read_sql(
                session.query(DsxInput).statement,  # type: ignore
                session.bind,
                columns=list(DSX_INPUT_DTYPES.keys()),
                parse_dates=["perioddate"],
            ).astype(DSX_INPUT_DTYPES)

        logger.info(f"Loaded cleaning input data with {len(dsx_data)} rows from DSX database")
        return dsx_data[list(DSX_INPUT_DTYPES.keys())]

    def load_exogenous_feature_input_data(self) -> pd.DataFrame:
        """Load exogenous feature input data from DSX.

        Data is primarily queried from the DSX database. If the database connection was disabled
        via :data:`~forecasting_platform.master_config.db_connection_attempts`
        or the defined table does not exist, data will be loaded from the CSV file,
        configured in :data:`~forecasting_platform.master_config.dsx_exogenous_data_path`.

        Returns:
            Loaded data as :class:`pandas.DataFrame`.

        Raises:
            DataException: if data was loaded successfully but is empty.
        """
        if self._dsx_read_database.is_disabled():
            logger.warning("DSX database connection disabled in master_config. Loading cleaning input from csv file.")
            raw_dsx_input = self._load_exogenous_feature_input_from_csv()
        elif not self._dsx_read_database.has_table(DSX_EXOGENOUS_FEATURE_TABLE):
            logger.warning(
                f"Table {DSX_EXOGENOUS_FEATURE_TABLE} does not exist in {self._dsx_read_database}. "
                f"Loading exogenous feature data from csv file."
            )
            raw_dsx_input = self._load_exogenous_feature_input_from_csv()
        else:
            raw_dsx_input = self._load_exogenous_feature_input_from_database()

        if len(raw_dsx_input) < 1:
            raise DataException("Loaded exogenous feature input data is empty")

        return raw_dsx_input

    def _load_exogenous_feature_input_from_csv(self) -> pd.DataFrame:
        file_path = master_config.dsx_exogenous_data_path
        raw_exogenous_data = self.load_csv(
            Path(file_path),
            usecols=DSX_EXOGENOUS_DATA_DTYPES.keys(),
            dtype=DSX_EXOGENOUS_DATA_DTYPES,
            parse_dates=["perioddate"],
        )
        raw_exogenous_data.rename(columns=DSX_EXOGENOUS_DATA_COLUMN_MAPPING, inplace=True)

        logger.info(f"Loaded exogenous data with {len(raw_exogenous_data)} rows from {file_path}")
        return raw_exogenous_data[list(DSX_EXOGENOUS_DATA_COLUMN_MAPPING.values())]

    @retry_database_read_errors
    def _load_exogenous_feature_input_from_database(self) -> pd.DataFrame:
        with self._dsx_read_database.transaction_context() as session:
            exogenous_features = pd.read_sql(
                session.query(DsxExogenousFeature).statement,  # type: ignore
                session.bind,
                columns=list(DSX_EXOGENOUS_DATA_DTYPES.keys()),
                parse_dates=["perioddate"],
            ).astype(DSX_EXOGENOUS_DATA_DTYPES)
        exogenous_features.rename(columns=DSX_EXOGENOUS_DATA_COLUMN_MAPPING, inplace=True)

        logger.info(f"Loaded exogenous feature input data with {len(exogenous_features)} rows from DSX database")
        return exogenous_features[list(DSX_EXOGENOUS_DATA_COLUMN_MAPPING.values())]

    def load_exogenous_feature(self, feature_name: str, run_id: int) -> pd.DataFrame:
        """Load exogenous feature from internal database.

        Args:
            feature_name: Specification of data stream to load. Can be used to load build rates or airframe mapping
            run_id: ID of the run that created the cleaned data set in the internal database.

        Returns:
            Exogenous feature as :class:`pandas.DataFrame`

        Raises:
            DataException: if data was loaded successfully but is empty.
        """
        if self._internal_database.is_disabled():
            logger.warning(
                "Internal database connection disabled in master_config, loading exogenous data from CSV file"
            )
            raw_data = self._load_exogenous_feature_input_from_csv()
            exogenous_data = raw_data[raw_data["Periodic_Data_Stream"] == feature_name].reset_index(drop=True)
            logger.info(
                f'Loaded exogenous feature for Periodic_Data_Stream "{feature_name}" with {len(exogenous_data)} lines'
                f' from "{master_config.dsx_exogenous_data_path}"'
            )
        else:
            exogenous_data = self._load_exogenous_data_from_internal_database(feature_name, run_id)

        if len(exogenous_data) < 1:
            raise DataException(f'Exogenous feature data is empty for Periodic_Data_Stream "{feature_name}"')

        return exogenous_data[DSX_EXOGENOUS_DATA_COLUMN_MAPPING.values()]

    @retry_database_read_errors
    def _load_exogenous_data_from_internal_database(self, feature_name: str, run_id: int) -> pd.DataFrame:
        with self._internal_database.transaction_context() as session:
            exogenous_data = pd.read_sql(
                session.query(ExogenousFeature)  # type: ignore
                .filter(ExogenousFeature.c.run_id == run_id)
                .filter(ExogenousFeature.c.Periodic_Data_Stream == feature_name)
                .statement,
                session.bind,
            )
            exogenous_data.drop(columns=["run_id"], inplace=True)
            logger.info(f"Loaded exogenous data with {len(exogenous_data)} rows from internal database")
        return exogenous_data

    @staticmethod
    def load_csv(relative_path: Path, **kwargs: Any) -> pd.DataFrame:
        """Load data to :class:`~pandas.DataFrame` via :func:`pandas.read_csv`.

        Args:
            relative_path: Path to csv file, relative to
                :data:`~forecasting_platform.master_config.default_data_loader_location`.
            kwargs: Options passed to :func:`pandas.read_csv`

        Returns:
           Loaded CSV data.
        """
        path = Path(master_config.default_data_loader_location) / relative_path
        logger.debug(f'Loading CSV file "{relative_path}"')
        return pd.read_csv(path, **kwargs)
