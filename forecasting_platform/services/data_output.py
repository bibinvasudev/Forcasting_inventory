from __future__ import annotations

import json
from datetime import datetime
from logging import getLogger
from pathlib import Path
from typing import (
    TYPE_CHECKING,
    Dict,
    List,
    Optional,
)

import numpy as np
import pandas as pd
from forecasting_platform import master_config
from forecasting_platform.forecasting import compute_accuracy
from forecasting_platform.helpers import absolute_path
from forecasting_platform.internal_schema import (
    CleanedData,
    ExogenousFeature,
    ForecastModelRun,
)
from forecasting_platform.static import (
    CLEANED_DATA_TABLE,
    DSX_OUTPUT_TABLE,
    EXOGENOUS_FEATURE_TABLE,
    FORECAST_DATA_TABLE,
    PREDICT_RESULT_TYPE,
    PREDICTION_MONTH_FORMAT,
    EngineRunType,
    OutputFormat,
)

from .database import Database
from .runtime_config import RuntimeConfig

if TYPE_CHECKING:
    from forecasting_platform.model_config_scripts import BaseModelConfig

logger = getLogger("data_output")


class DataOutput:
    """Store and save output data into a database or file system."""

    def __init__(self, runtime_config: RuntimeConfig, internal_database: Database, dsx_write_database: Database):
        self._runtime_config = runtime_config
        self._internal_database = internal_database
        self._dsx_write_database = dsx_write_database

    def store_cleaned_data(self, cleaned_data: pd.DataFrame, forecast_run_id: Optional[int]) -> None:
        """Store cleaned data in the internal database.

        In case the internal database is disabled, store the cleaned data as individual files for each account.

        Args:
            cleaned_data: :class:`~pandas.DataFrame` to store.
            forecast_run_id: Unique run ID of the forecast.

        """
        if self._internal_database.is_disabled():
            self._store_cleaned_data_files(cleaned_data)
        else:
            self._store_cleaned_data_in_database(cleaned_data, forecast_run_id)

    def _store_cleaned_data_in_database(self, cleaned_data: pd.DataFrame, forecast_run_id: Optional[int]) -> None:
        assert forecast_run_id is not None, "forecast_run_id cannot be None when the internal database is enabled"
        cleaned_data = cleaned_data.assign(run_id=forecast_run_id)

        self._internal_database.insert_data_frame(cleaned_data, CLEANED_DATA_TABLE)

        with self._internal_database.transaction_context() as session:
            deleted_count = (
                session.query(CleanedData)  # type: ignore
                .filter(CleanedData.c.run_id < forecast_run_id)
                .delete(synchronize_session=False)
            )
            logger.info(f"Deleted {deleted_count} rows of outdated cleaned data from internal database.")

    def _store_cleaned_data_files(self, cleaned_data: pd.DataFrame) -> None:
        output_path = Path(master_config.default_data_loader_location) / master_config.account_processed_data_path
        output_path.mkdir(parents=True, exist_ok=True)
        logger.info(f"Storing cleaning result to local files in directory {output_path}")

        contracts = cleaned_data["Contract_ID"].unique()
        for contract in contracts:
            out = cleaned_data.loc[cleaned_data["Contract_ID"] == contract].copy()
            if len(out) > 0:
                out.to_csv(output_path / f"DSX_{contract}_Data.csv.gz", index=False)

    def store_exogenous_features(self, exogenous_feature: pd.DataFrame, forecast_run_id: Optional[int]) -> None:
        """Store exogenous feature data in the internal database.

        In case the internal database is disabled we do nothing, because we can load the existing CSV file if necessary.

        Args:
            exogenous_feature: :class:`~pandas.DataFrame` to store.
            forecast_run_id: Unique run ID of the forecast.
        """
        if self._internal_database.is_disabled():
            logger.info("Skipping storing of exogenous feature data because of disabled internal database")
            return

        assert forecast_run_id is not None, "forecast_run_id cannot be None when the internal database is enabled"
        exogenous_feature = exogenous_feature.assign(run_id=forecast_run_id)

        self._internal_database.insert_data_frame(exogenous_feature, EXOGENOUS_FEATURE_TABLE)

        with self._internal_database.transaction_context() as session:
            deleted_count = (
                session.query(ExogenousFeature)  # type: ignore
                .filter(ExogenousFeature.c.run_id < forecast_run_id)
                .delete(synchronize_session=False)
            )
            logger.info(f"Deleted {deleted_count} rows of outdated exogenous feature data from internal database.")

    def store_result(self, path: Path, df: pd.DataFrame, include_index: bool = False) -> Path:
        """Save :class:`pandas.DataFrame` to a file.

        File type, output path and extension are configured via :mod:`~forecasting_platform.master_config`.

        Args:
            path: Complete file path for saving the result, intermediate directories will be created if not existing.
            df: :class:`~pandas.DataFrame` to save.
            include_index: Include index when storing the :class:`~pandas.DataFrame`.

        Returns:
            Path were the results are stored.

        Raises:
            AttributeError: If path is an existing directory.

        """
        assert path.is_absolute(), f"Expected path to be absolute: {path}"
        DataOutput._ensure_is_file_in_existing_directory(path)

        output_format = self._runtime_config.output_format
        path_with_extension = DataOutput._change_file_extension(path, output_format.value)

        if "Actual" in df.columns:
            df["Actual"] = df["Actual"].astype("Int64")

        if self._runtime_config.engine_run_type == EngineRunType.backward:
            df = df.rename(columns={"Prediction_Start_Month": "Prediction_End_Month"})

        if output_format == OutputFormat.csv:
            df.to_csv(path_with_extension, index=include_index)
        elif output_format == OutputFormat.xlsx:
            df.to_excel(path_with_extension, index=include_index)

        logger.debug(f'Created "{path_with_extension}"')
        return path_with_extension

    @staticmethod
    def _ensure_is_file_in_existing_directory(path: Path) -> None:
        if path.is_dir():
            raise AttributeError(f"Path for saving is an existing directory! ({absolute_path(path)})")
        path.parent.mkdir(parents=True, exist_ok=True)

    @staticmethod
    def _change_file_extension(path: Path, new_extension: str) -> Path:
        return path.with_suffix(f".{new_extension}")

    def store_optimized_hyperparameters(self, directory_path: Path, hyper_params: Dict[str, object]) -> Path:
        """Store given hyper-parameters as a JSON file in the given directory.

        Args:
            directory_path: Complete file path for saving the result, intermediate directories will be created if not
                existing.
            hyper_params: Dictionary of hyper-parameters to store as JSON file.

        Returns:
            Path were the optimized hyper-parameters are stored.

        Raises:
            AttributeError: If path is an existing directory.

        """
        path = self._change_file_extension(directory_path / "optimized_hyperparameters", "json")

        self._ensure_is_file_in_existing_directory(path)

        with path.open("w") as file:
            json.dump(hyper_params, file, indent=4, sort_keys=True)

        logger.info(f"Stored optimized hyperparameters in {path}")
        return path

    def store_forecast(
        self,
        model_config: BaseModelConfig,
        model_run: ForecastModelRun,
        account_data: pd.DataFrame,
        forecast_raw: pd.DataFrame,
        forecast_post: pd.DataFrame,
        actuals_newest_month: datetime,
    ) -> ForecastModelRun:
        """Store forecast result as a file or in a database.

        Backward run forecasts are stored as a file.
        Development run forecasts are stored in internal database.
        Production run forecasts are stored in internal and DSX database.

        Args:
            account_data: Cleaned data with actual values of order quantities
            model_config: Model config instance of this forecast.
            model_run: Current model run information used to reference the stored forecast data.
            forecast_raw: Raw forecast :class:`~pandas.DataFrame` to store.
            forecast_post: Post-processed :class:`~pandas.DataFrame` to store.
            actuals_newest_month: Month of newest imported input data determines which historical order quantities can
                be considered zero.

        Returns:
            Updated forecast model run information.
        """
        forecast_data = self._convert_forecast_data(
            model_config=model_config,
            account_data=account_data,
            forecast_raw=forecast_raw,
            forecast_post=forecast_post,
            actuals_newest_month=actuals_newest_month,
        )

        forecast_file_path = self.store_result(model_config.forecast_path / "result_data", forecast_data)
        logger.info(f'Stored forecast with {len(forecast_data.index)} lines in "{forecast_file_path}"')

        if self._runtime_config.engine_run_type == EngineRunType.backward:
            logger.info("Skip storing forecast in internal database for backward run.")
            return model_run
        elif self._runtime_config.engine_run_type == EngineRunType.development:
            return self._store_forecast_in_internal_database(forecast_data, model_run)
        elif self._runtime_config.engine_run_type == EngineRunType.production:
            dsx_output_data = self._convert_dsx_output_data(
                model_config=model_config, account_data=account_data, forecast_post=forecast_post,
            )
            model_run = self._store_forecast_in_internal_database(forecast_data, model_run)
            return self._store_forecast_in_dsx_database(dsx_output_data, model_run)
        else:
            assert False, f"Invalid EngineRunType: {self._runtime_config.engine_run_type}"

    def _store_forecast_in_internal_database(
        self, forecast: pd.DataFrame, model_run: ForecastModelRun
    ) -> ForecastModelRun:
        if self._internal_database.is_disabled():
            logger.debug(f"Skip storing forecast in {FORECAST_DATA_TABLE} table because of disabled internal database")
            return model_run

        with self._internal_database.transaction_context() as session:
            model_run = session.merge(model_run)
            forecast_data = self._add_model_run_id(forecast, model_run.id)

            logger.info(f"Storing {model_run.model_name} forecast to {FORECAST_DATA_TABLE} table in internal database")

        self._internal_database.insert_data_frame(forecast_data, FORECAST_DATA_TABLE)

        return model_run

    def _store_forecast_in_dsx_database(
        self, dsx_output_data: pd.DataFrame, model_run: ForecastModelRun
    ) -> ForecastModelRun:
        if self._dsx_write_database.is_disabled() or self._internal_database.is_disabled():
            logger.debug(f"Skip storing forecast in {DSX_OUTPUT_TABLE} table because of disabled database")
            return model_run

        with self._internal_database.transaction_context() as session:
            model_run = session.merge(model_run)
            model_name = model_run.model_name

        logger.info(f"Storing {model_name} forecast to {DSX_OUTPUT_TABLE} table in dsx write database")

        self._dsx_write_database.insert_data_frame(dsx_output_data, DSX_OUTPUT_TABLE)

        return model_run

    def _convert_forecast_data(
        self,
        model_config: BaseModelConfig,
        account_data: pd.DataFrame,
        forecast_raw: pd.DataFrame,
        forecast_post: pd.DataFrame,
        actuals_newest_month: datetime,
    ) -> pd.DataFrame:
        grouping = model_config.GROUPING + ["Date"]

        def _extract_aggregated_predictions(df: pd.DataFrame) -> pd.DataFrame:
            return (
                df[df.type == PREDICT_RESULT_TYPE]
                .groupby(grouping, observed=True)["Order_Quantity"]
                .sum()
                .reset_index()
            )

        raw_prediction_df = _extract_aggregated_predictions(forecast_raw)
        post_prediction_df = _extract_aggregated_predictions(forecast_post)

        raw_prediction_df = raw_prediction_df.astype({column: "category" for column in model_config.GROUPING})
        post_prediction_df = post_prediction_df.astype({column: "category" for column in model_config.GROUPING})

        assert raw_prediction_df[grouping].equals(
            post_prediction_df[grouping]
        ), "Granularity and order of raw and post prediction results is not the same"

        forecast_data = raw_prediction_df[["Item_ID", "Contract_ID"]].copy()

        prediction_start_month = self._runtime_config.prediction_month
        predicted_month = pd.to_datetime(raw_prediction_df["Date"], format="%Y-%m-%d")
        prediction_months_delta = (predicted_month - prediction_start_month) / np.timedelta64(1, "M")

        forecast_data["Prediction_Start_Month"] = prediction_start_month.strftime(PREDICTION_MONTH_FORMAT)
        forecast_data["Predicted_Month"] = predicted_month.dt.strftime(PREDICTION_MONTH_FORMAT)
        forecast_data["Prediction_Months_Delta"] = np.round(abs(prediction_months_delta)).astype(np.int32)
        forecast_data["Prediction_Raw"] = raw_prediction_df["Order_Quantity"]
        forecast_data["Prediction_Post"] = post_prediction_df["Order_Quantity"]

        forecast_data["Actual"] = self._extract_actuals(
            account_data, forecast_data, model_config.GROUPING, actuals_newest_month
        )
        forecast_data["Accuracy"] = compute_accuracy(forecast_data)

        return forecast_data

    @staticmethod
    def _convert_dsx_output_data(
        model_config: BaseModelConfig, account_data: pd.DataFrame, forecast_post: pd.DataFrame
    ) -> pd.DataFrame:

        prediction = forecast_post.loc[(forecast_post["type"] == "predict")]
        wa_master_number = account_data[["Item_ID", "Wesco_Master_Number"]]
        wa_master_number = wa_master_number.groupby(["Item_ID"])[["Wesco_Master_Number"]].first().reset_index()
        prediction = prediction.merge(wa_master_number, on="Item_ID", how="left")

        dsx_output_data = (
            prediction.groupby(model_config.GROUPING + ["Wesco_Master_Number", "Date"], observed=True)["Order_Quantity"]
            .sum()
            .reset_index()
        )

        dsx_output_data["projectid"] = np.nan
        dsx_output_data["Ship To"] = np.nan
        dsx_output_data["binid"] = np.nan
        dsx_output_data["branch"] = np.nan
        dsx_output_data["PeriodicDataElementType"] = "Forecast"
        dsx_output_data["PeriodicDataElement"] = "Additional Forecast 1"
        dsx_output_data["NewItemFlag"] = 0
        dsx_output_data["CreatedDateTime"] = datetime.utcnow()
        dsx_output_data["Value"] = np.round(dsx_output_data["Order_Quantity"]).map(int)
        dsx_output_data["Item Name"] = dsx_output_data.agg(
            lambda row: f"Global|ContractID_Master_Part|{row['Contract_ID']}|{row['Wesco_Master_Number']}", axis=1
        )
        dsx_output_data["PeriodDate"] = pd.to_datetime(dsx_output_data["Date"], format="%Y-%m-%d").dt.strftime(
            "%Y-%m-%d %H:%M:%S.%f"
        )

        dsx_output_data = dsx_output_data.reset_index()
        dsx_output_data.rename(columns={"index": "STG_Import_Periodic_ML_RowID"}, inplace=True)

        return dsx_output_data[
            [
                "STG_Import_Periodic_ML_RowID",
                "Item Name",
                "Ship To",
                "projectid",
                "binid",
                "branch",
                "PeriodicDataElementType",
                "PeriodicDataElement",
                "PeriodDate",
                "Value",
                "NewItemFlag",
                "CreatedDateTime",
            ]
        ]

    @staticmethod
    def _add_model_run_id(df: pd.DataFrame, model_run_id: int) -> pd.DataFrame:
        assert model_run_id > 0
        df["model_run_id"] = model_run_id
        return df

    @staticmethod
    def _extract_actuals(
        account_data: pd.DataFrame, forecast_data: pd.DataFrame, grouping: List[str], actuals_newest_month: datetime
    ) -> pd.DataFrame:
        assert "Date" not in grouping, f'Unexpected grouping, did not expect "Date": {grouping}'

        df = account_data[grouping + ["Date", "Order_Quantity"]].copy()
        df["Date"] = df.Date.dt.strftime(PREDICTION_MONTH_FORMAT)

        df = df.groupby(grouping + ["Date"], observed=True)["Order_Quantity"].sum().reset_index()

        df = df.rename(columns={"Date": "Predicted_Month"})

        actuals = forecast_data.merge(df, how="left", on=grouping + ["Predicted_Month"])

        newest_month_str = actuals_newest_month.strftime(PREDICTION_MONTH_FORMAT)
        actuals.loc[
            actuals["Order_Quantity"].isna() & (actuals["Predicted_Month"] <= newest_month_str), "Order_Quantity"
        ] = 0

        return np.round(actuals["Order_Quantity"])
