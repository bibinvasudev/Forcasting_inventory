from __future__ import annotations

import logging
from datetime import datetime
from multiprocessing import Queue
from multiprocessing.context import SpawnContext
from typing import (
    TYPE_CHECKING,
    Callable,
    ContextManager,
    Optional,
    Type,
    Union,
    overload,
)

from forecasting_platform.forecasting import (
    clean_input_data,
    forecast_model,
    get_grouping_columns,
    get_last_successful_cleaning_run_id,
    get_newest_cleaned_data_month,
    update_forecast_data_with_cleaned_data_sales,
    validate_input_data,
)
from forecasting_platform.internal_schema import (
    ForecastModelRun,
    ForecastRun,
)
from forecasting_platform.static import (
    EngineRunType,
    ForecastModelRunStatus,
    ForecastRunStatus,
)
from sqlalchemy import not_

from .data_loader import DataLoader
from .data_output import DataOutput
from .database import Database
from .model_executor import execute_models
from .runtime_config import RuntimeConfig

if TYPE_CHECKING:
    from forecasting_platform.model_config_scripts import BaseModelConfig
    from .initialize import SubProcessServices

logger = logging.getLogger("orchestrator")


class Orchestrator:
    """Coordinate different steps of the forecasting pipeline."""

    def __init__(
        self,
        runtime_config: RuntimeConfig,
        data_loader: DataLoader,
        data_output: DataOutput,
        internal_database: Database,
        log_queue: "Queue[logging.LogRecord]",
        multiprocessing_context: SpawnContext,
        initialize_subprocess: Callable[[RuntimeConfig, str], ContextManager[SubProcessServices]],
    ):
        self._runtime_config = runtime_config
        self._data_loader = data_loader
        self._data_output = data_output  # CAUTION: references internal_database, which cannot be serialized
        self._internal_database = internal_database  # CAUTION: internal_database cannot be serialized to sub-process
        self._log_queue = log_queue
        self._multiprocessing_context = multiprocessing_context
        self._initialize_subprocess = initialize_subprocess

        self._forecast_run = ForecastRun.create(runtime_config)
        self._forecast_run_id: Optional[int] = None
        self._cleaned_data_run_id: Optional[int] = None
        self._cleaned_data_newest_month: Optional[datetime] = None

    def _initialize_forecast_run(self) -> None:
        if self._internal_database.is_disabled():
            logger.debug(f"Skipping save of forecast run because of disabled internal database: {self._forecast_run}")
            return

        with self._internal_database.transaction_context() as session:
            session.add(self._forecast_run)
            logger.debug(f"Initializing forecast run: {self._forecast_run}")
            session.flush()
            session.refresh(self._forecast_run)
            logger.debug(f"Saved forecast run: {self._forecast_run}")
            self._forecast_run_id = self._forecast_run.id

    @staticmethod
    def _initialize_forecast_model_run(
        model_name: str, forecast_run_id: int, internal_database: Database
    ) -> ForecastModelRun:
        if internal_database.is_disabled():
            model_run = ForecastModelRun.create(forecast_run_id=forecast_run_id, model_name=model_name)
            logger.debug(f"Skipping save of model run because of disabled internal database: {model_run}")
            return model_run

        with internal_database.transaction_context() as session:
            model_run = ForecastModelRun.create(forecast_run_id=forecast_run_id, model_name=model_name)
            session.add(model_run)
            logger.debug(f"Initializing forecast model run: {model_run}")

            return model_run

    @overload
    @staticmethod
    def _update_forecast_status(
        internal_database: Database, run: ForecastRun, status: ForecastRunStatus
    ) -> ForecastRun:
        ...

    @overload
    @staticmethod
    def _update_forecast_status(
        internal_database: Database, run: ForecastModelRun, status: ForecastModelRunStatus
    ) -> ForecastModelRun:
        ...

    @staticmethod
    def _update_forecast_status(
        internal_database: Database,
        run: Union[ForecastRun, ForecastModelRun],
        status: Union[ForecastRunStatus, ForecastModelRunStatus],
    ) -> Union[ForecastRun, ForecastModelRun]:
        assert (isinstance(run, ForecastRun) and isinstance(status, ForecastRunStatus)) or (
            isinstance(run, ForecastModelRun) and isinstance(status, ForecastModelRunStatus)
        )

        if internal_database.is_disabled():
            logger.debug(f"Skipping update of forecast_run because of disabled internal database: {run}")
            return run

        with internal_database.transaction_context() as session:
            run = session.merge(run)
            logger.debug(f"Updating status to {status}: {run}")

            if run.status.is_end_state():
                logger.warning(f"Attempted invalid state transition from end state {run.status} to {status}")
                return run

            run.status = status  # type: ignore
            if status.is_end_state():
                run.end = datetime.utcnow()
        return run

    def _cancel_forecast_model_runs(self) -> None:
        if self._internal_database.is_disabled():
            logger.info("Skip cancelling forecast model run in database because of disabled internal database")
            return

        with self._internal_database.transaction_context() as session:
            (
                session.query(ForecastModelRun)  # type: ignore
                .filter(ForecastModelRun.run_id == self._forecast_run_id)
                .filter(not_(ForecastModelRun.status.in_(ForecastModelRunStatus.get_end_states())))  # type: ignore
                .update(
                    {
                        ForecastModelRun.status: ForecastModelRunStatus.CANCELLED,
                        ForecastModelRun.end: datetime.utcnow(),
                    },
                    synchronize_session=False,
                )
            )

    def run(self) -> None:
        """Run forecasts for the given model configs."""
        try:
            self._initialize_forecast_run()

            if not self._internal_database.is_disabled():
                assert self._forecast_run_id is not None, "Invalid program state: Expected forecast_run_id to be set"

            self._forecast_run = Orchestrator._update_forecast_status(
                self._internal_database, self._forecast_run, ForecastRunStatus.PREPROCESS
            )

            self._handle_generic_preprocessing()

            assert (
                self._cleaned_data_newest_month is not None
            ), "Invalid program state: Expected cleaned_data_newest_month to be defined"

            assert (
                self._cleaned_data_run_id is not None
            ), "Invalid program state: Expected cleaned_data_run_id to be defined"

            self._forecast_run = Orchestrator._update_forecast_status(
                self._internal_database, self._forecast_run, ForecastRunStatus.RUN_MODELS
            )

            execute_models(
                self._runtime_config,
                self._multiprocessing_context,
                self._log_queue,
                self._start_subprocess_run,
                (
                    self._initialize_subprocess,
                    self._forecast_run_id,
                    self._cleaned_data_run_id,
                    self._cleaned_data_newest_month,
                    self._runtime_config,
                ),
            )
            self._forecast_run = Orchestrator._update_forecast_status(
                self._internal_database, self._forecast_run, ForecastRunStatus.COMPLETED
            )
        except KeyboardInterrupt:
            self._forecast_run = Orchestrator._update_forecast_status(
                self._internal_database, self._forecast_run, ForecastRunStatus.CANCELLED
            )
            self._cancel_forecast_model_runs()
            raise
        except Exception:
            self._forecast_run = Orchestrator._update_forecast_status(
                self._internal_database, self._forecast_run, ForecastRunStatus.FAILED
            )
            raise

    def _handle_generic_preprocessing(self) -> None:
        if self._runtime_config.includes_cleaning:
            logger.info("Starting import and cleaning of DSX input data")
            self._import_exogenous_features()
            self._import_cleaned_data()
        else:
            logger.info(f"Skipping data cleaning for {self._runtime_config.engine_run_type}")

        self._set_cleaned_data_attributes()

        if self._runtime_config.includes_cleaning:
            logger.info("Updating actuals of previous forecasts in internal database with newly cleaned DSX input data")
            update_forecast_data_with_cleaned_data_sales(
                self._internal_database, self._cleaned_data_run_id, self._cleaned_data_newest_month
            )
        else:
            logger.info(f"Skipping update of previous forecasts for {self._runtime_config.engine_run_type}")

    def _set_cleaned_data_attributes(self) -> None:
        self._cleaned_data_run_id = self._determine_cleaned_data_run_id()
        self._cleaned_data_newest_month = self._determine_cleaned_data_newest_month()
        logger.info(f"Newest month in cleaned data: {self._cleaned_data_newest_month} (id={self._cleaned_data_run_id})")

    def _import_cleaned_data(self) -> None:
        raw_dsx_input = self._data_loader.load_cleaning_input_data()
        cleaning_input = validate_input_data(raw_dsx_input)
        cleaned_data = clean_input_data(cleaning_input)
        self._data_output.store_cleaned_data(cleaned_data, forecast_run_id=self._forecast_run_id)

    def _import_exogenous_features(self) -> None:
        raw_dsx_input = self._data_loader.load_exogenous_feature_input_data()
        self._data_output.store_exogenous_features(raw_dsx_input, forecast_run_id=self._forecast_run_id)

    def _determine_cleaned_data_newest_month(self) -> datetime:
        if self._internal_database.is_disabled():
            logger.warning("Internal database connection disabled. Assuming newest month is prediction_month.")
            return self._runtime_config.prediction_month.to_pydatetime()  # type: ignore
        else:
            assert self._cleaned_data_run_id is not None, "Invalid program state: cleaned_data_run_id must be set"
            return get_newest_cleaned_data_month(self._internal_database, self._cleaned_data_run_id)

    def _determine_cleaned_data_run_id(self) -> int:
        if self._internal_database.is_disabled():
            logger.warning("Internal database connection disabled. Using placeholder run_id for loading cleaned data.")
            return -1  # Dummy run_id when loading data from files
        elif self._runtime_config.includes_cleaning:
            assert (
                self._forecast_run_id is not None
            ), "Invalid program state: forecast_run_id must be defined when running with internal database"
            return self._forecast_run_id  # Take current run_id if the same run already includes cleaning
        else:  # If cleaning is NOT done within this run, determine the last successful and completed cleaning run_id.
            return get_last_successful_cleaning_run_id(self._internal_database)

    @staticmethod
    def _start_subprocess_run(
        model_config_class: Type[BaseModelConfig],
        initialize_subprocess: Callable[[RuntimeConfig, str], ContextManager[SubProcessServices]],
        forecast_run_id: int,
        cleaned_data_run_id: int,
        cleaned_data_newest_month: datetime,
        runtime_config: RuntimeConfig,
    ) -> None:
        with initialize_subprocess(runtime_config, str(model_config_class.MODEL_NAME)) as subprocess_services:
            Orchestrator._run_model_config(
                model_config_class,
                forecast_run_id,
                cleaned_data_run_id,
                cleaned_data_newest_month,
                runtime_config,
                subprocess_services.data_loader,
                subprocess_services.data_output,
                subprocess_services.internal_database,
            )

    @staticmethod
    def _run_model_config(
        model_config_class: Type[BaseModelConfig],
        forecast_run_id: int,
        cleaned_data_run_id: int,
        cleaned_data_newest_month: datetime,
        runtime_config: RuntimeConfig,
        data_loader: DataLoader,
        data_output: DataOutput,
        internal_database: Database,
    ) -> None:
        model_config = model_config_class(runtime_config, data_loader)

        if runtime_config.engine_run_type == EngineRunType.backward:
            logger.info(f'Forecasting {model_config.MODEL_NAME} to "{model_config.forecast_path}"')
        elif runtime_config.engine_run_type == EngineRunType.development:
            logger.info(f"Forecasting {model_config.MODEL_NAME} to internal database")
        elif runtime_config.engine_run_type == EngineRunType.production:
            logger.info(f"Forecasting {model_config.MODEL_NAME} to internal and DSX database")
        else:
            assert False, f"Invalid EngineRunType: {runtime_config.engine_run_type}"

        model_run = Orchestrator._initialize_forecast_model_run(
            model_config.MODEL_NAME, forecast_run_id, internal_database
        )

        try:
            model_run = Orchestrator._update_forecast_status(
                internal_database, model_run, ForecastModelRunStatus.LOAD_DATA
            )
            account_data = data_loader.load_account_data(model_config, cleaned_data_run_id)
            internal_features, exogenous_features = model_config.configure_features(cleaned_data_run_id)

            model_run = Orchestrator._update_forecast_status(
                internal_database, model_run, ForecastModelRunStatus.PREPROCESS
            )
            grouping = get_grouping_columns(model_config.GROUPING, list(internal_features.keys()))
            sales = model_config.preprocess_account_data(account_data, grouping, internal_features)

            model_run = Orchestrator._update_forecast_status(
                internal_database, model_run, ForecastModelRunStatus.PREPARE_TRAINING
            )
            time_series = model_config.prepare_training_data(sales, grouping, exogenous_features)

            model_run = Orchestrator._update_forecast_status(
                internal_database, model_run, ForecastModelRunStatus.FORECAST
            )
            forecast = forecast_model(model_config, time_series, data_output, runtime_config.optimize_hyperparameters)
            raw_forecast = forecast.result_data

            model_run = Orchestrator._update_forecast_status(
                internal_database, model_run, ForecastModelRunStatus.POSTPROCESS
            )
            postprocessed_forecast = model_config.postprocess_forecast(time_series, forecast, sales, grouping)

            model_run = data_output.store_forecast(
                model_config,
                model_run,
                account_data=account_data,
                forecast_raw=raw_forecast,
                forecast_post=postprocessed_forecast,
                actuals_newest_month=cleaned_data_newest_month,
            )

            model_run = Orchestrator._update_forecast_status(
                internal_database, model_run, ForecastModelRunStatus.COMPLETED
            )
        except Exception:
            logger.exception("Account model run failed")
            Orchestrator._update_forecast_status(internal_database, model_run, ForecastModelRunStatus.FAILED)
            raise
