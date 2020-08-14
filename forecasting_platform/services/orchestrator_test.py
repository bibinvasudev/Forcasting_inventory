import logging
from contextlib import contextmanager
from datetime import datetime
from multiprocessing.queues import Queue
from typing import (
    Dict,
    Iterator,
    List,
    NamedTuple,
    Type,
    cast,
)
from unittest.mock import (
    Mock,
    patch,
)

import pandas as pd
import pytest
from _pytest.logging import LogCaptureFixture
from _pytest.monkeypatch import MonkeyPatch
from forecasting_platform.internal_schema import (
    CleanedData,
    ForecastModelRun,
    ForecastRun,
)
from forecasting_platform.model_config_scripts import BaseModelConfig
from forecasting_platform.static import (
    DatabaseType,
    DataException,
    EngineRunType,
    ExogenousFeatures,
    ForecastRunStatus,
    InternalFeatures,
)
from forecasting_platform.test_utils import (
    delete_test_data,
    get_forecast_run,
)
from freezegun.api import FrozenDateTimeFactory
from h2o.backend import H2OConnection
from owforecasting import TimeSeries
from owforecasting.features import default_features
from sqlalchemy.orm import Query

from . import (
    DataOutput,
    RuntimeConfig,
    initialize_multiprocessing,
)
from . import orchestrator as orchestrator_module
from .data_loader import DataLoader
from .database import Database
from .initialize import SubProcessServices

TEST_FORECAST_PERIODS = 1
TEST_OUTPUT_LOCATION = "."
TEST_ACCOUNT = f'Account_{__name__.split(".")[-1]}'
TEST_CONTRACT = f'Contract_{__name__.split(".")[-1]}'


class ModelConfigAccountTestDummy(BaseModelConfig):
    MODEL_NAME = "Account_Dummy"
    CONTRACTS = [TEST_CONTRACT]
    DEFAULT_FEATURES = default_features()
    POSTPROCESS_DEPTH = 1
    TRAINING_START = pd.Timestamp(2001, 1, 1)
    WEIGHTING = 1

    def preprocess_account_data(
        self, sales_raw: pd.DataFrame, grouping: List[str], internal_features: InternalFeatures
    ) -> pd.DataFrame:
        return pd.DataFrame()

    def prepare_training_data(
        self, sales: pd.DataFrame, grouping: List[str], exo_features: ExogenousFeatures,
    ) -> TimeSeries:
        dummy_time_series = TimeSeries(
            pd.DataFrame(data={"date": [datetime.utcnow()], "response": [1]}), "date", "response"
        )
        return dummy_time_series

    def postprocess_forecast(
        self, ts: TimeSeries, ts_pred: TimeSeries, sales: pd.DataFrame, grouping: List[str]
    ) -> pd.DataFrame:
        return pd.DataFrame()


class ModelConfigAccountTestDummy2(ModelConfigAccountTestDummy):
    MODEL_NAME = "Account_Dummy2"


class OrchestratorResult(NamedTuple):
    orchestrator: orchestrator_module.Orchestrator
    runtime_config: RuntimeConfig
    result: Dict[Type[BaseModelConfig], pd.DataFrame]

    def assert_results(self, expected: List[Type[BaseModelConfig]]) -> None:
        assert len(self.result) == len(expected)
        assert expected == list(self.result.keys())
        assert all(isinstance(item, pd.DataFrame) for item in self.result.values())


def setup_orchestrator_result(use_real_database: bool = False) -> OrchestratorResult:
    result = {}

    runtime_config = RuntimeConfig(
        engine_run_type=EngineRunType.backward,
        forecast_periods=TEST_FORECAST_PERIODS,
        output_location=TEST_OUTPUT_LOCATION,
        prediction_month=pd.Timestamp(year=2020, month=2, day=1),
    )
    runtime_config.run_timestamp = "<TIME>"

    def dummy_store_forecast(
        model_config: BaseModelConfig,
        model_run: ForecastModelRun,
        account_data: pd.DataFrame,
        forecast_raw: pd.DataFrame,
        forecast_post: pd.DataFrame,
        actuals_newest_month: str,
    ) -> ForecastModelRun:
        result[model_config.__class__] = forecast_raw
        return model_run

    if use_real_database:
        internal_database = Database(DatabaseType.internal)
    else:
        internal_database = Mock(spec=Database)

    data_loader = Mock(spec=DataLoader)
    data_output = Mock(spec=DataOutput, store_forecast=dummy_store_forecast)

    log_queue = Mock(spec=Queue)

    @contextmanager
    def dummy_initialize_subprocess(_: RuntimeConfig, __: str) -> Iterator[SubProcessServices]:
        yield SubProcessServices(Mock(spec=H2OConnection), internal_database, data_loader, data_output)

    orchestrator = orchestrator_module.Orchestrator(
        runtime_config,
        data_loader,
        data_output,
        internal_database,
        log_queue,
        initialize_multiprocessing(),
        dummy_initialize_subprocess,
    )

    if not use_real_database:
        orchestrator._forecast_run_id = 42

    return OrchestratorResult(orchestrator, runtime_config, result)


def dummy_forecast_model(
    model_config: BaseModelConfig, ts: TimeSeries, data_output: DataOutput, optimize_hyperparameters: bool
) -> TimeSeries:
    return ts


def test_orchestrator_empty_list() -> None:
    orchestrator_result = setup_orchestrator_result()
    orchestrator_result.runtime_config.model_configs = []
    orchestrator_result.orchestrator.run()
    assert orchestrator_result.result == {}


def test_orchestrator_dummy_model(monkeypatch: MonkeyPatch) -> None:
    monkeypatch.setattr(orchestrator_module, "forecast_model", dummy_forecast_model)
    orchestrator_result = setup_orchestrator_result()
    orchestrator_result.runtime_config.model_configs = [ModelConfigAccountTestDummy]
    orchestrator_result.orchestrator.run()
    orchestrator_result.assert_results([ModelConfigAccountTestDummy])


def test_orchestrator_multiple_dummy_models(monkeypatch: MonkeyPatch) -> None:
    monkeypatch.setattr(orchestrator_module, "forecast_model", dummy_forecast_model)
    orchestrator_result = setup_orchestrator_result()
    orchestrator_result.runtime_config.model_configs = [ModelConfigAccountTestDummy, ModelConfigAccountTestDummy2]
    orchestrator_result.orchestrator.run()
    orchestrator_result.assert_results([ModelConfigAccountTestDummy, ModelConfigAccountTestDummy2])


@pytest.mark.freeze_time("2010-12-31-15-00")  # type: ignore
def test_orchestrator_dummy_model_updates_database_status(
    freezer: FrozenDateTimeFactory, with_cleaned_data_in_database: OrchestratorResult, monkeypatch: MonkeyPatch
) -> None:
    monkeypatch.setattr(orchestrator_module, "forecast_model", dummy_forecast_model)

    with_cleaned_data_in_database.orchestrator._import_cleaned_data = lambda: None  # type: ignore
    with_cleaned_data_in_database.runtime_config.engine_run_type = (
        EngineRunType.development
    )  # Allow incomplete cleaning run in test

    freezer.move_to("2015-12-31-15-00")

    with_cleaned_data_in_database.runtime_config.model_configs = [ModelConfigAccountTestDummy]
    with_cleaned_data_in_database.orchestrator.run()
    with_cleaned_data_in_database.assert_results([ModelConfigAccountTestDummy])

    forecast_run = get_forecast_run(with_cleaned_data_in_database.orchestrator)
    assert forecast_run
    assert forecast_run.status == ForecastRunStatus.COMPLETED
    assert forecast_run.start == datetime(2010, 12, 31, 15, 0)
    assert forecast_run.end == datetime(2015, 12, 31, 15, 0)


def _get_forecast_model_run_id(forecast_model_run: ForecastModelRun) -> int:
    with Database(DatabaseType.internal).transaction_context() as session:
        forecast_model_run = session.merge(forecast_model_run)
        return forecast_model_run.id


def test_orchestrator_dummy_model_handles_disabled_database(
    freezer: FrozenDateTimeFactory, monkeypatch: MonkeyPatch
) -> None:
    monkeypatch.setattr(orchestrator_module, "forecast_model", dummy_forecast_model)

    freezer.move_to("2010-12-31-15-00")
    orchestrator_result = setup_orchestrator_result(use_real_database=True)

    orchestrator_result.orchestrator._internal_database._is_disabled = True

    freezer.move_to("2015-12-31-15-00")
    orchestrator_result.runtime_config.model_configs = [ModelConfigAccountTestDummy]
    orchestrator_result.orchestrator.run()
    orchestrator_result.assert_results([ModelConfigAccountTestDummy])

    orchestrator_result.orchestrator._internal_database._is_disabled = False

    forecast_run = get_forecast_run(orchestrator_result.orchestrator)
    assert forecast_run is None


@pytest.mark.freeze_time("2010-12-31-15-00")  # type: ignore
def test_orchestrator_dummy_model_updates_database_status_on_keyboard_interrupt(
    freezer: FrozenDateTimeFactory, with_cleaned_data_in_database: OrchestratorResult
) -> None:
    with_cleaned_data_in_database.orchestrator._import_cleaned_data = lambda: None  # type: ignore
    with_cleaned_data_in_database.runtime_config.engine_run_type = (
        EngineRunType.development
    )  # Allow incomplete cleaning run in test

    freezer.move_to("2015-12-31-15-00")
    with_cleaned_data_in_database.runtime_config.model_configs = [ModelConfigAccountTestDummy]

    with patch("forecasting_platform.services.orchestrator.execute_models", side_effect=KeyboardInterrupt):
        with pytest.raises(KeyboardInterrupt):
            with_cleaned_data_in_database.orchestrator.run()

    forecast_run = get_forecast_run(with_cleaned_data_in_database.orchestrator)
    assert forecast_run
    assert forecast_run.status == ForecastRunStatus.CANCELLED
    assert forecast_run.start == datetime(2010, 12, 31, 15, 0)
    assert forecast_run.end == datetime(2015, 12, 31, 15, 0)


def test_orchestrator_dummy_model_handles_disabled_db_on_keyboard_interrupt(
    freezer: FrozenDateTimeFactory, caplog: LogCaptureFixture
) -> None:
    freezer.move_to("2010-12-31-15-00")
    orchestrator_result = setup_orchestrator_result(use_real_database=True)

    caplog.set_level(logging.INFO)
    orchestrator_result.orchestrator._internal_database._is_disabled = True

    freezer.move_to("2015-12-31-15-00")
    orchestrator_result.runtime_config.model_configs = [ModelConfigAccountTestDummy]

    with patch("forecasting_platform.services.orchestrator.execute_models", side_effect=KeyboardInterrupt):
        with pytest.raises(KeyboardInterrupt):
            orchestrator_result.orchestrator.run()

    orchestrator_result.orchestrator._internal_database._is_disabled = False

    forecast_run = get_forecast_run(orchestrator_result.orchestrator)
    assert forecast_run is None

    assert "Skip cancelling forecast model run in database because of disabled internal database" in caplog.messages


@pytest.fixture()  # type: ignore
def with_cleaned_data_in_database() -> Iterator[OrchestratorResult]:
    cleanup_cleaned_data_query = Query(CleanedData).filter(CleanedData.c.Contract_ID == TEST_CONTRACT)  # type: ignore
    delete_test_data(cleanup_cleaned_data_query)  # Cleanup in case of previously failed test

    with Database(DatabaseType.internal).transaction_context() as session:
        cleaned_data_count = cleanup_cleaned_data_query.with_session(session).count()
    assert cleaned_data_count == 0, "Found old test data in database when setting up the test"

    orchestrator_result = setup_orchestrator_result(use_real_database=True)
    orchestrator_result.orchestrator._initialize_forecast_run()
    test_run_id = cast(int, orchestrator_result.orchestrator._forecast_run_id)
    test_cleaned_data = [
        {
            "run_id": test_run_id,
            "Project_ID": "Test_Project",
            "Contract_ID": TEST_CONTRACT,
            "Wesco_Master_Number": "Test_Master_Number",
            "Date": "2020-03-01 00:00:00.000",
            "Date_YYYYMM": 202003,
            "Item_ID": -2,
            "Unit_Cost": 1,
            "Order_Quantity": 2,
            "Order_Cost": 2,
        }
    ]

    with Database(DatabaseType.internal).transaction_context() as session:
        session.execute(CleanedData.insert().values(test_cleaned_data))

    yield orchestrator_result

    assert delete_test_data(cleanup_cleaned_data_query) == len(test_cleaned_data)
    assert delete_test_data(Query(ForecastRun).filter(ForecastRun.id == test_run_id)) == 1  # type: ignore


def test_determine_cleaned_data_newest_month(with_cleaned_data_in_database: OrchestratorResult) -> None:
    with_cleaned_data_in_database.orchestrator._cleaned_data_run_id = (
        with_cleaned_data_in_database.orchestrator._forecast_run_id
    )
    assert with_cleaned_data_in_database.orchestrator._determine_cleaned_data_newest_month() == datetime(
        2020, 3, 1, 0, 0
    )


def test_determine_cleaned_data_newest_month_database_disabled() -> None:
    orchestrator_result = setup_orchestrator_result(use_real_database=False)
    assert orchestrator_result.orchestrator._determine_cleaned_data_newest_month() == datetime(2020, 2, 1, 0, 0)


def test_determine_cleaned_data_run_id_development_run(with_cleaned_data_in_database: OrchestratorResult) -> None:
    with_cleaned_data_in_database.orchestrator._runtime_config.engine_run_type = EngineRunType.development
    cleaned_data_run_id = with_cleaned_data_in_database.orchestrator._determine_cleaned_data_run_id()
    assert cleaned_data_run_id == with_cleaned_data_in_database.orchestrator._forecast_run_id


def test_determine_cleaned_data_run_id_invalid(with_cleaned_data_in_database: OrchestratorResult) -> None:
    with pytest.raises(DataException, match="Cannot determine valid cleaning data"):
        with_cleaned_data_in_database.orchestrator._determine_cleaned_data_run_id()
