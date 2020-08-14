"""Pytest configuration file.

Documentation: https://docs.pytest.org/en/latest/writing_plugins.html#conftest-py-plugins
"""
import logging
from contextlib import contextmanager
from multiprocessing.queues import Queue
from pathlib import Path
from random import randint
from typing import (
    Any,
    Callable,
    Iterator,
    Optional,
    cast,
)
from unittest.mock import (
    Mock,
    patch,
)

import forecasting_platform.model_config_scripts
import pandas as pd
import pytest
from _pytest.monkeypatch import MonkeyPatch
from _pytest.tmpdir import TempPathFactory
from click.testing import CliRunner
from forecasting_platform import master_config
from h2o.backend import H2OConnection

from .internal_schema import ForecastData
from .model_config_scripts import BaseModelConfig
from .services import (
    Database,
    DataLoader,
    DataOutput,
    Orchestrator,
    RuntimeConfig,
    SubProcessServices,
    initialize_h2o_connection,
    initialize_multiprocessing,
    initialize_random_seed,
)
from .services import logging as forecasting_platform_logging
from .services.logging import _configure_logging
from .static import (
    DatabaseType,
    EngineRunType,
    ForecastModelRunStatus,
    ForecastRunStatus,
    ForecastTestResult,
    OrchestratorResult,
)
from .test_utils import insert_cleaned_data_for_database_test


@pytest.fixture(scope="session")  # type: ignore
def monkeypatch_session() -> Iterator[MonkeyPatch]:
    """Monkeypatch that can be used from other session-scoped pytest fixtures.

    See: https://github.com/pytest-dev/pytest/issues/363
    """
    patch = MonkeyPatch()
    yield patch
    patch.undo()


@pytest.fixture(scope="class")  # type: ignore
def h2o_connection() -> Iterator[H2OConnection]:
    """Provide fresh H2O connection for each test class."""
    connection = initialize_h2o_connection(master_config.h2o_urls, master_config.fallback_h2o_port)
    yield connection
    connection.close()


@pytest.fixture  # type: ignore
def cli_runner() -> CliRunner:
    """Provide CliRunner that can be used to test CLI commands."""
    return CliRunner()


@pytest.fixture(scope="class", autouse=True)  # type: ignore
def patch_default_output_location(monkeypatch_session: MonkeyPatch, tmp_path_factory: TempPathFactory) -> None:
    """Patch output location by default to avoid tests writing into the repository during development."""
    tmp_path = tmp_path_factory.mktemp("test_forecasting_platform")
    print(f"Using temporary directory for default_output_location: {tmp_path}")
    monkeypatch_session.setattr(master_config, "default_output_location", tmp_path)


@pytest.fixture(scope="class", autouse=True)  # type: ignore
def patch_default_dsx_exogenous_data_path(monkeypatch_session: MonkeyPatch) -> None:
    """Patch path to ``DSX_exogenous_features.csv`` for all unit and integration tests."""
    monkeypatch_session.setattr(master_config, "dsx_exogenous_data_path", "01 Raw data/DSX_exogenous_features.csv")


@pytest.fixture(scope="session", autouse=True)  # type: ignore
def patch_multiprocessing(monkeypatch_session: MonkeyPatch) -> None:
    """Patch master_config to disable multiprocessing during pytest run to avoid issues with mocks and memory usage."""
    monkeypatch_session.setattr(master_config, "max_parallel_models", 1)


@pytest.fixture(autouse=True)  # type: ignore
def patch_configure_logging(monkeypatch: MonkeyPatch, tmp_path_factory: TempPathFactory, request: Any) -> None:
    """Use basic test log config to allow pytest ``caplog`` features to be used.

    Production log config can be enabled with the ``@pytest.mark.testlogging`` decorator.
    """
    logging_unit_test = request.node.get_closest_marker("testlogging")
    if logging_unit_test:
        tmp_path = tmp_path_factory.mktemp("test_forecasting_platform")
        print(f"Using temporary directory for log_output_location: {tmp_path}")
        monkeypatch.setattr(master_config, "log_output_location", tmp_path)
        return

    def configure_logging_for_tests(*args: Any) -> None:
        logger = logging.getLogger()
        logger.setLevel(logging.NOTSET)

    print("Setting logging to log all levels. Disabling logging to log files.")
    monkeypatch.setattr(forecasting_platform_logging, "_configure_logging", configure_logging_for_tests)


@pytest.fixture(scope="class")  # type: ignore
def backward_forecast(h2o_connection: H2OConnection, request: Any) -> OrchestratorResult:
    """Trigger backward forecast integration test."""
    return _run_orchestrator(
        engine_run_type=EngineRunType.backward,
        h2o_connection=h2o_connection,
        model_config=request.param.model_config,
        forecast_periods=request.param.forecast_periods,
        prediction_month=request.param.prediction_month,
        input_filter=request.param.input_filter,
        disable_database=request.param.disable_database,
    )


@pytest.fixture(scope="class")  # type: ignore
def development_forecast(h2o_connection: H2OConnection, request: Any) -> OrchestratorResult:
    """Trigger development forecast integration test."""
    return _run_orchestrator(
        engine_run_type=EngineRunType.development,
        h2o_connection=h2o_connection,
        model_config=request.param.model_config,
        forecast_periods=request.param.forecast_periods,
        prediction_month=request.param.prediction_month,
        input_filter=request.param.input_filter,
        disable_database=request.param.disable_database,
    )


@pytest.fixture(scope="class")  # type: ignore
def production_forecast(h2o_connection: H2OConnection, request: Any) -> OrchestratorResult:
    """Trigger development forecast integration test."""
    return _run_orchestrator(
        engine_run_type=EngineRunType.production,
        h2o_connection=h2o_connection,
        model_config=request.param.model_config,
        forecast_periods=request.param.forecast_periods,
        prediction_month=request.param.prediction_month,
        input_filter=request.param.input_filter,
        disable_database=request.param.disable_database,
    )


def _run_orchestrator(
    engine_run_type: EngineRunType,
    h2o_connection: H2OConnection,
    model_config: str,
    forecast_periods: int,
    prediction_month: pd.Timestamp,
    disable_database: bool,
    input_filter: Optional[Callable[[pd.DataFrame], pd.DataFrame]],
) -> OrchestratorResult:

    _configure_logging(engine_run_type.value)

    runtime_config = RuntimeConfig(
        engine_run_type,
        forecast_periods=forecast_periods,
        prediction_month=prediction_month,
        only_model_config=model_config,
    )

    db_connection_attempts = 0 if disable_database else master_config.db_connection_attempts
    with patch.object(master_config, "db_connection_attempts", db_connection_attempts):
        internal_database = Database(DatabaseType.internal)
        dsx_read_database = Database(DatabaseType.dsx_read)
        dsx_write_database = Database(DatabaseType.dsx_write)

    data_loader = DataLoader(internal_database, dsx_read_database)

    def wrapped_load_account_data(model_config: BaseModelConfig, cleaned_data_run_id: int) -> pd.DataFrame:
        data = DataLoader.load_account_data(data_loader, model_config, cleaned_data_run_id)
        if input_filter:
            return input_filter(data)
        return data

    data_loader.load_account_data = wrapped_load_account_data  # type: ignore

    data_output = DataOutput(
        runtime_config=runtime_config, internal_database=internal_database, dsx_write_database=dsx_write_database
    )

    forecast_result_path: Optional[Path] = None

    def wrapped_store_result(path: Path, df: pd.DataFrame, include_index: bool = False) -> Path:
        nonlocal forecast_result_path

        forecast_result_path = DataOutput.store_result(data_output, path, df, include_index)

        return forecast_result_path

    data_output.store_result = wrapped_store_result  # type: ignore

    multiprocessing_context = initialize_multiprocessing()
    log_queue = Mock(spec=Queue)

    @contextmanager
    def dummy_initialize_subprocess(_: RuntimeConfig, __: str) -> Iterator[SubProcessServices]:
        yield SubProcessServices(h2o_connection, internal_database, data_loader, data_output)

    test_cleaned_data_id = randint(10000, 99999)  # Get unique id for this test to avoid collisions with parallel runs

    initialize_random_seed()  # Post-processing uses randomness
    orchestrator = Orchestrator(
        runtime_config,
        data_loader,
        data_output,
        internal_database,
        log_queue,
        multiprocessing_context,
        dummy_initialize_subprocess,
    )

    orchestrator._import_cleaned_data = lambda: None  # type: ignore
    orchestrator._determine_cleaned_data_run_id = lambda: test_cleaned_data_id  # type: ignore

    assert len(runtime_config.model_configs) == 1, "Only single model config is supported by this test setup."
    with insert_cleaned_data_for_database_test(runtime_config.model_configs[0], test_cleaned_data_id, disable_database):
        orchestrator.run()

    model_run_id = None
    if not disable_database:
        model_run_id = _assert_internal_database_after_run(orchestrator, internal_database, model_config)

    forecast_test_result = ForecastTestResult(model_run_id=model_run_id, result_data=pd.read_csv(forecast_result_path))

    return OrchestratorResult(runtime_config=runtime_config, forecast_result=forecast_test_result)


def _assert_internal_database_after_run(
    orchestrator: Orchestrator, internal_database: Database, model_config: str,
) -> int:
    with internal_database.transaction_context() as session:
        run = session.merge(orchestrator._forecast_run)
        session.refresh(run)
        assert run.id is not None
        assert run.end > run.start
        assert run.status is ForecastRunStatus.COMPLETED

        assert len(run.model_runs) == 1

        model_run = run.model_runs[0]
        assert model_run.id is not None
        assert model_run.run_id == run.id
        model_config_class = forecasting_platform.model_config_scripts.__dict__[model_config]
        assert model_run.model_name == model_config_class.MODEL_NAME
        assert model_run.end > model_run.start
        assert model_run.status is ForecastModelRunStatus.COMPLETED

        total_rows = (
            session.query(ForecastData)  # type: ignore
            .filter(ForecastData.c.model_run_id == model_run.id)
            .count()
        )

        assert total_rows > 0

        return cast(int, model_run.id)
