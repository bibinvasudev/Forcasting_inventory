import re
from datetime import datetime
from typing import Iterator
from unittest.mock import Mock

import numpy as np
import pandas as pd
import pytest
from _pytest.logging import LogCaptureFixture
from _pytest.monkeypatch import MonkeyPatch
from click.testing import CliRunner
from forecasting_platform import master_config
from forecasting_platform.helpers import compare_structure_database as compare_structure_database_helpers
from forecasting_platform.helpers import forecast_structure
from forecasting_platform.internal_schema import (
    ForecastData,
    ForecastModelRun,
    ForecastRun,
)
from forecasting_platform.services import Database
from forecasting_platform.static import (
    DatabaseType,
    DataFrameStructure,
    EngineRunType,
    ForecastModelRunStatus,
    ForecastRunStatus,
)
from forecasting_platform.test_utils import delete_test_data
from sqlalchemy.orm import Query

from . import compare_structure_database

FIXED_TIMESTAMP = datetime(year=1970, month=1, day=1)


@pytest.fixture  # type: ignore
def prepare_database() -> Iterator[None]:
    internal_database = Database(DatabaseType.internal)
    with internal_database.transaction_context() as session:
        model_run_with_one_result_row = ForecastModelRun(
            model_name="model_run_with_one_forecast_data_row",
            start=FIXED_TIMESTAMP,
            end=FIXED_TIMESTAMP,
            status=ForecastModelRunStatus.COMPLETED,
        )
        model_run_with_zero_result_rows = ForecastModelRun(
            model_name="model_run_with_zero_forecast_data_rows",
            start=FIXED_TIMESTAMP,
            end=FIXED_TIMESTAMP,
            status=ForecastModelRunStatus.COMPLETED,
        )
        forecast_run = ForecastRun(
            start=FIXED_TIMESTAMP,
            end=FIXED_TIMESTAMP,
            run_type=EngineRunType.production,
            includes_cleaning=True,
            status=ForecastRunStatus.COMPLETED,
            forecast_periods=1,
            prediction_start_month=FIXED_TIMESTAMP,
            model_runs=[model_run_with_one_result_row, model_run_with_zero_result_rows],  # type: ignore
        )

        session.add(forecast_run)
        session.flush()
        test_model_run_id = model_run_with_one_result_row.id

        forecast_data = {
            "model_run_id": test_model_run_id,
            "Contract_ID": "test_contract_for_compare_structure_database",
            "Item_ID": -1,
            "Prediction_Start_Month": 20200101,
            "Predicted_Month": 20200101,
            "Prediction_Months_Delta": 0,
            "Prediction_Raw": 0,
            "Prediction_Post": 0,
            "Actual": None,
            "Accuracy": None,
        }

        session.execute(ForecastData.insert().values(forecast_data))

    yield

    # cleanup
    with internal_database.transaction_context() as session:
        forecast_run = session.merge(forecast_run)
        # following delete will cascade and delete also forecast_model_runs due to FK relationship
        session.delete(forecast_run)  # type: ignore
        assert (
            delete_test_data(
                Query(ForecastData).filter(ForecastData.c.model_run_id == test_model_run_id)  # type: ignore
            )
            == 1
        ), "Cleanup failed, check database for uncleaned data"


def test_compare_structure_database(
    prepare_database: Iterator[None], cli_runner: CliRunner, monkeypatch: MonkeyPatch, caplog: LogCaptureFixture
) -> None:
    def mock_get_expected_structure(
        test_parameters: forecast_structure.ExpectedForecastStructureParameters,
    ) -> DataFrameStructure:
        if test_parameters.account_name == "model_run_with_one_forecast_data_row":
            number_of_rows = 1
        elif test_parameters.account_name == "model_run_with_zero_forecast_data_rows":
            number_of_rows = 0
        else:
            assert False, "Inconsistent test setup, check test model names in fixture"
        return DataFrameStructure(
            columns=pd.Index(["0"], dtype="object"), dtypes=pd.Series([np.dtype("object")]), shape=(number_of_rows, 1),
        )

    monkeypatch.setattr(
        compare_structure_database_helpers, "get_expected_forecast_structure", mock_get_expected_structure
    )
    monkeypatch.setattr(master_config, "model_configs", ["ModelConfigAccount1", "ModelConfigAccount2"])

    # As we are always querying all values in dsx_write database, it is not easy to achieve
    # a reproducible consistent state for integration test. As a workaround the dsx outputs are not asserted here
    monkeypatch.setattr(compare_structure_database, "assert_dsx_output_count", Mock())
    monkeypatch.setattr(compare_structure_database, "assert_dsx_output_total_sum", Mock())

    result = cli_runner.invoke(compare_structure_database.compare_structure_database_command)

    assert result.exit_code == 0
    assert "Asserted number of completed model_runs (2)" in caplog.messages
    assert any(
        re.search(r"Asserted forecast data count \(1\) for model_run_with_one_forecast_data_row.*", message)
        for message in caplog.messages
    )
    assert any(
        re.search(r"Asserted forecast data count \(0\) for model_run_with_zero_forecast_data_rows.*", message)
        for message in caplog.messages
    )
    assert re.search(r"All database entries for last production run have valid structure", result.output, re.MULTILINE)


def test_compare_structure_database_if_different_number_of_forecast_data(
    prepare_database: Iterator[None], cli_runner: CliRunner, monkeypatch: MonkeyPatch, caplog: LogCaptureFixture
) -> None:
    def mock_get_expected_structure_that_fails_assertion(
        test_parameters: forecast_structure.ExpectedForecastStructureParameters,
    ) -> DataFrameStructure:
        if test_parameters.account_name == "model_run_with_one_forecast_data_row":
            number_of_rows = 42
        elif test_parameters.account_name == "model_run_with_zero_forecast_data_rows":
            number_of_rows = 42
        else:
            assert False, "Inconsistent test setup, check test model names in fixture"
        return DataFrameStructure(
            columns=pd.Index(["0"], dtype="object"), dtypes=pd.Series([np.dtype("object")]), shape=(number_of_rows, 1),
        )

    monkeypatch.setattr(
        compare_structure_database_helpers,
        "get_expected_forecast_structure",
        mock_get_expected_structure_that_fails_assertion,
    )
    monkeypatch.setattr(master_config, "model_configs", ["ModelConfigAccount1", "ModelConfigAccount2"])

    result = cli_runner.invoke(compare_structure_database.compare_structure_database_command)

    expected_error_message = (
        r"Forecast data count \(1\) for model_run_with_one_forecast_data_row"
        r" \(model_run=\d*\) does not match the expectation \(42\) defined by"
        r" ExpectedForecastStructureParameters.*"
    )

    assert result.exit_code == 1
    assert any((re.search(expected_error_message, message,) for message in caplog.messages))
    assert re.search(expected_error_message, result.output, re.MULTILINE)


def test_compare_structure_database_if_different_number_of_model_runs(
    prepare_database: Iterator[None], cli_runner: CliRunner, monkeypatch: MonkeyPatch, caplog: LogCaptureFixture
) -> None:

    monkeypatch.setattr(master_config, "model_configs", ["ModelConfigAccount1", "ModelConfigAccount2"])
    monkeypatch.setattr(
        compare_structure_database, "get_model_run_ids_for_forecast_run", Mock(return_value=["ModelConfigAccount1"])
    )

    result = cli_runner.invoke(compare_structure_database.compare_structure_database_command)

    assert result.exit_code == 1
    assert (
        "Number of completed model runs (1) does not match the number of model_configs in master_config.py (2)"
        in caplog.messages
    )
    assert re.search(
        r"Number of completed model runs \(1\) does not match the number of model_configs in master_config.py \(2\)",
        result.output,
        re.MULTILINE,
    )


def test_compare_structure_database_if_no_production_run(
    cli_runner: CliRunner, monkeypatch: MonkeyPatch, caplog: LogCaptureFixture
) -> None:
    monkeypatch.setattr(compare_structure_database, "get_last_successful_production_run", Mock(return_value=None))

    result = cli_runner.invoke(compare_structure_database.compare_structure_database_command)

    expected_error_message = "Could not determine last successful production run"
    assert result.exit_code == 1
    assert expected_error_message in caplog.messages
    assert re.search(expected_error_message, result.output, re.MULTILINE)


def test_compare_structure_database_if_no_database_available(
    cli_runner: CliRunner, monkeypatch: MonkeyPatch, caplog: LogCaptureFixture
) -> None:
    monkeypatch.setattr(master_config, "db_connection_attempts", 0)
    monkeypatch.setattr(compare_structure_database, "get_last_successful_production_run", Mock(return_value=None))

    result = cli_runner.invoke(compare_structure_database.compare_structure_database_command)

    expected_error_message = "Cannot validate database tables, because database connection is not available"
    assert result.exit_code == 1
    assert expected_error_message in caplog.messages
    assert re.search(expected_error_message, result.output, re.MULTILINE)
