from __future__ import annotations

from pathlib import Path
from typing import Union

import pandas as pd
from forecasting_platform.static import (
    PREDICTION_MONTH_FORMAT,
    DataFrameStructure,
    ForecastTestParameters,
    OrchestratorResult,
)

from .forecast_structure import (
    ExpectedForecastStructureParameters,
    get_expected_forecast_structure,
)


def generate_test_id_from_test_parameters(test_parameters: ForecastTestParameters) -> str:
    """Generate human-readable name for account integration tests.

    Args:
        test_parameters: Parameters for forecast test.

    Returns:
        A human-readable string.

    """
    return str(test_parameters.model_config)


def assert_backward_forecast(backward_forecast: OrchestratorResult) -> None:
    """Assert backward forecast result in account integration tests.

    Args:
        backward_forecast: Backward forecasting result.

    """
    account_name = backward_forecast.runtime_config.model_configs[0].MODEL_NAME
    prediction_end_month = backward_forecast.runtime_config.prediction_month

    result_data = backward_forecast.forecast_result.result_data
    expected_result = pd.read_csv(Path(f"expected_results/WA_{account_name}_result_data.csv"))

    assert_backward_forecast_result(expected_result, result_data, prediction_end_month)


def assert_forward_forecast(forward_forecast: OrchestratorResult) -> None:
    """Assert development or production forecast result in account integration tests.

    Args:
        forward_forecast: Development of production forecast result.

    """
    prediction_start_month = forward_forecast.runtime_config.prediction_month
    expected_structure = get_expected_forecast_structure(
        ExpectedForecastStructureParameters(
            account_name=forward_forecast.runtime_config.model_configs[0].MODEL_NAME,  # type: ignore
            forecast_periods=forward_forecast.runtime_config.predict_periods,
            prediction_month=prediction_start_month,
        )
    )

    result_data = forward_forecast.forecast_result.result_data

    assert_same_structure(expected_structure, result_data)
    _assert_prediction_months_for_development_forecast(result_data, prediction_start_month)


def assert_backward_forecast_result(
    expected_result: pd.DataFrame, result_data: pd.DataFrame, prediction_end_month: pd.Timestamp
) -> None:
    """Assert backward forecast result in account integration tests.

    Args:
        expected_result: Expected result.
        result_data: Forecasting results.
        prediction_end_month: End month of prediction.

    """
    assert_same_structure(expected_result, result_data)
    assert_forecast_result_equal(expected_result, result_data)
    _assert_prediction_months_for_backward_forecast(result_data, prediction_end_month)


def assert_forecast_result_equal(expected: pd.DataFrame, actual: pd.DataFrame) -> None:
    """Assert expected and actual forecast results match, used in integration and end-to-end testing.

    Args:
        expected: Expected results.
        actual: Actual results.

    """
    assert_same_structure(expected, actual)
    for column in actual.columns:
        _assert_column_equal(expected, actual, column)


def assert_same_structure(expected: Union[pd.DataFrame, DataFrameStructure], actual: pd.DataFrame) -> None:
    """Assert expected and actual forecast structure matches, used in integration and end-to-end testing.

    Args:
        expected: Expected results or structure of results.
        actual: Actual results.

    """
    assert list(actual.columns) == list(expected.columns), (
        f"Column names differ:"
        f"\n{'Expected: '.ljust(60, '-')}\n{expected.columns}"
        f"\n{'Actual: '.ljust(60, '-')}\n{actual.columns}"
    )
    assert list(actual.dtypes) == list(expected.dtypes), (
        f"Column datatypes differ"
        f"\n{'Expected: '.ljust(60, '-')}\n{expected.dtypes}"
        f"\n{'Actual: '.ljust(60, '-')}\n{actual.dtypes}"
    )
    assert list(actual.shape) == list(expected.shape), (
        f"Number of columns and rows differ"
        f"\n{'Expected: '.ljust(60, '-')}\n{expected.shape}"
        f"\n{'Actual: '.ljust(60, '-')}\n{actual.shape}"
    )


def _assert_prediction_months_for_development_forecast(
    forecast_result: pd.DataFrame, prediction_start_month: pd.Timestamp = pd.Timestamp(year=2020, month=2, day=1)
) -> None:
    prediction_months = _extract_prediction_months(forecast_result)
    assert (prediction_start_month <= prediction_months).all(), (
        f"Development forecast prediction months shall be larger or equal"
        f" than prediction start month: {prediction_start_month}"
    )


def _assert_prediction_months_for_backward_forecast(
    forecast_result: pd.DataFrame, prediction_end_month: pd.Timestamp = pd.Timestamp(year=2020, month=2, day=1)
) -> None:
    prediction_months = _extract_prediction_months(forecast_result)
    assert (prediction_months <= prediction_end_month).all(), (
        f"Backward forecast prediction months shall be smaller or equal to "
        f"prediction start month: {prediction_end_month}"
    )


def _assert_column_equal(
    expected: pd.DataFrame, actual: pd.DataFrame, column: str, numerical_tolerance: float = 10 ** (-6),
) -> None:
    if pd.api.types.is_float_dtype(actual[column].dtype):
        absolute_difference = abs(expected[column] - actual[column])
        location = absolute_difference.idxmax()
        assert absolute_difference.max() < numerical_tolerance, (
            f"Difference: {absolute_difference.max()} "
            f"(using tolerance: {numerical_tolerance}) in {column}, location: {location}"
            f"\n{'Expected: '.ljust(30, '-')}\n{expected.iloc[location]}"
            f"\n{'Actual: '.ljust(30, '-')}\n{actual.iloc[location]}"
        )
    else:
        comparison_df = pd.concat([expected[column], actual[column]], axis=1, keys=["expected", "actual"])
        assert expected[column].equals(actual[column]), (
            f"Difference in {column}" f"\n{comparison_df.loc[comparison_df.expected != comparison_df.actual]}"
        )


def _extract_prediction_months(forecast_result: pd.DataFrame) -> pd.Series:
    prediction_months = forecast_result["Predicted_Month"]

    return pd.to_datetime(prediction_months, format=PREDICTION_MONTH_FORMAT)
