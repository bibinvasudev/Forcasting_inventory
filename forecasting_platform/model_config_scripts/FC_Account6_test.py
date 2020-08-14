from pathlib import Path

import pandas as pd
import pytest
from forecasting_platform.helpers import (
    assert_backward_forecast_result,
    assert_forward_forecast,
    generate_test_id_from_test_parameters,
)
from forecasting_platform.static import (
    ForecastTestParameters,
    OrchestratorResult,
)


def reduce_input_data(sales_raw: pd.DataFrame) -> pd.DataFrame:
    """Account with full input data is very slow to run, therefore we reduce the input for integration tests."""
    return sales_raw[
        sales_raw["Item_ID"].isin(
            [
                7084,
                31104,
                72181,
                227208,
                244491,
                244497,
                245721,
                245751,
                263987,
                275143,
                275347,
                331095,
                333735,
                334079,
                381337,
                393446,
                911365,
                930282,
                941310,
                941468,
                948527,
                1001011,
                1254168,
                1269703,
                1341246,
                1394909,
                1411044,
                1433703,
                1438263,
                1440944,
            ]
        )
    ]


BACKWARD_FORECAST_PARAMETERS = ForecastTestParameters(
    model_config="ModelConfigAccount6",
    forecast_periods=2,
    prediction_month=pd.Timestamp(year=2020, month=1, day=1),
    input_filter=reduce_input_data,
)


@pytest.mark.account
@pytest.mark.parametrize(
    "backward_forecast", [BACKWARD_FORECAST_PARAMETERS], indirect=True, ids=generate_test_id_from_test_parameters
)
class TestBackwardForecastAccount6:
    def test_result(self, backward_forecast: OrchestratorResult) -> None:
        expected_result = pd.read_csv(Path("expected_results/WA_Account_6_result_data_reduced.csv"))
        result_data = backward_forecast.forecast_result.result_data
        prediction_month = backward_forecast.runtime_config.prediction_month

        assert_backward_forecast_result(expected_result, result_data, prediction_month)


def reduce_input_data_development_forecast(sales_raw: pd.DataFrame) -> pd.DataFrame:
    """Account 6 with full input data is very slow to run, therefore we reduce the input for integration tests."""
    return sales_raw.head(10000)


DEVELOPMENT_FORECAST_PARAMETERS = ForecastTestParameters(
    model_config="ModelConfigAccount6",
    forecast_periods=1,
    prediction_month=pd.Timestamp(year=2020, month=6, day=1),
    input_filter=reduce_input_data_development_forecast,
)


@pytest.mark.account
@pytest.mark.parametrize(
    "development_forecast", [DEVELOPMENT_FORECAST_PARAMETERS], indirect=True, ids=generate_test_id_from_test_parameters,
)
class TestDevelopmentForecastAccount6:
    def test_structure(self, development_forecast: OrchestratorResult) -> None:
        assert_forward_forecast(development_forecast)
