import pandas as pd
import pytest
from forecasting_platform.helpers import (
    assert_backward_forecast,
    assert_forward_forecast,
    generate_test_id_from_test_parameters,
)
from forecasting_platform.static import (
    ForecastTestParameters,
    OrchestratorResult,
)

BACKWARD_FORECAST_PARAMETERS = ForecastTestParameters(
    model_config="ModelConfigAccount14", forecast_periods=9, prediction_month=pd.Timestamp(year=2020, month=3, day=1),
)


@pytest.mark.account
@pytest.mark.parametrize(
    "backward_forecast", [BACKWARD_FORECAST_PARAMETERS], indirect=True, ids=generate_test_id_from_test_parameters
)
class TestBackwardForecastAccount14:
    def test_result(self, backward_forecast: OrchestratorResult) -> None:
        assert_backward_forecast(backward_forecast)


DEVELOPMENT_FORECAST_PARAMETERS = ForecastTestParameters(
    model_config="ModelConfigAccount14", forecast_periods=1, prediction_month=pd.Timestamp(year=2020, month=6, day=1),
)


@pytest.mark.account
@pytest.mark.parametrize(
    "development_forecast", [DEVELOPMENT_FORECAST_PARAMETERS], indirect=True, ids=generate_test_id_from_test_parameters,
)
class TestDevelopmentForecastAccount14:
    def test_structure(self, development_forecast: OrchestratorResult) -> None:
        assert_forward_forecast(development_forecast)
