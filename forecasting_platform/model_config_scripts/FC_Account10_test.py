import pandas as pd
import pytest
from forecasting_platform.helpers import (
    assert_backward_forecast,
    generate_test_id_from_test_parameters,
)
from forecasting_platform.static import (
    ForecastTestParameters,
    OrchestratorResult,
)

BACKWARD_FORECAST_PARAMETERS = ForecastTestParameters(
    model_config="ModelConfigAccount10", forecast_periods=9, prediction_month=pd.Timestamp(year=2020, month=3, day=1),
)


@pytest.mark.account
@pytest.mark.parametrize(
    "backward_forecast", [BACKWARD_FORECAST_PARAMETERS], indirect=True, ids=generate_test_id_from_test_parameters
)
class TestBackwardForecastAccount10:
    def test_result(self, backward_forecast: OrchestratorResult) -> None:
        assert_backward_forecast(backward_forecast)
