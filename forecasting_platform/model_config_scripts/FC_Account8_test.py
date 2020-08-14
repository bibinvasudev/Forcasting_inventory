from typing import List

import pandas as pd
import pytest
from forecasting_platform.helpers import (
    assert_backward_forecast,
    generate_test_id_from_test_parameters,
)
from forecasting_platform.helpers.identifier import ProjectID
from forecasting_platform.static import (
    ForecastTestParameters,
    OrchestratorResult,
)

from .FC_Account8 import ModelConfigAccount8

BACKWARD_FORECAST_PARAMETERS = ForecastTestParameters(
    model_config="ModelConfigAccount8", forecast_periods=9, prediction_month=pd.Timestamp(year=2020, month=3, day=1),
)


@pytest.mark.account
@pytest.mark.parametrize(
    "backward_forecast", [BACKWARD_FORECAST_PARAMETERS], indirect=True, ids=generate_test_id_from_test_parameters
)
class TestBackwardForecastAccount8:
    def test_result(self, backward_forecast: OrchestratorResult) -> None:
        assert_backward_forecast(backward_forecast)


@pytest.mark.parametrize(
    "old_project_ids, expected_project_ids",
    [
        ([ProjectID("Project_365")], [ProjectID("Project_364")],),
        (
            [ProjectID("Project_365"), "do_not_replace_this_project_id"],
            [ProjectID("Project_364"), "do_not_replace_this_project_id"],
        ),
        (["do_not_replace_this_project_id"], ["do_not_replace_this_project_id"],),
        ([], []),
    ],
    ids=["replace_all", "replace_some", "replace_none", "empty_dataframe"],
)  # type: ignore
def test_replace_replenishment_project_ids(old_project_ids: List[str], expected_project_ids: List[str]) -> None:
    expected_dataframe = pd.DataFrame({"Project_ID": expected_project_ids}, dtype="object")
    old_dataframe = pd.DataFrame({"Project_ID": old_project_ids}, dtype="object")
    new_dataframe = ModelConfigAccount8._replace_replenishment_project_ids(old_dataframe)

    pd.testing.assert_frame_equal(new_dataframe, expected_dataframe)
