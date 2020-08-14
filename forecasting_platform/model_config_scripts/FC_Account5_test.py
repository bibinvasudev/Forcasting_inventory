import pandas as pd
import pytest
from forecasting_platform.dsx_write_schema import DsxOutput
from forecasting_platform.helpers import (
    assert_backward_forecast,
    assert_forward_forecast,
    generate_test_id_from_test_parameters,
)
from forecasting_platform.helpers.identifier import ContractID
from forecasting_platform.internal_schema import ForecastData
from forecasting_platform.services import Database
from forecasting_platform.static import (
    DatabaseType,
    ForecastTestParameters,
    OrchestratorResult,
)

BACKWARD_FORECAST_PARAMETERS = ForecastTestParameters(
    model_config="ModelConfigAccount5", forecast_periods=9, prediction_month=pd.Timestamp(year=2020, month=3, day=1),
)


@pytest.mark.account
@pytest.mark.parametrize(
    "backward_forecast", [BACKWARD_FORECAST_PARAMETERS], indirect=True, ids=generate_test_id_from_test_parameters
)
class TestBackwardForecastAccount5:
    def test_result(self, backward_forecast: OrchestratorResult) -> None:
        assert_backward_forecast(backward_forecast)


DEVELOPMENT_FORECAST_WITH_PREDICTION_START_MONTH_PARAMETERS = ForecastTestParameters(
    model_config="ModelConfigAccount5", forecast_periods=1, prediction_month=(pd.Timestamp(year=2020, month=6, day=1)),
)


@pytest.mark.account
@pytest.mark.parametrize(
    "development_forecast",
    [DEVELOPMENT_FORECAST_WITH_PREDICTION_START_MONTH_PARAMETERS],
    indirect=True,
    ids=generate_test_id_from_test_parameters,
)
class TestDevelopmentForecastAccount5:
    def test_result(self, development_forecast: OrchestratorResult) -> None:
        assert_forward_forecast(development_forecast)


PRODUCTION_FORECAST_WITH_PREDICTION_START_MONTH_PARAMETERS = ForecastTestParameters(
    model_config="ModelConfigAccount5",
    forecast_periods=3,
    prediction_month=(pd.Timestamp(year=2020, month=1, day=1)),
    disable_database=False,
)


@pytest.mark.account
@pytest.mark.parametrize(
    "production_forecast",
    [PRODUCTION_FORECAST_WITH_PREDICTION_START_MONTH_PARAMETERS],
    indirect=True,
    ids=generate_test_id_from_test_parameters,
)
class TestProductionForecastAccount5:
    EXPECTED_FORECAST_DATA_COUNT = 2415

    def test_structure(self, production_forecast: OrchestratorResult) -> None:
        assert_forward_forecast(production_forecast)

    def test_forecast_data_in_internal_database(self, production_forecast: OrchestratorResult) -> None:
        model_run_id = production_forecast.forecast_result.model_run_id
        expected = [
            (
                model_run_id,
                ContractID("Contract_378"),
                64987,
                202001,
                202001,
                0,
                pytest.approx(29.589586),
                pytest.approx(29.589586),
                0,
                0.0,
            ),
            (
                model_run_id,
                ContractID("Contract_378"),
                64987,
                202001,
                202002,
                1,
                pytest.approx(60.164522),
                pytest.approx(60.164522),
                138,
                pytest.approx(0.4359747),
            ),
            (
                model_run_id,
                ContractID("Contract_378"),
                64987,
                202001,
                202003,
                2,
                pytest.approx(25.365093),
                pytest.approx(25.365093),
                0,
                0.0,
            ),
        ]

        with Database(DatabaseType.internal).transaction_context() as session:
            forecast_data = (
                session.query(ForecastData)  # type: ignore
                .filter(ForecastData.c.model_run_id == model_run_id)
                .filter(ForecastData.c.Contract_ID == ContractID("Contract_378"))
                .filter(ForecastData.c.Item_ID == 64987)
                .all()
            )
            assert forecast_data == expected

            forecast_data_count = (
                session.query(ForecastData)  # type: ignore
                .filter(ForecastData.c.model_run_id == model_run_id)
                .count()
            )
            assert forecast_data_count == self.EXPECTED_FORECAST_DATA_COUNT

    def test_forecast_data_in_dsx_write_database(self, production_forecast: OrchestratorResult) -> None:
        with Database(DatabaseType.dsx_write).transaction_context() as session:
            dsx_output_data_count = session.query(DsxOutput).count()  # type: ignore
            # Currently there is no good identifier to get the results of this specific test run
            assert (
                dsx_output_data_count >= self.EXPECTED_FORECAST_DATA_COUNT
            )  # Further checks are done in the end-to-end test
