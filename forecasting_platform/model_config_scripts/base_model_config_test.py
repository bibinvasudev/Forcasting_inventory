import forecasting_platform.model_config_scripts
import pandas as pd
import pytest
from _pytest.monkeypatch import MonkeyPatch
from forecasting_platform import master_config
from forecasting_platform.services import initialize
from forecasting_platform.static import (
    PREDICTION_MONTH_FORMAT,
    ConfigurationException,
    EngineRunType,
)


@pytest.mark.parametrize("model_config_str", master_config.model_configs)  # type: ignore
def test_sales_min_period_backward(model_config_str: str, monkeypatch: MonkeyPatch) -> None:
    monkeypatch.setattr(master_config, "db_connection_attempts", 0)
    model_config = forecasting_platform.model_config_scripts.__dict__[model_config_str]
    default_prediction_start_month = pd.Timestamp(year=2020, month=3, day=1)

    end_period = pd.to_datetime(master_config.default_prediction_start_month, format=PREDICTION_MONTH_FORMAT)
    start_period = pd.to_datetime(model_config.TRAINING_START)
    diff = (end_period.year - start_period.year) * 12 + (end_period.month - start_period.month)
    erroneous_forecast_periods = diff - model_config.SALES_MIN_PERIOD + 1

    with initialize(
        engine_run_type=EngineRunType.backward,
        forecast_periods=erroneous_forecast_periods,
        prediction_start_month=default_prediction_start_month,
    ) as services:
        with pytest.raises(
            ConfigurationException, match="Please check configuration of --forecast-periods and TRAINING_START."
        ):
            services.internal_database.is_disabled = lambda: True  # type: ignore  # Avoid blinking tests with DB access
            services.runtime_config.model_configs = [model_config]
            services.orchestrator.run()
