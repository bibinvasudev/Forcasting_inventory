import logging
from datetime import datetime
from typing import Any

import pandas as pd
import pytest
from _pytest.logging import LogCaptureFixture
from forecasting_platform import master_config
from forecasting_platform.model_config_scripts import ModelConfigAccount2
from forecasting_platform.static import EngineRunType

from .runtime_config import RuntimeConfig


@pytest.mark.freeze_time("2000-12-31-17-00")  # type: ignore
def test_logs_current_config(caplog: LogCaptureFixture) -> None:
    runtime_config = RuntimeConfig(
        engine_run_type=EngineRunType.backward,
        forecast_periods=13,
        output_location="test",
        prediction_month=pd.Timestamp(year=2000, month=1, day=1),
    )

    with caplog.at_level(logging.INFO):
        runtime_config.log_config()
        assert caplog.messages == [
            "Runtime config engine_run_type = EngineRunType.backward",
            "Runtime config force_reload = False",
            "Runtime config run_timestamp = 20001231_1700",
            "Runtime config prediction_month = 200001",
            "Runtime config test_periods = 13",
            "Runtime config predict_periods = 0",
            "Runtime config output_path = test",
            "Runtime config output_format = OutputFormat.csv",
            "Runtime config optimize_hyperparameters = False",
            f"Runtime config model_configs = {[config for config in master_config.model_configs]}",
        ]


def _generate_test_id(val: Any) -> Any:
    if isinstance(val, (datetime,)):
        return val.strftime("%Y_%m")
    if isinstance(val, (int,)):
        return f"forecast_periods:{val}"
    return val


@pytest.mark.parametrize(
    "forecast_periods,expected_forecast_start,expected_forecast_end",
    [
        (1, pd.Timestamp(year=2020, month=2, day=1), pd.Timestamp(year=2020, month=2, day=1)),
        (2, pd.Timestamp(year=2020, month=1, day=1), pd.Timestamp(year=2020, month=2, day=1)),
        (3, pd.Timestamp(year=2019, month=12, day=1), pd.Timestamp(year=2020, month=2, day=1)),
        (15, pd.Timestamp(year=2018, month=12, day=1), pd.Timestamp(year=2020, month=2, day=1)),
    ],
    ids=_generate_test_id,
)  # type: ignore
def test_forecast_interval_backward_run(
    forecast_periods: int, expected_forecast_start: pd.Timestamp, expected_forecast_end: pd.Timestamp
) -> None:
    runtime_config = RuntimeConfig(
        engine_run_type=EngineRunType.backward,
        forecast_periods=forecast_periods,
        prediction_month=pd.Timestamp(year=2020, month=2, day=1),
    )
    assert runtime_config.forecast_start == expected_forecast_start
    assert runtime_config.forecast_end == expected_forecast_end


@pytest.mark.parametrize(
    "forecast_periods,expected_forecast_start,expected_forecast_end",
    [
        (1, pd.Timestamp(year=2020, month=2, day=1), pd.Timestamp(year=2020, month=2, day=1)),
        (2, pd.Timestamp(year=2020, month=2, day=1), pd.Timestamp(year=2020, month=3, day=1)),
        (12, pd.Timestamp(year=2020, month=2, day=1), pd.Timestamp(year=2021, month=1, day=1)),
    ],
    ids=_generate_test_id,
)  # type: ignore
def test_forecast_interval_development_run(
    forecast_periods: int, expected_forecast_start: pd.Timestamp, expected_forecast_end: pd.Timestamp
) -> None:
    runtime_config = RuntimeConfig(
        engine_run_type=EngineRunType.development,
        forecast_periods=forecast_periods,
        prediction_month=pd.Timestamp(year=2020, month=2, day=1),
    )
    assert runtime_config.forecast_start == expected_forecast_start
    assert runtime_config.forecast_end == expected_forecast_end


@pytest.mark.parametrize(
    "forecast_periods,expected_forecast_start,expected_forecast_end",
    [
        (1, pd.Timestamp(year=2020, month=2, day=1), pd.Timestamp(year=2020, month=2, day=1)),
        (2, pd.Timestamp(year=2020, month=2, day=1), pd.Timestamp(year=2020, month=3, day=1)),
        (12, pd.Timestamp(year=2020, month=2, day=1), pd.Timestamp(year=2021, month=1, day=1)),
    ],
    ids=_generate_test_id,
)  # type: ignore
def test_forecast_interval_production_run(
    forecast_periods: int, expected_forecast_start: pd.Timestamp, expected_forecast_end: pd.Timestamp
) -> None:
    runtime_config = RuntimeConfig(
        engine_run_type=EngineRunType.production,
        forecast_periods=forecast_periods,
        prediction_month=pd.Timestamp(year=2020, month=2, day=1),
    )
    assert runtime_config.forecast_start == expected_forecast_start
    assert runtime_config.forecast_end == expected_forecast_end


class TestModelConfig:
    def test_exclude_model_config(self) -> None:
        runtime_config = RuntimeConfig(
            engine_run_type=EngineRunType.backward, exclude_model_config="ModelConfigAccount2"
        )

        expected_configs = master_config.model_configs.copy()
        expected_configs.remove("ModelConfigAccount2")

        assert [config.__name__ for config in runtime_config.model_configs] == expected_configs

    def test_only_model_config(self) -> None:
        runtime_config = RuntimeConfig(engine_run_type=EngineRunType.backward, only_model_config="ModelConfigAccount2")
        assert runtime_config.model_configs == [ModelConfigAccount2]

    def test_default_model_configs(self) -> None:
        runtime_config = RuntimeConfig(engine_run_type=EngineRunType.backward)
        assert [config.__name__ for config in runtime_config.model_configs] == master_config.model_configs


@pytest.mark.parametrize(
    "engine_run_type,force_reload,includes_cleaning",
    [
        (EngineRunType.backward, False, False),
        (EngineRunType.backward, True, True),
        (EngineRunType.development, False, True),
        (EngineRunType.production, False, True),
    ],
)  # type: ignore
def test_includes_cleaning(engine_run_type: EngineRunType, force_reload: bool, includes_cleaning: bool) -> None:
    runtime_config = RuntimeConfig(engine_run_type=engine_run_type, force_reload=force_reload,)
    assert runtime_config.includes_cleaning == includes_cleaning
