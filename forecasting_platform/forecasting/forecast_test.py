import logging
import re
from pathlib import Path
from unittest.mock import Mock

import pytest
from _pytest.logging import LogCaptureFixture
from _pytest.monkeypatch import MonkeyPatch
from forecasting_platform.model_config_scripts import BaseModelConfig
from forecasting_platform.services import DataOutput
from forecasting_platform.static import ConfigurationException
from owforecasting import TimeSeries
from owforecasting.models import H2OGradientBoostingModel

from . import forecast


def test_forecast_model(monkeypatch: MonkeyPatch, caplog: LogCaptureFixture) -> None:
    caplog.set_level(logging.INFO)

    mock_model_config = Mock(
        spec=BaseModelConfig,
        DEFAULT_HYPER_PARAMS=BaseModelConfig.DEFAULT_HYPER_PARAMS,
        HYPER_SPACE=BaseModelConfig.HYPER_SPACE,
        OVERRIDE_HYPER_PARAMS={},
    )

    mock_cleanup_h2o_model = Mock()
    mock_time_series = Mock(spec=TimeSeries)

    monkeypatch.setattr(H2OGradientBoostingModel, "train", lambda _: None)
    monkeypatch.setattr(H2OGradientBoostingModel, "predict", lambda _, __: mock_time_series)
    monkeypatch.setattr(H2OGradientBoostingModel, "estimator", Mock(key="Test Model"))
    monkeypatch.setattr(forecast, "_cleanup_h2o_model", mock_cleanup_h2o_model)

    time_series_prediction = forecast.forecast_model(
        mock_model_config, mock_time_series, Mock(spec=DataOutput), optimize_hyperparameters=False
    )

    assert "Forecasting model: Test Model" in caplog.messages
    assert time_series_prediction is mock_time_series
    mock_cleanup_h2o_model.assert_called_once()


def test_forecast_model_and_optimize_hyperparams(
    monkeypatch: MonkeyPatch, caplog: LogCaptureFixture, tmp_path: Path
) -> None:
    caplog.set_level(logging.INFO)

    mock_model_config = Mock(
        spec=BaseModelConfig,
        DEFAULT_HYPER_PARAMS=BaseModelConfig.DEFAULT_HYPER_PARAMS,
        HYPER_SPACE=BaseModelConfig.HYPER_SPACE,
        OVERRIDE_HYPER_PARAMS={"extra": "hyperparameter"},
        forecast_path=tmp_path,
    )

    mock_cleanup_h2o_model = Mock()
    mock_optimize_bayes = Mock()
    mock_time_series = Mock(spec=TimeSeries)

    monkeypatch.setattr(H2OGradientBoostingModel, "train", lambda _: None)
    monkeypatch.setattr(H2OGradientBoostingModel, "predict", lambda _, __: mock_time_series)
    monkeypatch.setattr(H2OGradientBoostingModel, "estimator", Mock(key="Test Model"))
    monkeypatch.setattr(forecast, "_cleanup_h2o_model", mock_cleanup_h2o_model)
    monkeypatch.setattr(forecast, "optimize_bayes", mock_optimize_bayes)

    time_series_prediction = forecast.forecast_model(
        mock_model_config, mock_time_series, Mock(spec=DataOutput), optimize_hyperparameters=True
    )

    assert "Forecasting model: Test Model" in caplog.messages

    assert time_series_prediction is mock_time_series
    assert any(
        (
            re.search("Starting hyper-parameter optimization.*'extra': 'hyperparameter'.*", message)
            for message in caplog.messages
        )
    )
    assert any(
        (
            re.search("Finished hyper-parameter optimization.*'extra': 'hyperparameter'.*", message)
            for message in caplog.messages
        )
    )
    mock_cleanup_h2o_model.assert_called_once()
    mock_optimize_bayes.assert_called_once()


def test_forecast_model_raises_configuration_exception(monkeypatch: MonkeyPatch) -> None:
    mock_model_config = Mock(
        spec=BaseModelConfig,
        DEFAULT_HYPER_PARAMS=BaseModelConfig.DEFAULT_HYPER_PARAMS,
        HYPER_SPACE=BaseModelConfig.HYPER_SPACE,
        OVERRIDE_HYPER_PARAMS={},
    )

    mock_time_series = Mock(spec=TimeSeries)
    monkeypatch.setattr(H2OGradientBoostingModel, "train", Mock(side_effect=ValueError("Test Value Error")))

    with pytest.raises(
        ConfigurationException,
        match=".*Please check configuration of --prediction-start-month or --prediction-end-month.",
    ):
        forecast.forecast_model(
            mock_model_config, mock_time_series, Mock(spec=DataOutput), optimize_hyperparameters=False
        )
