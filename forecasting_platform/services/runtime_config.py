from __future__ import annotations

import logging
from datetime import datetime
from pathlib import Path
from typing import (
    TYPE_CHECKING,
    List,
    Optional,
    Tuple,
    Type,
    Union,
)

import forecasting_platform.model_config_scripts
import pandas as pd
from forecasting_platform import master_config
from forecasting_platform.static import (
    PREDICTION_MONTH_FORMAT,
    ConfigurationException,
    EngineRunType,
    OutputFormat,
)

logger = logging.getLogger("runtime_config")

if TYPE_CHECKING:
    from forecasting_platform.model_config_scripts import BaseModelConfig


class RuntimeConfig:
    """Combination of :mod:`~forecasting_platform.master_config` and CLI :mod:`~forecasting_platform.cli.forecast_options`.

    In many cases, :mod:`~forecasting_platform.master_config` only contains reasonable default values,
    which can be overridden by explicit CLI parameters.

    This provides the definitive configuration for a specific forecasting run.
    """

    def __init__(
        self,
        engine_run_type: EngineRunType,
        forecast_periods: int = master_config.default_forecast_periods,
        output_location: str = master_config.default_output_location,
        prediction_month: pd.Timestamp = master_config.default_prediction_start_month,
        output_format: OutputFormat = master_config.default_output_format,
        optimize_hyperparameters: bool = False,
        force_reload: bool = False,
        only_model_config: Optional[Union[Type[BaseModelConfig], str]] = None,
        exclude_model_config: Optional[str] = None,
    ):
        self.engine_run_type = engine_run_type

        self.force_reload = force_reload

        self.run_timestamp = datetime.now().strftime("%Y%m%d_%H%M")

        self.prediction_month = prediction_month

        self.test_periods, self.predict_periods = RuntimeConfig._toggle_forecast_direction(
            engine_run_type, forecast_periods
        )

        self.output_path = Path(output_location)

        self.output_format = output_format

        self.optimize_hyperparameters = optimize_hyperparameters

        self.model_configs = self._filter_model_configs(
            master_config.model_configs, only_model_config=only_model_config, exclude_model_config=exclude_model_config
        )

    @property
    def includes_cleaning(self) -> bool:
        """Return ``True`` if data import and cleaning is included in the current run."""
        return self.engine_run_type is not EngineRunType.backward or self.force_reload

    @property
    def full_forecast_periods(self) -> int:
        """Return number of months for the forecast."""
        return self.test_periods + self.predict_periods

    @property
    def forecast_start(self) -> pd.Timestamp:
        """:class:`pandas.Timestamp` when the forecast begins."""
        return self._compute_forecast_start(self.prediction_month, self.test_periods, self.engine_run_type)

    @property
    def forecast_end(self) -> pd.Timestamp:
        """:class:`pandas.Timestamp` when the forecast ends."""
        return self._compute_forecast_end(self.forecast_start, full_forecast_periods=self.full_forecast_periods)

    @staticmethod
    def _toggle_forecast_direction(engine_run_type: EngineRunType, forecast_periods: int) -> Tuple[int, int]:
        if engine_run_type == EngineRunType.backward:
            return forecast_periods, 0
        return 0, forecast_periods

    @staticmethod
    def _compute_forecast_start(
        prediction_month: pd.Timestamp, test_periods: int, engine_run_type: EngineRunType
    ) -> pd.Timestamp:
        return (
            prediction_month - pd.DateOffset(months=test_periods - 1)
            if engine_run_type == EngineRunType.backward
            else prediction_month
        )

    @staticmethod
    def _compute_forecast_end(forecast_start: pd.Timestamp, full_forecast_periods: int) -> pd.Timestamp:
        return forecast_start + pd.DateOffset(months=full_forecast_periods - 1)

    @staticmethod
    def _filter_model_configs(
        model_configs: List[str],
        only_model_config: Optional[Union[Type[BaseModelConfig], str]],
        exclude_model_config: Optional[str],
    ) -> List[Type[BaseModelConfig]]:
        if only_model_config and exclude_model_config:
            raise ConfigurationException("--only-model-config and --exclude-model-config cannot be used together")

        returned_model_configs = model_configs
        if only_model_config:
            if not isinstance(only_model_config, str):
                return [only_model_config]
            returned_model_configs = [only_model_config]
        if exclude_model_config:
            returned_model_configs = [config for config in model_configs if config != exclude_model_config]
        return [forecasting_platform.model_config_scripts.__dict__[config] for config in returned_model_configs]

    def log_config(self) -> None:
        """Log current runtime configuration."""
        for k, v in self.__dict__.items():
            if k == "prediction_month":
                v = self.prediction_month.strftime(PREDICTION_MONTH_FORMAT)
            elif k == "model_configs":
                v = [config.__name__ for config in self.model_configs]
            logger.info(f"Runtime config {k} = {v}")
