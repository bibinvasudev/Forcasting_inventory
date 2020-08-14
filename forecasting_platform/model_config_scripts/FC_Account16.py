from typing import (
    List,
    Tuple,
)

import pandas as pd
from forecasting_platform.forecasting import (
    calculate_weights_shock,
    generate_build_rates_features,
    generate_market_shock_feature,
)
from forecasting_platform.helpers.identifier import (
    AccountID,
    ContractID,
)
from forecasting_platform.static import (
    ExogenousFeatures,
    InternalFeatures,
    Weights,
)
from owforecasting import TimeSeries
from owforecasting.features import default_features

from .base_model_config import BaseModelConfig


class ModelConfigAccount16(BaseModelConfig):
    """Model config for Account 16."""

    MODEL_NAME = AccountID("Account 16")

    CONTRACTS = [ContractID("Contract_361"), ContractID("Contract_360")]

    TRAINING_START = pd.Timestamp(year=2018, month=1, day=1)

    POSTPROCESS_DEPTH = 6

    DEFAULT_FEATURES = default_features(lag=[1, 3, 5], moving_window=[3], moving_avg=[3],)

    WEIGHTING = 40  # Only used for backward forecasts before COVID-19

    def calculate_weights(self) -> Weights:
        if self._runtime_config.forecast_start >= pd.Timestamp(2020, 6, 1):
            last_train_month = self._runtime_config.forecast_start - pd.DateOffset(months=1)
            return calculate_weights_shock(
                self.TRAINING_START, self._runtime_config.forecast_end, pd.Timestamp(2020, 4, 1), last_train_month,
            )

        return super().calculate_weights()

    def configure_features(self, cleaned_data_run_id: int) -> Tuple[InternalFeatures, ExogenousFeatures]:
        # Build Rates
        build_rates = self._data_loader.load_exogenous_feature(feature_name="Build Rate", run_id=cleaned_data_run_id)
        internal_features, exogenous_features = generate_build_rates_features(build_rates)

        # Market shock feature: COVID
        market_shock = generate_market_shock_feature(
            train_start=self.TRAINING_START, first_shock_month=pd.Timestamp(2020, 4, 1)
        )
        exogenous_features["Shock_Feature"] = (market_shock, "None")

        return internal_features, exogenous_features

    def postprocess_forecast(
        self, ts: TimeSeries, ts_pred: TimeSeries, sales: pd.DataFrame, grouping: List[str]
    ) -> pd.DataFrame:
        # Do not apply any post-processing for this account
        return ts_pred.result_data
