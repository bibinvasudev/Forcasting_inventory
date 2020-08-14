from typing import Tuple

import pandas as pd
from forecasting_platform.forecasting import (
    calculate_weights_shock,
    generate_build_rates_features,
    generate_market_shock_feature,
)
from forecasting_platform.helpers import (
    AccountID,
    ContractID,
)
from forecasting_platform.static import (
    ExogenousFeatures,
    InternalFeatures,
    Weights,
)
from owforecasting.features import default_features

from .base_model_config import BaseModelConfig


class ModelConfigAccount1(BaseModelConfig):
    """Model config for Account 1."""

    MODEL_NAME = AccountID("Account 1")

    CONTRACTS = [ContractID("Contract_404"), ContractID("Contract_402"), ContractID("Contract_403")]

    TRAINING_START = pd.Timestamp(2018, 2, 1)

    POSTPROCESS_DEPTH = 4

    DEFAULT_FEATURES = default_features(lag=[1, 2, 3], moving_window=[3])

    WEIGHTING = 10  # Only used for backward forecasts before COVID-19

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
