from typing import Tuple

import pandas as pd
from forecasting_platform.forecasting import (
    calculate_weights_shock,
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
from owforecasting.features import default_features

from .base_model_config import BaseModelConfig


class ModelConfigAccount44(BaseModelConfig):
    """Model config for Account 44."""

    MODEL_NAME = AccountID("Account 44")

    CONTRACTS = [
        ContractID("Contract_153"),
        ContractID("Contract_148"),
        ContractID("Contract_162"),
        ContractID("Contract_160"),
        ContractID("Contract_150"),
        ContractID("Contract_108"),
    ]

    TRAINING_START = pd.Timestamp(year=2018, month=1, day=1)

    POSTPROCESS_DEPTH = 4

    DEFAULT_FEATURES = default_features(
        lag=[1, 2, 3], moving_window=[], moving_avg=[3], moving_std_dev=[3], moving_non_zero=[]
    )

    WEIGHTING = 30  # Only used for backward forecasts before COVID-19

    def calculate_weights(self) -> Weights:
        if self._runtime_config.forecast_start >= pd.Timestamp(2020, 6, 1):
            last_train_month = self._runtime_config.forecast_start - pd.DateOffset(months=1)
            return calculate_weights_shock(
                self.TRAINING_START, self._runtime_config.forecast_end, pd.Timestamp(2020, 4, 1), last_train_month,
            )

        return super().calculate_weights()

    def configure_features(self, cleaned_data_run_id: int) -> Tuple[InternalFeatures, ExogenousFeatures]:
        # Market shock feature: COVID
        market_shock = generate_market_shock_feature(
            train_start=self.TRAINING_START, first_shock_month=pd.Timestamp(2020, 4, 1)
        )

        return {}, {"Shock_Feature": (market_shock, "None")}
