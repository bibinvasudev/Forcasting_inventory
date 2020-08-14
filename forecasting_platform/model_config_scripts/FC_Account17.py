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


class ModelConfigAccount17(BaseModelConfig):
    """Model config for Account 17."""

    MODEL_NAME = AccountID("Account 17")

    CONTRACTS = [
        ContractID("Contract_566"),
        ContractID("Contract_573"),
        ContractID("Contract_598"),
        ContractID("Contract_599"),
        ContractID("Contract_596"),
    ]

    TRAINING_START = pd.Timestamp(2018, 7, 1)

    DEFAULT_FEATURES = default_features(lag=[2], moving_window=[3, 5])

    WEIGHTING = 10

    POSTPROCESS_DEPTH = 4

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
