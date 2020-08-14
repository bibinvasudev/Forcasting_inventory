from typing import Tuple

import pandas as pd
from forecasting_platform.forecasting import (
    calculate_weights_shock,
    generate_airframe_feature,
    generate_market_shock_feature,
)
from forecasting_platform.helpers.identifier import (
    AccountID,
    ContractID,
    ProjectID,
)
from forecasting_platform.static import (
    ExogenousFeatures,
    InternalFeatures,
    Weights,
)
from owforecasting.features import default_features

from .base_model_config import BaseModelConfig


class ModelConfigAccount13(BaseModelConfig):
    """Model config for Account 13."""

    MODEL_NAME = AccountID("Account 13")

    CONTRACTS = [ContractID("Contract_453"), ContractID("Contract_460"), ContractID("Contract_456")]

    EXCLUDE_PROJECTS = [ProjectID("Project_1743")]

    TRAINING_START = pd.Timestamp(year=2017, month=8, day=1)

    POSTPROCESS_DEPTH = 4

    DEFAULT_FEATURES = default_features(lag=[1, 2, 3], moving_window=[3],)

    WEIGHTING = 20  # Only used for backward forecasts before COVID-19

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

        airframes = self._data_loader.load_exogenous_feature(feature_name="Airframe Map", run_id=cleaned_data_run_id)

        return generate_airframe_feature(airframes), {"Shock_Feature": (market_shock, "None")}
