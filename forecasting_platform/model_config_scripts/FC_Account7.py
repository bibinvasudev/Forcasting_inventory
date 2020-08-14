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
from skopt.space import (
    Integer,
    Real,
)

from .base_model_config import BaseModelConfig


class ModelConfigAccount7(BaseModelConfig):
    """Model config for Account 7."""

    MODEL_NAME = AccountID("Account 7")

    CONTRACTS = [
        ContractID("Contract_48"),
        ContractID("Contract_386"),
        ContractID("Contract_385"),
        ContractID("Contract_383"),
        ContractID("Contract_391"),
        ContractID("Contract_395"),
        ContractID("Contract_394"),
        ContractID("Contract_387"),
        ContractID("Contract_392"),
        ContractID("Contract_393"),
        ContractID("Contract_390"),
        ContractID("Contract_384"),
        ContractID("Contract_389"),
        ContractID("Contract_396"),
    ]

    TRAINING_START = pd.Timestamp(2018, 1, 1)

    DEFAULT_FEATURES = default_features(lag=[1, 2, 3], moving_window=[2], moving_std_dev=[3], moving_avg=[2, 3, 4])

    WEIGHTING = 10  # Only used for backward forecasts before COVID-19

    POSTPROCESS_DEPTH = 8

    HYPER_SPACE = [
        Integer(5, 50, name="max_depth"),
        Real(1e-4, 1e-1, name="learn_rate", prior="log-uniform"),
        Real(0.9, 0.9999, name="learn_rate_annealing"),
        Integer(10, 25, name="min_rows"),
        Integer(50, 200, name="ntrees"),
        Real(1e-4, 1e-2, name="stopping_tolerance"),
        Integer(2, 10, name="stopping_rounds"),
        Real(1e-1, 1, name="sample_rate"),
        Real(0.99, 1, name="col_sample_rate"),
        Integer(100, 500, name="nbins"),
        Real(1e-10, 1e-3, name="min_split_improvement"),
        Integer(10, 11, name="nfolds"),
    ]

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
