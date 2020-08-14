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


class ModelConfigAccount14(BaseModelConfig):
    """Model config for Account 14."""

    MODEL_NAME = AccountID("Account 14")

    CONTRACTS = [
        ContractID("Contract_206"),
        ContractID("Contract_142"),
        ContractID("Contract_194"),
        ContractID("Contract_138"),
        ContractID("Contract_196"),
        ContractID("Contract_141"),
        ContractID("Contract_189"),
        ContractID("Contract_139"),
        ContractID("Contract_140"),
        ContractID("Contract_132"),
        ContractID("Contract_205"),
        ContractID("Contract_192"),
        ContractID("Contract_186"),
        ContractID("Contract_188"),
    ]

    TRAINING_START = pd.Timestamp(year=2018, month=1, day=1)

    POSTPROCESS_DEPTH = 4

    DEFAULT_FEATURES = default_features(lag=[1, 2], moving_window=[2],)

    PREPROCESS_UNIT_COST_AGGREGATION = "max"

    SALES_MIN_PERIOD = 2

    WEIGHTING = 10  # Only used for backward forecasts before COVID-19

    def calculate_weights(self) -> Weights:
        if self._runtime_config.forecast_start >= pd.Timestamp(2020, 6, 1):
            last_train_month = self._runtime_config.forecast_start - pd.DateOffset(months=1)
            return calculate_weights_shock(
                self.TRAINING_START, self._runtime_config.forecast_end, pd.Timestamp(2020, 4, 1), last_train_month,
            )

        return super().calculate_weights()

    def _add_project_feature(self, sales: pd.DataFrame, name: str) -> pd.DataFrame:
        """Add project feature to input data.

        Args:
            sales: Sales to add feature to.
            name: Name of the feature column.

        Returns:
            :class:`~pandas.DataFrame` with added project feature.

        """
        sales[name] = sales["Project_ID"]
        return sales

    def configure_features(self, cleaned_data_run_id: int) -> Tuple[InternalFeatures, ExogenousFeatures]:
        # Market shock feature: COVID
        market_shock = generate_market_shock_feature(
            train_start=self.TRAINING_START, first_shock_month=pd.Timestamp(2020, 4, 1)
        )

        return {"Project_Feature": self._add_project_feature}, {"Shock_Feature": (market_shock, "None")}
