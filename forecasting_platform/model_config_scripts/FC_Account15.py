from typing import (
    List,
    Tuple,
)

import pandas as pd
from forecasting_platform.forecasting import (
    calculate_weights_shock,
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
from owforecasting import TimeSeries
from owforecasting.features import default_features

from .base_model_config import BaseModelConfig


class ModelConfigAccount15(BaseModelConfig):
    """Model config for Account 15."""

    MODEL_NAME = AccountID("Account 15")

    CONTRACTS = [
        ContractID("Contract_254"),
        ContractID("Contract_545"),
        ContractID("Contract_548"),
        ContractID("Contract_547"),
    ]

    EXCLUDE_PROJECTS = [
        ProjectID("Project_2199"),
        ProjectID("Project_2196"),
        ProjectID("Project_2188"),
        ProjectID("Project_864"),
    ]

    TRAINING_START = pd.Timestamp(year=2018, month=4, day=1)

    POSTPROCESS_DEPTH = 3

    DEFAULT_FEATURES = default_features(lag=[1, 2, 3], moving_window=[3, 5],)

    WEIGHTING = 10  # Only used for backward forecasts before COVID-19

    def preprocess_account_data(
        self, sales_raw: pd.DataFrame, grouping: List[str], internal_features: InternalFeatures,
    ) -> pd.DataFrame:
        sales_raw = self._additional_scope_filters(sales_raw)
        return super().preprocess_account_data(sales_raw, grouping, internal_features)

    @staticmethod
    def _additional_scope_filters(sales_raw: pd.DataFrame) -> pd.DataFrame:
        sales_raw = sales_raw.copy()
        exclusion_parts = [362698, 21102, 1033556, 226282, 20754, 21297, 14518]
        mask = sales_raw["Contract_ID"].isin([ContractID("Contract_236")])
        mask &= sales_raw["Item_ID"].isin(exclusion_parts)
        return sales_raw.loc[~mask]

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

    def postprocess_forecast(
        self, ts: TimeSeries, ts_pred: TimeSeries, sales: pd.DataFrame, grouping: List[str]
    ) -> pd.DataFrame:
        # Do not apply any post-processing for this account
        return ts_pred.result_data
