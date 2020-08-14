from typing import (
    List,
    Tuple,
)

import pandas as pd
from forecasting_platform.forecasting import generate_airframe_feature
from forecasting_platform.helpers.identifier import (
    AccountID,
    ContractID,
)
from forecasting_platform.static import (
    ExogenousFeatures,
    InternalFeatures,
)
from owforecasting import TimeSeries
from owforecasting.features import default_features

from .base_model_config import BaseModelConfig


class ModelConfigAccount11(BaseModelConfig):
    """Model config for Account 11."""

    MODEL_NAME = AccountID("Account 11")

    CONTRACTS = [ContractID("Contract_284")]

    TRAINING_START = pd.Timestamp(year=2018, month=1, day=1)

    POSTPROCESS_DEPTH = 4

    DEFAULT_FEATURES = default_features(lag=[1, 2, 3], moving_window=[3, 5],)

    WEIGHTING = 10

    def configure_features(self, cleaned_data_run_id: int) -> Tuple[InternalFeatures, ExogenousFeatures]:
        airframes = self._data_loader.load_exogenous_feature(feature_name="Airframe Map", run_id=cleaned_data_run_id)
        return generate_airframe_feature(airframes), {}

    def postprocess_forecast(
        self, ts: TimeSeries, ts_pred: TimeSeries, sales: pd.DataFrame, grouping: List[str]
    ) -> pd.DataFrame:
        # Do not apply any post-processing for this account
        return ts_pred.result_data
