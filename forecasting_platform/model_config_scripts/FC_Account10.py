import pandas as pd
from forecasting_platform.helpers.identifier import (
    AccountID,
    ContractID,
)
from owforecasting.features import default_features

from .base_model_config import BaseModelConfig


class ModelConfigAccount10(BaseModelConfig):
    """Model config for Account 10."""

    MODEL_NAME = AccountID("Account 10")

    CONTRACTS = [ContractID("Contract_242")]

    TRAINING_START = pd.Timestamp(2018, 1, 1)

    DEFAULT_FEATURES = default_features(lag=[1, 2, 3], moving_window=[3])

    WEIGHTING = 10

    POSTPROCESS_DEPTH = 8
