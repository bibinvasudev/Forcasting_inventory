from typing import List

import pandas as pd
from forecasting_platform.helpers.identifier import (
    AccountID,
    ContractID,
    ProjectID,
)
from forecasting_platform.static import InternalFeatures
from owforecasting.features import default_features

from .base_model_config import BaseModelConfig


class ModelConfigAccount8(BaseModelConfig):
    """Model config for Account 8."""

    MODEL_NAME = AccountID("Account 8")

    CONTRACTS = [ContractID("Contract_102"), ContractID("Contract_28"), ContractID("Contract_61")]

    TRAINING_START = pd.Timestamp(2018, 4, 1)

    DEFAULT_FEATURES = default_features(lag=[1, 2, 3], moving_window=[3])

    WEIGHTING = 10

    POSTPROCESS_DEPTH = 4

    def preprocess_account_data(
        self, sales_raw: pd.DataFrame, grouping: List[str], internal_features: InternalFeatures
    ) -> pd.DataFrame:
        sales_raw = self._replace_replenishment_project_ids(sales_raw)
        return super().preprocess_account_data(sales_raw, grouping, internal_features)

    @staticmethod
    def _replace_replenishment_project_ids(sales_raw: pd.DataFrame) -> pd.DataFrame:
        sales_raw = sales_raw.copy()
        replace_ids = {
            ProjectID("Project_365"): ProjectID("Project_364"),
        }
        for old, new in replace_ids.items():
            sales_raw.loc[sales_raw["Project_ID"] == old, "Project_ID"] = new
        return sales_raw
