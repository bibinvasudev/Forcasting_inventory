from typing import (
    List,
    Tuple,
)

import pandas as pd
from forecasting_platform.forecasting import (
    RR_NR_Flag,
    calculate_weights_shock,
    extract_cost_info,
    generate_market_shock_feature,
    postprocess_forecast_results,
    reduce_hits,
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
from owforecasting import TimeSeries
from owforecasting.features import default_features
from skopt.space import (
    Integer,
    Real,
)

from .base_model_config import BaseModelConfig


class ModelConfigAccount6(BaseModelConfig):
    """Model config for Account 6."""

    MODEL_NAME = AccountID("Account 6")

    CONTRACTS = [ContractID("Contract_730")]

    TRAINING_START = pd.Timestamp(2018, 1, 1)

    _LAGS = [1, 2, 3, 4, 6, 8, 10]

    DEFAULT_FEATURES = default_features(lag=_LAGS, moving_window=[3, 4, 6, 8, 10])

    WEIGHTING = 1000  # Only used for backward forecasts before COVID-19

    POSTPROCESS_DEPTH = 6

    OVERRIDE_HYPER_PARAMS = {
        "nfolds": 5,
        "min_split_improvement": 1e-5,
    }

    OPTIMIZE_HYPER_PARAMETERS_N_CALLS = 10

    HYPER_SPACE = [
        Integer(5, 30, name="max_depth"),
        Real(1e-4, 1, name="learn_rate", prior="log-uniform"),
        Real(0.9, 0.9999, name="learn_rate_annealing"),
        Integer(10, 25, name="min_rows"),
        Integer(50, 200, name="ntrees"),
        Real(1e-4, 1e-2, name="stopping_tolerance"),
        Integer(2, 10, name="stopping_rounds"),
        Real(1e-1, 1, name="sample_rate"),
        Real(0.99, 1, name="col_sample_rate"),
        Integer(100, 500, name="nbins"),
        Real(1e-10, 1e-3, name="min_split_improvement"),
        Integer(5, 6, name="nfolds"),
    ]

    SALES_MIN_PERIOD = max(_LAGS)

    PREPROCESS_OUTLIERS = True

    PREPROCESS_UNIT_COST_AGGREGATION = "max"

    _runner_flag: pd.DataFrame

    def preprocess_account_data(
        self, sales_raw: pd.DataFrame, grouping: List[str], internal_features: InternalFeatures
    ) -> pd.DataFrame:
        ltm_end = pd.to_datetime(self._runtime_config.prediction_month, format="%Y%m")
        ltm_start = ltm_end - pd.DateOffset(months=11)
        self._runner_flag = RR_NR_Flag(sales_raw, ltm_start, ltm_end)

        return super().preprocess_account_data(sales_raw, grouping, internal_features)

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

    def prepare_training_data(
        self, sales: pd.DataFrame, grouping: List[str], exo_features: ExogenousFeatures
    ) -> TimeSeries:
        ts = super().prepare_training_data(sales, grouping, exo_features)

        ts.add_exogenous_feature("W_UC", extract_cost_info(sales))
        ts.add_exogenous_feature("RR", self._runner_flag, default=False)

        return ts

    def postprocess_forecast(
        self, ts: TimeSeries, ts_pred: TimeSeries, sales: pd.DataFrame, grouping: List[str]
    ) -> pd.DataFrame:
        results = postprocess_forecast_results(
            ts_pred.result_data,
            grouping,
            self._runtime_config.forecast_start,
            self.POSTPROCESS_DEPTH,
            is_adhoc_forecast=True,
        )
        return reduce_hits(results, self._runner_flag, grouping, self._runtime_config.forecast_start)
