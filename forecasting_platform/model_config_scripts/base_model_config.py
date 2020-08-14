from __future__ import annotations

from abc import (
    ABCMeta,
    abstractmethod,
)
from typing import (
    TYPE_CHECKING,
    Any,
    Dict,
    List,
    Optional,
    Tuple,
)

import pandas as pd
from forecasting_platform.forecasting import (
    add_sales_features,
    calculate_default_weights,
    filter_sales,
    group_sales,
    postprocess_forecast_results,
    preprocess_grouped_sales,
    preprocess_outliers,
)
from forecasting_platform.helpers import absolute_path
from forecasting_platform.internal_schema import CleanedData
from forecasting_platform.static import (
    PREDICTION_MONTH_FORMAT,
    ConfigurationException,
    ExogenousFeatures,
    InternalFeatures,
    Weights,
)
from owforecasting import (
    Granularity,
    TimeSeries,
)
from owforecasting.features import Feature
from skopt.space import (
    Dimension,
    Integer,
    Real,
)

if TYPE_CHECKING:
    from forecasting_platform.services import RuntimeConfig, DataLoader


class BaseModelConfig(metaclass=ABCMeta):
    """Base class for each account model.

    Implements generic default configuration and pipeline steps that can be overridden and extended in sub-classes.

    Runtime state should be managed via :class:`~forecasting_platform.services.Services` and not as instance attributes.

    Any :func:`~abc.abstractmethod` must be defined in the account model config classes,
    therefore these abstract methods are the minimum required configuration for a new account model.

    Args:
        runtime_config: All configuration of the current run.
        data_loader: Service for loading data.

    """

    def __init__(self, runtime_config: RuntimeConfig, data_loader: DataLoader):
        self._runtime_config = runtime_config
        self._data_loader = data_loader

        self.forecast_path = absolute_path(
            runtime_config.output_path
            / "08 Predictions"
            / f"Forecast {self.MODEL_NAME}"
            / f"{runtime_config.prediction_month.strftime(PREDICTION_MONTH_FORMAT)}"
            / f"{runtime_config.run_timestamp}"
            f"_{runtime_config.prediction_month.strftime(PREDICTION_MONTH_FORMAT)}"
            f"_T{runtime_config.test_periods}"
            f"_P{runtime_config.predict_periods}"
            f"_{'_'.join(self.GROUPING)}"
        )

    def __str__(self) -> str:
        return f"{self.MODEL_NAME}: {self.forecast_path}"

    @property
    @abstractmethod
    def MODEL_NAME(self) -> str:
        """Model identifier used to reference this model in created files and log messages."""
        ...

    @property
    @abstractmethod
    def CONTRACTS(self) -> List[str]:
        """List of contracts to be included in the model."""
        ...

    @property
    def model_data_query(self) -> Any:
        """Database query used to split training data for this model."""
        return CleanedData.c.Contract_ID.in_(self.CONTRACTS)

    @property
    @abstractmethod
    def TRAINING_START(self) -> pd.Timestamp:
        """Account-specific :class:`pandas.Timestamp` to include training data newer than this month."""
        ...

    @property
    def GROUPING(self) -> List[str]:
        """Define the granularity of aggregations and the identifier/index level for this model."""
        return ["Contract_ID", "Item_ID"]

    @property
    @abstractmethod
    def POSTPROCESS_DEPTH(self) -> int:
        """Consider these number of months when performing certain postprocessing steps."""
        ...

    @property
    @abstractmethod
    def DEFAULT_FEATURES(self) -> List[Feature]:
        """Create the list of account-specific features with :func:`~owforecasting.features.default_features`."""
        ...

    HYPER_SPACE: List[Dimension] = [
        Integer(5, 30, name="max_depth"),
        Real(1e-5, 1e-1, name="learn_rate", prior="log-uniform"),
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

    #: Apply weighting feature to training data. Disable this feature, when set to 0.
    WEIGHTING: Optional[int] = None

    PREPROCESS_OUTLIERS = False

    PREPROCESS_UNIT_COST_AGGREGATION = "mean"

    EXCLUDE_PROJECTS: List[str] = []
    ONLY_INCLUDE_PROJECTS: List[str] = []

    EXCLUDE_ITEMS: List[int] = []
    ONLY_INCLUDE_ITEMS: List[int] = []

    SALES_MIN_PERIOD = 5

    DEFAULT_HYPER_PARAMS = {
        "weights_column": "W_Weight",
        "score_each_iteration": True,  # Avoid non-deterministic behavior
        "score_tree_interval": 1,  # Avoid non-deterministic behavior,
        # see http://docs.h2o.ai/h2o/latest-stable/h2o-docs/data-science/algo-params/score_tree_interval.html
    }

    #: Number of points to evaluate by :func:`~owforecasting.models.optimize_bayes`.
    OPTIMIZE_HYPER_PARAMETERS_N_CALLS = 20

    _OPTIMIZE_BAYES_GP_ARGS = None  # Only exists so it can be changed to speed-up integration tests

    @property
    def OVERRIDE_HYPER_PARAMS(self) -> Dict[str, object]:
        """Set this to extend or overwrite values from DEFAULT_HYPER_PARAMS."""
        return {}

    def configure_features(self, cleaned_data_run_id: int) -> Tuple[InternalFeatures, ExogenousFeatures]:
        """Override this to configure account-specific internal or exogenous features, e.g. build-rates.

        Args:
            cleaned_data_run_id: ID of the run that created the cleaned data set in the internal database.
        """
        return {}, {}

    def calculate_weights(self) -> Weights:
        """Override this to configure account-specific weighting functions."""
        assert self.WEIGHTING is not None, f"Must define WEIGHTING for default calculate_weights function on {self}"
        return calculate_default_weights(self.TRAINING_START, self._runtime_config.forecast_end, self.WEIGHTING)

    def preprocess_account_data(
        self, sales_raw: pd.DataFrame, grouping: List[str], internal_features: InternalFeatures
    ) -> pd.DataFrame:
        """Preprocess account data by filtering and aggregating the raw sales data and adding internal features.

        Args:
            sales_raw: Raw sales data to pre-process.
            grouping: Grouping columns to use.
            internal_features: Internal features to add.

        Returns:
            :class:`~pandas.DataFrame` with pre-processing applied.

        """
        sales = filter_sales(
            sales_raw,
            exclude_projects=self.EXCLUDE_PROJECTS,
            only_include_projects=self.ONLY_INCLUDE_PROJECTS,
            exclude_items=self.EXCLUDE_ITEMS,
            only_include_items=self.ONLY_INCLUDE_ITEMS,
        )
        sales = add_sales_features(sales, internal_features)
        return group_sales(sales, grouping, unit_cost_aggregation=self.PREPROCESS_UNIT_COST_AGGREGATION)

    def prepare_training_data(
        self, sales: pd.DataFrame, grouping: List[str], exo_features: ExogenousFeatures,
    ) -> TimeSeries:
        """Prepare sales data for training by configuring :class:`~owforecasting.timeseries.TimeSeries`.

        This includes selection of training data from all sales and weighting of provided exogenous features.

        Args:
            sales: Sales data to prepare for training.
            grouping: Grouping columns to use for the time series.
            exo_features: Exogenous features to add to the data.
            weights: Weighting feature based on dates.

        Returns:
            Prepared training data.

        """
        sales_train = preprocess_grouped_sales(
            sales,
            self._runtime_config.forecast_start,
            self._runtime_config.test_periods,
            self.TRAINING_START,
            grouping,
            sales_min_period=self.SALES_MIN_PERIOD,
        )

        if sales_train.empty:
            raise ConfigurationException(
                f"Not enough training data for {self}. "
                "Please check configuration of --forecast-periods and TRAINING_START."
            )

        if self.PREPROCESS_OUTLIERS:
            ts_tmp = TimeSeries(
                sales_train,
                datetime_col="Date",
                response_col="Order_Quantity",
                grouping_cols=grouping,
                granularity=Granularity.MONTH,
                test_periods=self._runtime_config.test_periods,
                predict_periods=self._runtime_config.predict_periods,
                standardize_response=False,
            )
            sales_train = preprocess_outliers(
                ts_tmp._data, self.GROUPING, self._runtime_config.forecast_start
            ).reset_index(drop=True)

        # Create TimeSeries
        ts = TimeSeries(
            sales_train,
            datetime_col="Date",
            response_col="Order_Quantity",
            grouping_cols=grouping,
            granularity=Granularity.MONTH,
            test_periods=self._runtime_config.test_periods,
            predict_periods=self._runtime_config.predict_periods,
        ).add_features(self.DEFAULT_FEATURES)

        # Add exogenous features
        for i, (name, (exo, default)) in enumerate(exo_features.items()):
            ts.add_exogenous_feature(f"EXO{i}", exo, default)

        # Add weighting
        weights = self.calculate_weights()
        ts.add_exogenous_feature("W", weights)

        return ts

    def postprocess_forecast(
        self, ts: TimeSeries, ts_pred: TimeSeries, sales: pd.DataFrame, grouping: List[str]
    ) -> pd.DataFrame:
        """Postprocess account forecast, including filtering and grouping.

        Args:
            ts: :class:`~owforecasting.timeseries.TimeSeries` containing actual values.
            ts_pred: :class:`~owforecasting.timeseries.TimeSeries` containing predicted values.
            sales: :class:`~pandas.DataFrame` with actual sales data.
            grouping: Grouping columns to use.

        Returns:
            Post-processed forecast.

        """
        return postprocess_forecast_results(
            ts_pred.result_data, grouping, self._runtime_config.forecast_start, self.POSTPROCESS_DEPTH
        )
