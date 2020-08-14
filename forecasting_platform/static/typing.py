from __future__ import annotations

from typing import (
    TYPE_CHECKING,
    Callable,
    Dict,
    NamedTuple,
    Optional,
    Tuple,
    Type,
    TypeVar,
    Union,
)

import pandas as pd

if TYPE_CHECKING:
    from forecasting_platform.services import RuntimeConfig


if TYPE_CHECKING:
    from sqlalchemy.sql.type_api import TypeEngine

    T = TypeVar("T")

    class SqlAlchemyEnum(TypeEngine[T]):
        """Wrapper to allow proper type-checking for Enum columns.

        See: https://github.com/dropbox/sqlalchemy-stubs/issues/114
        """

        def __init__(self, enum: Type[T]) -> None:
            ...


else:
    from sqlalchemy import Enum as SqlAlchemyEnum  # noqa: F401


InternalFeatures = Dict[str, Callable[[pd.DataFrame, str], pd.DataFrame]]
ExogenousFeatures = Dict[str, Tuple[pd.DataFrame, Union[str, float]]]
Weights = Union[pd.DataFrame]


class OrchestratorResult(NamedTuple):
    """Container for :class:`~forecasting_platform.services.Orchestrator` results, used by integration tests."""

    runtime_config: RuntimeConfig
    forecast_result: ForecastTestResult


class ForecastTestResult(NamedTuple):
    """Container for forecast results, used by integration tests."""

    model_run_id: Optional[int]
    result_data: pd.DataFrame


class ForecastTestParameters(NamedTuple):
    """Container for forecast test parameters, used by integration tests."""

    model_config: str
    forecast_periods: int
    prediction_month: pd.Timestamp
    input_filter: Optional[Callable[[pd.DataFrame], pd.DataFrame]] = None

    #  Due to memory restrictions in Bitbucket pipeline we cannot use the database for all accounts at the moment
    disable_database: bool = True


class DataFrameStructure(NamedTuple):
    """Container for expected forecast :class:`~pandas.DataFrame` structure, used by integration tests."""

    columns: pd.Index
    dtypes: pd.Series
    shape: Tuple[int, int]
