from __future__ import annotations

from typing import (
    TYPE_CHECKING,
    Any,
    Union,
)

import numpy as np
import pandas as pd
from sqlalchemy import (
    case,
    func,
)

if TYPE_CHECKING:
    from sqlalchemy import Column
    from sqlalchemy.sql.elements import BinaryExpression


def compute_accuracy(forecast: pd.DataFrame) -> pd.Series:
    """Compute accuracy from postprocessed forecast and actual values.

    Accuracy is :data:`~numpy.nan` if any of the input columns is `None` or. :data:`~numpy.nan`
    Accuracy is always returned as :data:`~numpy.nan` if the formula yields non finite result (e.g. division by zero)

    Args:
        forecast: :class:`~pandas.DataFrame` with "Prediction_Post" and "Actual" column

    Returns:
        :class:`~pandas.Series` with computed accuracy

    """
    absolute = abs(forecast["Actual"] - forecast["Prediction_Post"])
    maximum = forecast[["Actual", "Prediction_Post"]].max(axis=1)
    accuracy = 1 - (absolute / maximum)

    # First: Cleanup all infinity values
    accuracy = accuracy.replace([np.inf, -np.inf], np.nan)

    # Second: If maximum is zero, worst we can have is zero accuracy, so replace cases of division-by-zero.
    accuracy[maximum == 0] = 0

    # Third: Ensure best case, if prediction matches actual value, is always 100% accuracy (e.g. for 0/0 case).
    accuracy[forecast["Actual"] == forecast["Prediction_Post"]] = 1

    return accuracy


def compute_accuracy_as_sql(
    actual: Union[int, Column[int]], prediction: Column[float]
) -> BinaryExpression[Any, Any, Any]:
    """Generate sqlalchemy expression that computes accuracy from predicted and actual values.

    Args:
        actual: Column with actual value
        prediction: Column with predicted value

    Returns:
        BinaryExpression used for database update with sqlalchemy

    """
    return 1 - (  # type: ignore
        func.abs(actual - prediction)
        / case([(actual == prediction, 1), (actual > prediction, actual)], else_=prediction)  # type: ignore
    )
