import numpy as np
import pandas as pd
from pandas._testing import assert_series_equal
from sqlalchemy import (
    Column,
    Float,
    Integer,
)
from sqlalchemy.dialects import mssql

from .reporting import (
    compute_accuracy,
    compute_accuracy_as_sql,
)


def test_compute_accuracy() -> None:
    forecast = pd.DataFrame(
        {
            "Prediction_Post": [1.0, 2.0, 0.0, 0.0, 1.0, 0.0, -1.0, -1.0],
            "Actual": [2.0, 1.0, 0.0, 1.0, 0.0, -1.0, 0.0, -1.0],
        }
    )
    assert_series_equal(compute_accuracy(forecast), pd.Series([0.5, 0.5, 1.0, 0.0, 0.0, 0.0, 0.0, 1.0]))


def test_compute_accuracy_100_percent() -> None:
    series = [-1e100, -1.0, -0.1, 0.0, 0.1, 1.0, 1e100]

    forecast = pd.DataFrame({"Prediction_Post": series, "Actual": series})
    assert_series_equal(compute_accuracy(forecast), pd.Series([1.0] * len(series)))


def test_compute_accuracy_0_percent() -> None:
    series = [-1e100, -1.0, -0.1, 0.1, 1.0, 1e100]

    forecast = pd.DataFrame({"Prediction_Post": series, "Actual": [0] * len(series)})
    assert_series_equal(compute_accuracy(forecast), pd.Series([0.0] * len(series)))

    forecast = pd.DataFrame({"Prediction_Post": [0] * len(series), "Actual": series})
    assert_series_equal(compute_accuracy(forecast), pd.Series([0.0] * len(series)))


def test_compute_accuracy_with_nan_values() -> None:
    forecast = pd.DataFrame(
        {
            "Prediction_Post": [1, None, None, np.nan, 1, np.nan, np.nan, None],
            "Actual": [None, 1, None, np.nan, np.nan, 1, None, np.nan],
        }
    )
    assert_series_equal(compute_accuracy(forecast), pd.Series(np.nan, index=range(len(forecast))))


def test_compute_accuracy_as_sql() -> None:
    expected_expression = (
        "1 - abs([Actual] - [Prediction]) / CASE WHEN ([Actual] = [Prediction]) THEN "
        "1 WHEN ([Actual] > [Prediction]) THEN [Actual] ELSE [Prediction] END"
    )

    assert (
        str(
            compute_accuracy_as_sql(
                Column("Actual", Integer, nullable=False), Column("Prediction", Float, nullable=False)
            ).compile(
                compile_kwargs={"literal_binds": True}, dialect=mssql.dialect()  # type: ignore
            )
        )
        == expected_expression
    )


def test_compute_accuracy_as_sql_int_values() -> None:
    expected_expression = (
        "1 - abs(0 - [Prediction]) / CASE WHEN ([Prediction] = 0)"
        " THEN 1 WHEN ([Prediction] < 0) THEN 0 ELSE [Prediction] END"
    )

    assert (
        str(
            compute_accuracy_as_sql(0, Column("Prediction", Float, nullable=False)).compile(
                compile_kwargs={"literal_binds": True}, dialect=mssql.dialect()  # type: ignore
            )
        )
        == expected_expression
    )
