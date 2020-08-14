from __future__ import annotations

from typing import NamedTuple

import pandas as pd
from forecasting_platform.static import DataFrameStructure

from .identifier import AccountID


class ExpectedForecastStructureParameters(NamedTuple):
    """Container for expected forecast structure, used by integration tests."""

    account_name: str
    forecast_periods: int
    prediction_month: pd.Timestamp


def get_expected_forecast_structure(test_parameters: ExpectedForecastStructureParameters) -> DataFrameStructure:
    """Determine expected forecast structure based on reference data, used in integration and end-to-end testing.

    Args:
        test_parameters: Parameter setting of forecast test.

    Returns:
        Expected structure of forecast results.

    """
    get_expected_rows = {
        ExpectedForecastStructureParameters(account, period, pd.Timestamp(year=year, month=month, day=1)): value
        for (account, period, (year, month)), value in {
            # development run with default prediction start month for integration test
            (AccountID("Account 1"), 1, (2020, 3)): 8840,
            (AccountID("Account 2"), 1, (2020, 3)): 9776,
            (AccountID("Account 3"), 1, (2020, 3)): 14507,
            # development run with prediction start month which includes COVID-19 weighting for integration test
            (AccountID("Account 1"), 1, (2020, 6)): 9286,
            (AccountID("Account 2"), 1, (2020, 6)): 10021,
            (AccountID("Account 3"), 1, (2020, 6)): 14892,
            (AccountID("Account 5"), 1, (2020, 6)): 900,
            (AccountID("Account 6"), 1, (2020, 6)): 460,  # Reduced input for testing adhoc model
            (AccountID("Account 7"), 1, (2020, 6)): 5913,
            (AccountID("Account 9"), 1, (2020, 6)): 1237,
            (AccountID("Account 12"), 1, (2020, 6)): 6026,
            (AccountID("Account 13"), 1, (2020, 6)): 572,
            (AccountID("Account 14"), 1, (2020, 6)): 1175,
            (AccountID("Account 15"), 1, (2020, 6)): 1375,
            (AccountID("Account 16"), 1, (2020, 6)): 2354,
            (AccountID("Account 17"), 1, (2020, 6)): 1242,
            (AccountID("Account 20"), 1, (2020, 6)): 2258,
            (AccountID("Account 29"), 1, (2020, 6)): 520,
            (AccountID("Account 38"), 1, (2020, 6)): 565,
            (AccountID("Account 39"), 1, (2020, 6)): 449,
            (AccountID("Account 44"), 1, (2020, 6)): 2230,
            (AccountID("Account 45"), 1, (2020, 6)): 937,
            (AccountID("Account 54"), 1, (2020, 6)): 1797,
            (AccountID("Account 466"), 1, (2020, 6)): 612,
            (AccountID("Account 468"), 1, (2020, 6)): 777,
            # development/production run with shifted prediction start month for integration test
            (AccountID("Account 5"), 3, (2020, 1)): 2415,
            # development run with shifted prediction start month for end to end test
            (AccountID("Account 1"), 1, (2020, 1)): 8752,
            (AccountID("Account 2"), 1, (2020, 1)): 9580,
            (AccountID("Account 3"), 1, (2020, 1)): 14117,
            (AccountID("Account 5"), 1, (2020, 1)): 805,
            (AccountID("Account 6"), 1, (2020, 1)): 134553,
            (AccountID("Account 7"), 1, (2020, 1)): 5788,
            (AccountID("Account 8"), 1, (2020, 1)): 11239,
            (AccountID("Account 9"), 1, (2020, 1)): 1199,
            (AccountID("Account 10"), 1, (2020, 1)): 2678,
            (AccountID("Account 11"), 1, (2020, 1)): 5053,
            (AccountID("Account 12"), 1, (2020, 1)): 5407,
            (AccountID("Account 13"), 1, (2020, 1)): 413,
            (AccountID("Account 14"), 1, (2020, 1)): 1103,
            (AccountID("Account 15"), 1, (2020, 1)): 1356,
            (AccountID("Account 16"), 1, (2020, 1)): 2301,
            (AccountID("Account 17"), 1, (2020, 1)): 908,
            (AccountID("Account 20"), 1, (2020, 1)): 2189,
            (AccountID("Account 29"), 1, (2020, 1)): 490,
            (AccountID("Account 38"), 1, (2020, 1)): 548,
            (AccountID("Account 39"), 1, (2020, 1)): 415,
            (AccountID("Account 44"), 1, (2020, 1)): 1896,
            (AccountID("Account 45"), 1, (2020, 1)): 905,
            (AccountID("Account 54"), 1, (2020, 1)): 1713,
            (AccountID("Account 466"), 1, (2020, 1)): 605,
            (AccountID("Account 468"), 1, (2020, 1)): 776,
        }.items()
    }

    expected_rows = get_expected_rows[test_parameters]
    expected_forecast_structure = DataFrameStructure(
        columns=pd.Index(
            [
                "Item_ID",
                "Contract_ID",
                "Prediction_Start_Month",
                "Predicted_Month",
                "Prediction_Months_Delta",
                "Prediction_Raw",
                "Prediction_Post",
                "Actual",
                "Accuracy",
            ],
            dtype="object",
        ),
        dtypes=pd.Series(["int64", "object", "int64", "int64", "int64", "float64", "float64", "int64", "float64"]),
        shape=(expected_rows, 9),
    )

    return expected_forecast_structure
