from __future__ import annotations

import re
from pathlib import Path
from typing import List

import pandas as pd
from forecasting_platform.static import PREDICTION_MONTH_FORMAT

from .assertions import (
    assert_forecast_result_equal,
    assert_same_structure,
)
from .forecast_structure import (
    ExpectedForecastStructureParameters,
    get_expected_forecast_structure,
)


def compare_csv(expected: Path, actual: Path) -> None:
    """Ensure expected and actual CSV files can be considered equal, used in end-to-end testing.

    Args:
        expected: Path to the expected results CSV file.
        actual: Path to the actual results CSV file.

    """
    expected_df = pd.read_csv(expected)
    actual_df = pd.read_csv(actual)
    assert_forecast_result_equal(expected_df, actual_df)

    # Ensure that our custom equality check is not weaker then the built-in pandas assertion
    pd.testing.assert_frame_equal(expected_df, actual_df)


def compare_csv_structure(actual: Path) -> None:
    """Ensure CSV file matches expected :class:`~pandas.DataFrame` structure, used in end-to-end testing.

    Args:
        actual: Path to the actual results CSV file.

    """
    forecast_parameters = _extract_forecast_parameters_from_result_path(actual)
    try:
        expected_structure = get_expected_forecast_structure(forecast_parameters)
    except KeyError as e:
        assert False, f"Could not find expected folder structure for: {e}"

    assert_same_structure(expected_structure, pd.read_csv(actual))


def collect_files_with_extension(path: Path, suffix: str) -> List[Path]:
    """Find all files matching the suffix within the given path recursively, used in end-to-end testing.

    Args:
        path: Directory to check for files.
        suffix: Suffix to match.

    """
    glob = f"**/*{suffix}"
    if path.is_file() and path.match(glob):
        return [path]

    return [file for file in sorted(path.glob(glob)) if file.is_file()]


def _extract_forecast_parameters_from_result_path(actual: Path,) -> ExpectedForecastStructureParameters:
    forecast_periods_dir = actual.parents[0].name
    prediction_start_month_dir = actual.parents[1].name
    account_dir = actual.parents[2].name

    error_msg = (
        f"Could not parse forecast parameters from file path: {actual}\n"
        "Make sure the csv file is located in the folder structure "
        "produced by running forecasting_platform CLI"
    )
    if account_match := re.match(r"Forecast (.*)", str(account_dir)):
        account_name = account_match[1]
    else:
        raise AssertionError(error_msg)

    if forecast_periods_match := re.search(r"_P(\d+)", str(forecast_periods_dir)):
        forecast_periods = int(forecast_periods_match.groups()[0])
    else:
        raise AssertionError(error_msg)

    prediction_start_month = pd.to_datetime(prediction_start_month_dir, format=PREDICTION_MONTH_FORMAT)
    return ExpectedForecastStructureParameters(account_name, int(forecast_periods), prediction_start_month,)
