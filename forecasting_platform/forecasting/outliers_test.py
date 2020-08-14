import pandas as pd
from pandas.testing import assert_frame_equal

from .outliers import adjust_outliers_to_standard_deviation


def test_adjust_outliers_calculation() -> None:
    df = pd.DataFrame(
        {
            "value": [-5.0, -3.0, -1.0, 0.0, 1.0, 3.0, 5.0],
            "var": [9.0, 4.0, 1.0, 0.0, 1.0, 4.0, 9.0],
            "data": [-5.0, -6.0, -5.0, 10.0, 3.0, 0.0, 6.0],
        }
    )
    result = adjust_outliers_to_standard_deviation(df, "data")
    assert_frame_equal(df.assign(data=[-5.0, -5.0, -2.0, 0.0, 2.0, 1.0, 6.0]), result)


def test_adjust_outliers_empty() -> None:
    df = pd.DataFrame({"value": [], "var": [], "data": []})
    result = adjust_outliers_to_standard_deviation(df, "data")
    assert_frame_equal(df, result)
