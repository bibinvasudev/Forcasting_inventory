import numpy as np
import pandas as pd


def adjust_outliers_to_standard_deviation(input_df: pd.DataFrame, column: str) -> pd.DataFrame:
    """Adjust outliers which exceed the standard deviation of the "value" column, to be within that deviation.

    Args:
        input_df: Input :class:`~pandas.DataFrame` containing "value" and "var" columns.
        column: Column, in which the outliers will be adjusted.

    Returns:
        ``input_df`` with adjusted outliers.

    """
    expected_columns = {column, "value", "var"}
    assert expected_columns.issubset(
        set(input_df.columns)
    ), f'Expected columns: {", ".join(expected_columns)}, but got {input_df.columns}'

    df = input_df.copy()
    del input_df  # Prevent accidental usage of input DataFrame

    df[column] = np.where(
        (df[column] > df["value"] + np.sqrt(df["var"])), df["value"] + np.sqrt(df["var"]), df[column],
    )
    df[column] = np.where(
        (df[column] < df["value"] - np.sqrt(df["var"])), df["value"] - np.sqrt(df["var"]), df[column],
    )

    return df
