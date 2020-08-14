import logging
from datetime import datetime

import pandas as pd
import pytest
from _pytest.logging import LogCaptureFixture

from .feature_engineering import (
    calculate_weights_shock,
    generate_airframe_feature,
    generate_build_rates_features,
    generate_market_shock_feature,
)

BUILD_RATES_AGGREGATION_WARNING = (
    "Aggregation of build rates by Contract_ID, Airframe and Date is done under assumption that "
    "the build rate value for different Project_IDs stays the same, "
    "but this assumption does not hold and can affect accuracy (proceeding with mean build rate value)"
)

INPUT_BUILD_RATES = pd.DataFrame(
    [
        ["Project_1", "Contract_1", "Airframe_1", "Build Rate", datetime(2015, 1, 1), 1.0],
        ["Project_2", "Contract_1", "Airframe_1", "Build Rate", datetime(2015, 1, 1), 1.0],
        ["Project_3", "Contract_1", "Airframe_2", "Build Rate", datetime(2016, 1, 1), 2.0],
        ["Project_1", "Contract_1", "Airframe_1", "Build Rate", datetime(2017, 1, 1), 3.0],
    ],
    columns=["Project_ID", "Contract_ID", "Airframe", "Periodic_Data_Stream", "Date", "Value"],
)

INPUT_AIRFRAMES = pd.DataFrame(
    [
        ["Project_1", "Contract_1", "Airframe_1", "Airframe Map", datetime(1900, 1, 1), 0.0],
        ["Project_2", "Contract_1", "Airframe_1", "Airframe Map", datetime(1900, 1, 1), 0.0],
        ["Project_3", "Contract_1", "Airframe_2", "Airframe Map", datetime(1900, 1, 1), 0.0],
        ["Project_1", "Contract_1", "Airframe_1", "Airframe Map", datetime(1900, 1, 1), 0.0],
    ],
    columns=["Project_ID", "Contract_ID", "Airframe", "Periodic_Data_Stream", "Date", "Value"],
)

INPUT_SALES = pd.DataFrame(
    [
        [1, "Project_1", "Contract_1", datetime(2015, 1, 1)],
        [2, "Project_3", "Contract_1", datetime(2016, 1, 1)],
        [3, "Project_1", "Contract_2", datetime(2016, 1, 1)],
    ],
    columns=["Item_ID", "Project_ID", "Contract_ID", "Date"],
)


def test_generate_build_rates_features(caplog: LogCaptureFixture) -> None:
    caplog.set_level(logging.WARNING)

    internal_features, exogenous_features = generate_build_rates_features(INPUT_BUILD_RATES)
    assert "Airframe" in internal_features.keys()
    assert "Build_Rate" in exogenous_features.keys()

    generated_build_rates, default_build_rate_value = exogenous_features["Build_Rate"]
    expected_build_rates = pd.DataFrame(
        [
            ["Contract_1", "Airframe_1", datetime(2015, 1, 1), 1.0],
            ["Contract_1", "Airframe_1", datetime(2017, 1, 1), 3.0],
            ["Contract_1", "Airframe_2", datetime(2016, 1, 1), 2.0],
        ],
        columns=["Contract_ID", "Airframe", "Date", "Value"],
    )

    assert default_build_rate_value == 1
    assert BUILD_RATES_AGGREGATION_WARNING not in caplog.messages

    pd.testing.assert_frame_equal(expected_build_rates, generated_build_rates)

    expected_sales = pd.DataFrame(
        [
            [1, "Project_1", "Contract_1", datetime(2015, 1, 1), "Airframe_1"],
            [2, "Project_3", "Contract_1", datetime(2016, 1, 1), "Airframe_2"],
            [3, "Project_1", "Contract_2", datetime(2016, 1, 1), "No Airframe"],
        ],
        columns=["Item_ID", "Project_ID", "Contract_ID", "Date", "Airframe"],
    )

    sales_with_airframe_flag = internal_features["Airframe"](INPUT_SALES, name="Airframe")  # type: ignore
    pd.testing.assert_frame_equal(expected_sales, sales_with_airframe_flag)


def test_generate_build_rates_features_aggregation_warning(caplog: LogCaptureFixture) -> None:
    caplog.set_level(logging.WARNING)

    corrupted_input_build_rates = pd.DataFrame(
        [
            ["Project_1", "Contract_1", "Airframe_1", "Build Rate", datetime(2015, 1, 1), 1.0],
            ["Project_2", "Contract_1", "Airframe_1", "Build Rate", datetime(2015, 1, 1), 10.0],
            ["Project_3", "Contract_1", "Airframe_2", "Build Rate", datetime(2016, 1, 1), 2.0],
            ["Project_1", "Contract_1", "Airframe_1", "Build Rate", datetime(2017, 1, 1), 3.0],
        ],
        columns=["Project_ID", "Contract_ID", "Airframe", "Periodic_Data_Stream", "Date", "Value"],
    )

    internal_features, exogenous_features = generate_build_rates_features(corrupted_input_build_rates)
    assert "Airframe" in internal_features.keys()
    assert "Build_Rate" in exogenous_features.keys()

    generated_build_rates, default_build_rate_value = exogenous_features["Build_Rate"]
    expected_build_rates = pd.DataFrame(
        [
            # 5.5 is a mean of build rate value after aggregating first two input rows that differ only by Project_ID
            ["Contract_1", "Airframe_1", datetime(2015, 1, 1), 5.5],
            ["Contract_1", "Airframe_1", datetime(2017, 1, 1), 3.0],
            ["Contract_1", "Airframe_2", datetime(2016, 1, 1), 2.0],
        ],
        columns=["Contract_ID", "Airframe", "Date", "Value"],
    )

    assert default_build_rate_value == 1
    assert BUILD_RATES_AGGREGATION_WARNING in caplog.messages
    pd.testing.assert_frame_equal(expected_build_rates, generated_build_rates)


def test_generate_airframe_feature() -> None:
    internal_features = generate_airframe_feature(INPUT_AIRFRAMES)
    assert "Airframe_Feature" in internal_features.keys()

    expected_sales = pd.DataFrame(
        [
            [1, "Project_1", "Contract_1", datetime(2015, 1, 1), "Airframe_1"],
            [2, "Project_3", "Contract_1", datetime(2016, 1, 1), "Airframe_2"],
            [3, "Project_1", "Contract_2", datetime(2016, 1, 1), "No Airframe"],
        ],
        columns=["Item_ID", "Project_ID", "Contract_ID", "Date", "Airframe_Feature"],
    )

    sales_with_airframe_flag = internal_features["Airframe_Feature"](
        INPUT_SALES, name="Airframe_Feature"  # type: ignore
    )
    pd.testing.assert_frame_equal(expected_sales, sales_with_airframe_flag)


def test_calculate_weights_shock() -> None:
    weights = calculate_weights_shock(
        train_start=datetime(2015, 1, 1),
        forecast_end=datetime(2015, 5, 1),
        first_shock_month=datetime(2015, 2, 1),
        last_shock_month=datetime(2015, 3, 1),
        decrease_factor=0.1,
    )

    pd.testing.assert_frame_equal(
        weights,
        pd.DataFrame(
            [
                {"Date": datetime(2015, 1, 1), "Weight": 1.0},
                {"Date": datetime(2015, 2, 1), "Weight": 1000.0},
                {"Date": datetime(2015, 3, 1), "Weight": 1000.0},
                {"Date": datetime(2015, 4, 1), "Weight": 100.0},
                {"Date": datetime(2015, 5, 1), "Weight": 10.0},
            ],
        ),
    )


def test_calculate_weights_shock_default_decrease_factor() -> None:
    weights = calculate_weights_shock(
        train_start=datetime(2015, 1, 1),
        forecast_end=datetime(2015, 5, 1),
        first_shock_month=datetime(2015, 2, 1),
        last_shock_month=datetime(2015, 3, 1),
    )

    pd.testing.assert_frame_equal(
        weights,
        pd.DataFrame(
            [
                {"Date": datetime(2015, 1, 1), "Weight": 1.0},
                {"Date": datetime(2015, 2, 1), "Weight": 1000.0},
                {"Date": datetime(2015, 3, 1), "Weight": 1000.0},
                {"Date": datetime(2015, 4, 1), "Weight": 700.0},
                {"Date": datetime(2015, 5, 1), "Weight": 490.0},
            ],
        ),
    )


@pytest.mark.parametrize(
    "first_shock_month,last_shock_month",
    [
        (datetime(2015, 2, 1), datetime(2014, 12, 1)),
        (datetime(2014, 12, 1), datetime(2014, 12, 1)),
        (datetime(2015, 2, 1), datetime(2015, 1, 1)),
    ],
    ids=[
        "last_shock_months_before_train_start",
        "both_shock_months_before_train_start",
        "last_shock_month_before_first_shock_month",
    ],
)  # type: ignore
def test_calculate_weights_shock_invalid_shock_months(first_shock_month: datetime, last_shock_month: datetime) -> None:
    with pytest.raises(AssertionError, match=".*required to be set chronologically.*"):
        calculate_weights_shock(
            train_start=datetime(2015, 1, 1),
            forecast_end=datetime(2015, 5, 1),
            first_shock_month=first_shock_month,
            last_shock_month=last_shock_month,
            decrease_factor=0.1,
        )


def test_calculate_weights_shock_single_shock_month() -> None:
    weights = calculate_weights_shock(
        train_start=datetime(2015, 1, 1),
        forecast_end=datetime(2015, 1, 1),
        first_shock_month=datetime(2015, 1, 1),
        last_shock_month=datetime(2015, 1, 1),
        decrease_factor=0.1,
    )

    pd.testing.assert_frame_equal(
        weights, pd.DataFrame([{"Date": datetime(2015, 1, 1), "Weight": 1000.0}]),
    )


def test_calculate_weights_for_shock_after_forecast_end() -> None:
    weights = calculate_weights_shock(
        train_start=datetime(2015, 1, 1),
        forecast_end=datetime(2015, 1, 1),
        first_shock_month=datetime(2015, 2, 1),
        last_shock_month=datetime(2015, 2, 1),
        decrease_factor=0.1,
    )

    pd.testing.assert_frame_equal(
        weights, pd.DataFrame([{"Date": datetime(2015, 1, 1), "Weight": 1.0}]),
    )


def test_generate_market_shock_feature() -> None:
    shock_feature = generate_market_shock_feature(
        train_start=datetime(2015, 1, 1), first_shock_month=datetime(2015, 2, 1), impact_duration=2
    )

    pd.testing.assert_frame_equal(
        shock_feature,
        pd.DataFrame(
            [
                {"Date": datetime(2015, 1, 1), "Shock_Feature": 0.0},
                {"Date": datetime(2015, 2, 1), "Shock_Feature": 1.0},
                {"Date": datetime(2015, 3, 1), "Shock_Feature": 1.0},
                {"Date": datetime(2015, 4, 1), "Shock_Feature": 1.0},
            ],
        ),
    )


def test_generate_market_shock_feature_first_shock_month_before_train_start() -> None:
    shock_feature = generate_market_shock_feature(
        train_start=datetime(2015, 1, 1), first_shock_month=datetime(2014, 12, 1), impact_duration=2
    )

    pd.testing.assert_frame_equal(
        shock_feature,
        pd.DataFrame(
            [
                {"Date": datetime(2015, 1, 1), "Shock_Feature": 1.0},
                {"Date": datetime(2015, 2, 1), "Shock_Feature": 1.0},
                {"Date": datetime(2015, 3, 1), "Shock_Feature": 0.0},
            ],
        ),
    )


def test_generate_market_shock_feature_default_impact_duration() -> None:
    shock_feature = generate_market_shock_feature(
        train_start=datetime(2015, 1, 1), first_shock_month=datetime(2015, 1, 1),
    )

    pd.testing.assert_frame_equal(
        shock_feature,
        pd.DataFrame(
            {
                "Date": pd.date_range(start=datetime(2015, 1, 1), end=datetime(2114, 12, 1), freq="MS"),
                "Shock_Feature": 1.0,
            }
        ),
    )
