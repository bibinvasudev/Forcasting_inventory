import logging
from multiprocessing.queues import Queue
from typing import (
    Any,
    Dict,
    Iterator,
    List,
    Optional,
    cast,
)
from unittest.mock import Mock

import numpy as np
import pandas as pd
import pytest
from _pytest.logging import LogCaptureFixture
from _pytest.monkeypatch import MonkeyPatch
from click.testing import CliRunner
from forecasting_platform.internal_schema import (
    CleanedData,
    ForecastData,
    ForecastModelRun,
    ForecastRun,
)
from forecasting_platform.services import (
    Database,
    DataLoader,
    DataOutput,
    Orchestrator,
    RuntimeConfig,
)
from forecasting_platform.static import (
    ConfigurationException,
    DatabaseType,
    DataException,
    EngineRunType,
)
from forecasting_platform.test_utils import (
    delete_test_data,
    get_forecast_run,
)
from sqlalchemy.orm import Query

from .preprocess import (
    clean_input_data,
    filter_sales,
    preprocess_grouped_sales,
    update_forecast_data_with_cleaned_data_sales,
    validate_input_data,
)

TEST_ACCOUNT = f'Account_{__name__.split(".")[-1]}'
TEST_CONTRACT = f'Contract_{__name__.split(".")[-1]}'

EMPTY_DATAFRAME = pd.DataFrame(
    {
        "Date": pd.Series([], dtype="datetime64[ns]"),
        "Contract_ID": pd.Series([], dtype="object"),
        "Item_ID": pd.Series([], dtype=np.int64),
        "Unit_Cost": pd.Series([], dtype=np.float64),
        "Order_Quantity": pd.Series([], dtype=np.float64),
    }
)


def test_filter_sales() -> None:
    sales = pd.DataFrame(
        {
            "Project_ID": ["proj0", "proj1", "proj2"],
            "Contract_ID": ["contr0", "contr1", "contr2"],
            "Item_ID": [11, 37, 101],
        }
    )
    assert list(filter_sales(sales, only_include_projects=["proj2"]).index) == [2]
    assert list(filter_sales(sales, exclude_projects=["proj1"]).index) == [0, 2]

    assert list(filter_sales(sales, only_include_items=[11]).index) == [0]
    assert list(filter_sales(sales, exclude_items=[11, 37]).index) == [2]

    assert list(filter_sales(sales, exclude_projects=["proj1"], exclude_items=[37]).index) == [0, 2]
    assert list(filter_sales(sales, exclude_projects=["proj1"], only_include_items=[37, 101]).index) == [2]

    assert list(filter_sales(sales).index) == [0, 1, 2]
    assert list(filter_sales(sales, exclude_projects=["proj0", "proj1", "proj2"]).index) == []
    assert list(filter_sales(sales, only_include_projects=[]).index) == [0, 1, 2]


@pytest.mark.parametrize(
    "inputs,expected_rows",
    [(list(), 0), (None, 1), ([{"Item_ID": 1}, {"Item_ID": 2}], 2),],
    ids=["empty df", "one default row", "two custom rows"],
)  # type: ignore
def test_generate_df_for_preprocessing(inputs, expected_rows) -> None:
    df = _generate_test_df_for_preprocessing(inputs)
    assert len(df.index) == expected_rows


def test_not_enough_training_data_in_forecast_period(cli_runner: CliRunner) -> None:
    data = _generate_test_df_for_preprocessing()
    with pytest.raises(ConfigurationException, match="No training data available for chosen forecast period."):
        preprocess_grouped_sales(
            sales=data,
            fc_start=pd.Timestamp(2019, 11, 1),
            test_periods=1,
            train_start=pd.Timestamp(2019, 2, 1),
            grouping=["Contract_ID", "Account_ID"],
            sales_min_period=5,
        )
    with pytest.raises(ConfigurationException, match="Not enough training data available for chosen forecast period."):
        preprocess_grouped_sales(
            sales=data,
            fc_start=pd.Timestamp(2020, 1, 1),
            test_periods=1,
            train_start=pd.Timestamp(2019, 2, 1),
            grouping=["Contract_ID", "Account_ID"],
            sales_min_period=5,
        )


def _generate_test_df_for_preprocessing(optional_data: Optional[List[Dict[str, Any]]] = None) -> pd.DataFrame:
    if optional_data is None:
        optional_data = [dict()]

    def _get_values_for_column(key: str, default_value: Any) -> List[Any]:
        return [row.get(key, default_value) for row in optional_data]  # type: ignore

    data = {
        "Date": pd.Series(_get_values_for_column("Date", "2019-12-01"), dtype="datetime64[ns]"),
        "Contract_ID": pd.Series(_get_values_for_column("Contract_ID", "Contract1"), dtype="object"),
        "Item_ID": pd.Series(_get_values_for_column("Item_ID", 123), dtype=np.int64),
        "Unit_Cost": pd.Series(_get_values_for_column("Unit_Cost", 1.0), dtype=np.float64),
        "Order_Quantity": pd.Series(_get_values_for_column("Order_Quantity", 1.0), dtype=np.float64),
    }
    return pd.DataFrame(data)


def test_clean_input_data(caplog: LogCaptureFixture) -> None:
    caplog.set_level(logging.INFO)
    test_dsx_data = pd.DataFrame(
        {
            "lowlevelcust": ["Account_0", "Account_0"],  # TODO FSC-371 Remove lowlevelcust again
            "projectid": ["Project_123", "Project_123"],
            "contractid": ["Contract_123", "Contract_123"],
            "shortid": [12345, 12345],
            "perioddate": [pd.Timestamp("2020-02-01"), pd.Timestamp("2019-12-01")],
            "Cost": [1.5, 3.0],
            "Adjusted History": [-2.0, 1.5],
            "masterpart": ["mn1234", "mn1234"],
        }
    )
    expected_data = pd.DataFrame(
        {
            "Project_ID": ["Project_123", "Project_123"],
            "Contract_ID": ["Contract_123", "Contract_123"],
            "Item_ID": [12345, 12345],
            "Date": [pd.Timestamp("2020-02-01"), pd.Timestamp("2019-12-01")],
            "Date_YYYYMM": [202002, 201912],
            "Unit_Cost": [1.5, 3.0],
            "Order_Cost": [0.0, 4.5],
            "Order_Quantity": [0.0, 1.5],
            "Wesco_Master_Number": ["mn1234", "mn1234"],
        }
    )
    pd.testing.assert_frame_equal(expected_data, clean_input_data(test_dsx_data))
    assert "Set 1 negative sales to zero" in caplog.messages


TEST_INVALID_DSX_DATA = [
    # negative cost
    pd.DataFrame(
        {
            "lowlevelcust": ["Account_0", "Account_0"],  # TODO FSC-371 Remove lowlevelcust again
            "projectid": ["Project_0", "Project_0"],
            "contractid": ["Contract_0", "Contract_0"],
            "shortid": [1234, 1234],
            "perioddate": [pd.Timestamp("2020-02-01"), pd.Timestamp("2017-05-01")],
            "Cost": [-1.0, 2.3],
            "Adjusted History": [1.3, 0.3],
            "masterpart": ["mn0", "mn0"],
        }
    ),
    # invalid date
    pd.DataFrame(
        {
            "lowlevelcust": ["Account_0", "Account_0"],  # TODO FSC-371 Remove lowlevelcust again
            "projectid": ["Project_0", "Project_0"],
            "contractid": ["Contract_0", "Contract_0"],
            "shortid": [1234, 1234],
            "perioddate": [pd.Timestamp("2020-01-13"), pd.Timestamp("2017-05-01")],
            "Cost": [1.0, 2.3],
            "Adjusted History": [1.3, 0.3],
            "masterpart": ["mn0", "mn0"],
        }
    ),
    # exceed int32 limit
    pd.DataFrame(
        {
            "lowlevelcust": ["Account_1", "Account_1"],  # TODO FSC-371 Remove lowlevelcust again
            "projectid": ["Project_0", "Project_0"],
            "contractid": ["Contract_0", "Contract_0"],
            "shortid": [1234, 1234],
            "perioddate": [pd.Timestamp("2020-02-01"), pd.Timestamp("2017-05-01")],
            "Cost": [1.0, 2.3],
            "Adjusted History": [2.2e9, 0.3],
            "masterpart": ["mn0", "mn0"],
        }
    ),
]


@pytest.mark.parametrize(
    "test_dsx_data",
    TEST_INVALID_DSX_DATA,
    ids=["negative costs", "invalid dates", "exceeding 32 bit integer maximum value"],
)  # type: ignore
def test_validate_incorrect_input_data(test_dsx_data: pd.DataFrame, request: Any) -> None:
    with pytest.raises(DataException, match=request.node.callspec.id):
        validate_input_data(test_dsx_data)


DSX_DATA_TEMPLATE = {
    "lowlevelcust": ["Account_1"],  # TODO FSC-371 Remove lowlevelcust again
    "projectid": ["Project_0"],
    "contractid": ["Contract_0"],
    "shortid": [1234],
    "perioddate": [pd.Timestamp("2020-02-01")],
    "Cost": [1.0],
    "Adjusted History": [0.3],
    "masterpart": ["mn0"],
}


@pytest.mark.parametrize("column_with_null_value", DSX_DATA_TEMPLATE.keys())  # type: ignore
def test_validate_null_values_in_input_data(column_with_null_value: str) -> None:
    test_dsx_data = DSX_DATA_TEMPLATE.copy()
    test_dsx_data[column_with_null_value] = [None]

    with pytest.raises(DataException, match=column_with_null_value):
        validate_input_data(pd.DataFrame(test_dsx_data))


def test_validate_correct_input_data() -> None:
    test_dsx_data = pd.DataFrame(
        {
            "lowlevelcust": ["Account_0", "Account_0"],  # TODO FSC-371 Remove lowlevelcust again
            "projectid": ["Project_0", "Project_0"],
            "contractid": ["Contract_0", "Contract_0"],
            "shortid": [1234, 1234],
            "perioddate": [pd.Timestamp("2020-02-01"), pd.Timestamp("2017-05-01")],
            "Cost": [1.0, 2.1],
            "Adjusted History": [1.3, 0.3],
            "masterpart": ["mn0", "mn0"],
        }
    )
    pd.testing.assert_frame_equal(test_dsx_data, validate_input_data(test_dsx_data))


@pytest.fixture()  # type: ignore
def with_cleaned_and_forecast_data_in_database() -> Iterator[Orchestrator]:
    cleanup_cleaned_data_query = Query(CleanedData).filter(CleanedData.c.Contract_ID == TEST_CONTRACT)  # type: ignore
    cleanup_forecast_data_query = Query(ForecastData).filter(
        ForecastData.c.Contract_ID == TEST_CONTRACT
    )  # type: ignore
    delete_test_data(cleanup_cleaned_data_query)  # Cleanup in case of previously failed test
    delete_test_data(cleanup_forecast_data_query)  # Cleanup in case of previously failed test

    with Database(DatabaseType.internal).transaction_context() as session:
        cleaned_data_count = cleanup_cleaned_data_query.with_session(session).count()
        forecast_data_count = cleanup_forecast_data_query.with_session(session).count()
    assert cleaned_data_count + forecast_data_count == 0, "Found old test data in database when setting up the test"

    runtime_config = RuntimeConfig(
        engine_run_type=EngineRunType.development,
        forecast_periods=1,
        output_location=".",
        prediction_month=pd.Timestamp(year=2020, month=2, day=1),
    )

    orchestrator = Orchestrator(
        runtime_config,
        Mock(spec=DataLoader),
        Mock(spec=DataOutput),
        Database(DatabaseType.internal),
        Mock(spec=Queue),
        Mock(),
        Mock(),
    )

    test_cleaned_data = _setup_cleaned_data(orchestrator)
    test_forecast_data = _setup_forecast_data(orchestrator)
    test_run_id = cast(int, orchestrator._forecast_run_id)

    with Database(DatabaseType.internal).transaction_context() as session:
        session.execute(CleanedData.insert().values(test_cleaned_data))
        session.execute(ForecastData.insert().values(test_forecast_data))

    yield orchestrator

    assert delete_test_data(cleanup_cleaned_data_query) == len(test_cleaned_data)
    assert delete_test_data(cleanup_forecast_data_query) == len(test_forecast_data)
    assert delete_test_data(Query(ForecastRun).filter(ForecastRun.id == test_run_id)) == 1  # type: ignore


def _setup_cleaned_data(orchestrator: Orchestrator) -> List[Dict[str, Any]]:
    orchestrator._initialize_forecast_run()
    test_run_id = cast(int, orchestrator._forecast_run_id)

    return [
        {
            "run_id": test_run_id,
            "Project_ID": "Test_Project",
            "Contract_ID": TEST_CONTRACT,
            "Wesco_Master_Number": "Test_Master_Number",
            "Date": "2019-12-01 00:00:00.000",
            "Date_YYYYMM": 201912,
            "Item_ID": -1,
            "Unit_Cost": 0.014394184847748253,
            "Order_Quantity": 172,
            "Order_Cost": 2.4757997938126994,
        },
        {
            "run_id": test_run_id,
            "Project_ID": "Test_Project",
            "Contract_ID": TEST_CONTRACT,
            "Wesco_Master_Number": "Test_Master_Number",
            "Date": "2020-01-01 00:00:00.000",
            "Date_YYYYMM": 202001,
            "Item_ID": -1,
            "Unit_Cost": 0.014394184847748253,
            "Order_Quantity": 0,
            "Order_Cost": 2.4757997938126994,
        },
        {
            "run_id": test_run_id,
            "Project_ID": "Test_Project",
            "Contract_ID": TEST_CONTRACT,
            "Wesco_Master_Number": "Test_Master_Number",
            "Date": "2020-02-01 00:00:00.000",
            "Date_YYYYMM": 202002,
            "Item_ID": -1,
            "Unit_Cost": 0.014394184847748253,
            "Order_Quantity": 86,
            "Order_Cost": 2.4757997938126994,
        },
        {
            "run_id": test_run_id,
            "Project_ID": "Test_Project",
            "Contract_ID": TEST_CONTRACT,
            "Wesco_Master_Number": "Test_Master_Number",
            "Date": "2020-03-01 00:00:00.000",
            "Date_YYYYMM": 202003,
            "Item_ID": -2,
            "Unit_Cost": 1,
            "Order_Quantity": 2,
            "Order_Cost": 2,
        },
    ]


def _setup_forecast_data(orchestrator: Orchestrator) -> List[Dict[str, Any]]:
    test_run_id = get_forecast_run(orchestrator)
    assert test_run_id is not None, "Make sure orchestrator._initialize_forecast_run() has been called"

    test_model_run_id = _get_forecast_model_run_id(
        Orchestrator._initialize_forecast_model_run(
            model_name="Account_Dummy",
            forecast_run_id=test_run_id.id,
            internal_database=orchestrator._internal_database,
        )
    )
    return [
        {
            "model_run_id": test_model_run_id,
            "Contract_ID": TEST_CONTRACT,
            "Item_ID": -1,
            "Prediction_Start_Month": 201912,
            "Predicted_Month": predicted_month,
            "Prediction_Months_Delta": 0,
            "Prediction_Raw": 288.0242537350653,
            "Prediction_Post": 172,
            "Actual": None,
            "Accuracy": None,
        }
        for predicted_month in [201912, 202001, 202002, 202003, 299912]
    ]


def _get_forecast_model_run_id(forecast_model_run: ForecastModelRun) -> int:
    with Database(DatabaseType.internal).transaction_context() as session:
        forecast_model_run = session.merge(forecast_model_run)
        return forecast_model_run.id


def test_update_forecast_data_during_cleaning(
    with_cleaned_and_forecast_data_in_database: Orchestrator, monkeypatch: MonkeyPatch, caplog: LogCaptureFixture
) -> None:
    caplog.set_level(logging.INFO)

    expected_forecast_data = [
        {
            "Contract_ID": "Contract_preprocess_test",
            "Predicted_Month": 201912,
            "Prediction_Post": 172.0,
            "Actual": 172,
            "Accuracy": 1.0,
        },
        {
            "Contract_ID": "Contract_preprocess_test",
            "Predicted_Month": 202001,
            "Prediction_Post": 172.0,
            "Actual": 0,
            "Accuracy": 0.0,
        },
        {
            "Contract_ID": "Contract_preprocess_test",
            "Predicted_Month": 202002,
            "Prediction_Post": 172.0,
            "Actual": 86,
            "Accuracy": 0.5,
        },
        {
            "Contract_ID": "Contract_preprocess_test",
            "Predicted_Month": 202003,
            "Prediction_Post": 172.0,
            "Actual": 0,
            "Accuracy": 0,
        },
        {
            # predicted month so far in the future can not represent a gap in cleaned_data
            # and thus it's actual value will not be replaced with zero during update
            "Contract_ID": "Contract_preprocess_test",
            "Predicted_Month": 299912,
            "Prediction_Post": 172.0,
            "Actual": None,
            "Accuracy": None,
        },
    ]

    # Simulate cleaning
    with_cleaned_and_forecast_data_in_database._set_cleaned_data_attributes()
    update_forecast_data_with_cleaned_data_sales(
        with_cleaned_and_forecast_data_in_database._internal_database,
        with_cleaned_and_forecast_data_in_database._cleaned_data_run_id,
        with_cleaned_and_forecast_data_in_database._cleaned_data_newest_month,
    )

    with Database(DatabaseType.internal).transaction_context() as session:
        updated_forecast_data = [
            row._asdict()
            for row in session.query(  # type: ignore
                ForecastData.c.Contract_ID,
                ForecastData.c.Predicted_Month,
                ForecastData.c.Prediction_Post,
                ForecastData.c.Actual,
                ForecastData.c.Accuracy,
            )
            .filter(ForecastData.c.Contract_ID == TEST_CONTRACT)
            .all()
        ]
        assert updated_forecast_data == expected_forecast_data

    assert (
        "Updated 3 rows of forecast_data with old actual values to newest actual values from cleaned_data"
        in caplog.messages
    )
    assert (
        "Updated 1 rows of forecast_data without actual values to newest actual values from cleaned_data"
        in caplog.messages
    )


def test_do_not_update_forecast_data_with_disabled_database(
    with_cleaned_and_forecast_data_in_database: Orchestrator, monkeypatch: MonkeyPatch, caplog: LogCaptureFixture
) -> None:
    caplog.set_level(logging.WARNING)

    monkeypatch.setattr(with_cleaned_and_forecast_data_in_database._internal_database, "is_disabled", lambda: True)
    update_forecast_data_with_cleaned_data_sales(
        with_cleaned_and_forecast_data_in_database._internal_database,
        with_cleaned_and_forecast_data_in_database._cleaned_data_run_id,
        with_cleaned_and_forecast_data_in_database._cleaned_data_newest_month,
    )

    with Database(DatabaseType.internal).transaction_context() as session:
        updated_forecast_data = [
            row._asdict()
            for row in session.query(  # type: ignore
                ForecastData.c.Contract_ID,
                ForecastData.c.Predicted_Month,
                ForecastData.c.Prediction_Post,
                ForecastData.c.Actual,
                ForecastData.c.Accuracy,
            )
            .filter(ForecastData.c.Contract_ID == TEST_CONTRACT)
            .all()
        ]

    assert updated_forecast_data == [
        {
            "Contract_ID": "Contract_preprocess_test",
            "Predicted_Month": 201912,
            "Prediction_Post": 172.0,
            "Actual": None,
            "Accuracy": None,
        },
        {
            "Contract_ID": "Contract_preprocess_test",
            "Predicted_Month": 202001,
            "Prediction_Post": 172.0,
            "Actual": None,
            "Accuracy": None,
        },
        {
            "Contract_ID": "Contract_preprocess_test",
            "Predicted_Month": 202002,
            "Prediction_Post": 172.0,
            "Actual": None,
            "Accuracy": None,
        },
        {
            "Contract_ID": "Contract_preprocess_test",
            "Predicted_Month": 202003,
            "Prediction_Post": 172.0,
            "Actual": None,
            "Accuracy": None,
        },
        {
            "Contract_ID": "Contract_preprocess_test",
            "Predicted_Month": 299912,
            "Prediction_Post": 172.0,
            "Actual": None,
            "Accuracy": None,
        },
    ]

    assert "Skipping update of previous forecasts due to disabled database" in caplog.messages
