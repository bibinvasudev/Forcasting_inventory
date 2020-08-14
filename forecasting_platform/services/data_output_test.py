import logging
from datetime import datetime
from operator import itemgetter
from pathlib import Path
from typing import (
    Any,
    Dict,
    List,
)

import numpy as np
import pandas as pd
import pytest
from _pytest.logging import LogCaptureFixture
from _pytest.monkeypatch import MonkeyPatch
from forecasting_platform.internal_schema import (
    CleanedData,
    ExogenousFeature,
    ForecastModelRun,
)
from forecasting_platform.model_config_scripts import ModelConfigAccount1
from forecasting_platform.services import (
    Database,
    DataLoader,
    DataOutput,
    RuntimeConfig,
)
from forecasting_platform.static import (
    DatabaseType,
    EngineRunType,
    OutputFormat,
)
from forecasting_platform.test_utils import delete_test_data
from pandas._testing import assert_frame_equal
from sqlalchemy.orm import Query

TEST_FREEZE_TIME = "2010-12-31"

TEST_ACCOUNT_1_POST_DATA = pd.DataFrame(
    [
        ["Contract_402", 1005, 0, "2019-06-01", 0.0, "predict"],
        ["Contract_402", 1005, 1, "2019-06-01", 0.0, "predict"],
        ["Contract_402", 1005, 2, "2019-06-01", 0.0, "predict"],
        ["Contract_402", 1005, 0, "2019-07-01", 3.8, "predict"],
        ["Contract_402", 2178, 0, "2019-06-01", 1.3, "predict"],
        ["Contract_402", 2178, 0, "2019-07-01", 1.4, "predict"],
        ["Contract_402", 2178, 0, "2019-08-01", 1.8, "predict"],
        ["Contract_402", 2178, 0, "2019-09-01", 1.8, "predict"],
        ["Contract_402", 2178, 0, "2019-10-01", 1.8, "predict"],
        ["Contract_402", 2178, 0, "2019-11-01", 1.6, "predict"],
        ["Contract_402", 2178, 0, "2019-12-01", 1.8, "predict"],
        ["Contract_402", 2178, 0, "2020-01-01", 1.6, "predict"],
        ["Contract_402", 2178, 0, "2020-02-01", 1.8, "predict"],
    ],
    columns=["Contract_ID", "Item_ID", "Build_Rate_Flag", "Date", "Order_Quantity", "type"],
)

TEST_ACCOUNT_1_RAW_DATA = pd.DataFrame(
    [
        # "train" values will be filtered out
        ["Contract_402", 1005, 0, "2019-05-01", 0.0, "train"],
        # predictions with exogenous features but same granularity will be summed up
        ["Contract_402", 1005, 0, "2019-06-01", 3.7, "predict"],
        ["Contract_402", 1005, 1, "2019-06-01", 3.7, "predict"],
        ["Contract_402", 1005, 2, "2019-06-01", 3.7, "predict"],
        ["Contract_402", 1005, 0, "2019-07-01", 3.8, "predict"],
        ["Contract_402", 2178, 0, "2019-06-01", 1.3, "predict"],
        ["Contract_402", 2178, 0, "2019-07-01", 1.4, "predict"],
        ["Contract_402", 2178, 0, "2019-08-01", 1.8, "predict"],
        ["Contract_402", 2178, 0, "2019-09-01", 1.8, "predict"],
        ["Contract_402", 2178, 0, "2019-10-01", 1.8, "predict"],
        ["Contract_402", 2178, 0, "2019-11-01", 1.6, "predict"],
        ["Contract_402", 2178, 0, "2019-12-01", 1.8, "predict"],
        ["Contract_402", 2178, 0, "2020-01-01", 1.6, "predict"],
        ["Contract_402", 2178, 0, "2020-02-01", 1.8, "predict"],
    ],
    columns=["Contract_ID", "Item_ID", "Build_Rate_Flag", "Date", "Order_Quantity", "type"],
)

TEST_ACCOUNT_1_EXPECTED_FORECAST_DATA = pd.DataFrame(
    [
        [1005, "Contract_402", "202002", "201906", 8, 11.1, 0.0, 0.0, 1.0],
        [1005, "Contract_402", "202002", "201907", 7, 3.8, 3.8, 8.0, 0.475],
        [2178, "Contract_402", "202002", "201906", 8, 1.3, 1.3, 0.0, 0.0],
        [2178, "Contract_402", "202002", "201907", 7, 1.4, 1.4, 4.0, 0.350],
        [2178, "Contract_402", "202002", "201908", 6, 1.8, 1.8, 0.0, 0.0],
        [2178, "Contract_402", "202002", "201909", 5, 1.8, 1.8, 8.0, 0.225],
        [2178, "Contract_402", "202002", "201910", 4, 1.8, 1.8, 0.0, 0.0],
        [2178, "Contract_402", "202002", "201911", 3, 1.6, 1.6, 4.0, 0.400],
        [2178, "Contract_402", "202002", "201912", 2, 1.8, 1.8, 0.0, 0.0],
        [2178, "Contract_402", "202002", "202001", 1, 1.6, 1.6, np.nan, np.nan],
        [2178, "Contract_402", "202002", "202002", 0, 1.8, 1.8, np.nan, np.nan],
    ],
    columns=[
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
).astype({"Item_ID": "category", "Contract_ID": "category", "Prediction_Months_Delta": np.int32})

TEST_ACCOUNT_1_EXPECTED_DSX_OUTPUT_DATA = pd.DataFrame(
    [
        [
            0,
            "Global|ContractID_Master_Part|Contract_402|mn_28400",
            np.nan,
            np.nan,
            np.nan,
            np.nan,
            "Forecast",
            "Additional Forecast 1",
            "2019-06-01 00:00:00.000000",
            0,
            0,
            pd.to_datetime(TEST_FREEZE_TIME, format="%Y-%m-%d"),
        ],
        [
            1,
            "Global|ContractID_Master_Part|Contract_402|mn_28400",
            np.nan,
            np.nan,
            np.nan,
            np.nan,
            "Forecast",
            "Additional Forecast 1",
            "2019-07-01 00:00:00.000000",
            4,
            0,
            pd.to_datetime(TEST_FREEZE_TIME, format="%Y-%m-%d"),
        ],
        [
            2,
            "Global|ContractID_Master_Part|Contract_402|mn_98712",
            np.nan,
            np.nan,
            np.nan,
            np.nan,
            "Forecast",
            "Additional Forecast 1",
            "2019-06-01 00:00:00.000000",
            1,
            0,
            pd.to_datetime(TEST_FREEZE_TIME, format="%Y-%m-%d"),
        ],
        [
            3,
            "Global|ContractID_Master_Part|Contract_402|mn_98712",
            np.nan,
            np.nan,
            np.nan,
            np.nan,
            "Forecast",
            "Additional Forecast 1",
            "2019-07-01 00:00:00.000000",
            1,
            0,
            pd.to_datetime(TEST_FREEZE_TIME, format="%Y-%m-%d"),
        ],
        [
            4,
            "Global|ContractID_Master_Part|Contract_402|mn_98712",
            np.nan,
            np.nan,
            np.nan,
            np.nan,
            "Forecast",
            "Additional Forecast 1",
            "2019-08-01 00:00:00.000000",
            2,
            0,
            pd.to_datetime(TEST_FREEZE_TIME, format="%Y-%m-%d"),
        ],
        [
            5,
            "Global|ContractID_Master_Part|Contract_402|mn_98712",
            np.nan,
            np.nan,
            np.nan,
            np.nan,
            "Forecast",
            "Additional Forecast 1",
            "2019-09-01 00:00:00.000000",
            2,
            0,
            pd.to_datetime(TEST_FREEZE_TIME, format="%Y-%m-%d"),
        ],
        [
            6,
            "Global|ContractID_Master_Part|Contract_402|mn_98712",
            np.nan,
            np.nan,
            np.nan,
            np.nan,
            "Forecast",
            "Additional Forecast 1",
            "2019-10-01 00:00:00.000000",
            2,
            0,
            pd.to_datetime(TEST_FREEZE_TIME, format="%Y-%m-%d"),
        ],
        [
            7,
            "Global|ContractID_Master_Part|Contract_402|mn_98712",
            np.nan,
            np.nan,
            np.nan,
            np.nan,
            "Forecast",
            "Additional Forecast 1",
            "2019-11-01 00:00:00.000000",
            2,
            0,
            pd.to_datetime(TEST_FREEZE_TIME, format="%Y-%m-%d"),
        ],
        [
            8,
            "Global|ContractID_Master_Part|Contract_402|mn_98712",
            np.nan,
            np.nan,
            np.nan,
            np.nan,
            "Forecast",
            "Additional Forecast 1",
            "2019-12-01 00:00:00.000000",
            2,
            0,
            pd.to_datetime(TEST_FREEZE_TIME, format="%Y-%m-%d"),
        ],
        [
            9,
            "Global|ContractID_Master_Part|Contract_402|mn_98712",
            np.nan,
            np.nan,
            np.nan,
            np.nan,
            "Forecast",
            "Additional Forecast 1",
            "2020-01-01 00:00:00.000000",
            2,
            0,
            pd.to_datetime(TEST_FREEZE_TIME, format="%Y-%m-%d"),
        ],
        [
            10,
            "Global|ContractID_Master_Part|Contract_402|mn_98712",
            np.nan,
            np.nan,
            np.nan,
            np.nan,
            "Forecast",
            "Additional Forecast 1",
            "2020-02-01 00:00:00.000000",
            2,
            0,
            pd.to_datetime(TEST_FREEZE_TIME, format="%Y-%m-%d"),
        ],
    ],
    columns=[
        "STG_Import_Periodic_ML_RowID",
        "Item Name",
        "Ship To",
        "projectid",
        "binid",
        "branch",
        "PeriodicDataElementType",
        "PeriodicDataElement",
        "PeriodDate",
        "Value",
        "NewItemFlag",
        "CreatedDateTime",
    ],
)


TEST_DATA = {
    "Item_ID": [3085, 3085, 3086],
    "Contract_ID": ["Contract_402", "Contract_402", "Contract_402"],
    "Prediction_Start_Month": [202002, 202002, 202002],
    "Predicted_Month": [202001, 202002, 201906],
    "Prediction_Months_Delta": [1, 0, 8],
    "Prediction_Raw": [27.588884418263355, 30.75161712752957, 2.9589109923171266],
    "Prediction_Post": [27.588884418263355, 30.75161712752957, 0.0],
    "Actual": [95.0, np.nan, np.nan],
    "Accuracy": [0.2904093096659299, np.nan, np.nan],
}
EXPECTED_DATA = (
    "Item_ID,Contract_ID,Prediction_End_Month,Predicted_Month,Prediction_Months_Delta,Prediction_Raw,Prediction_Post,Actual,Accuracy\n"  # noqa: E501
    "3085,Contract_402,202002,202001,1,27.588884418263355,27.588884418263355,95,0.2904093096659299\n"
    "3085,Contract_402,202002,202002,0,30.75161712752957,30.75161712752957,,\n"
    "3086,Contract_402,202002,201906,8,2.9589109923171266,0.0,,\n"
)

RUNTIME_CONFIG = RuntimeConfig(EngineRunType.backward, prediction_month=pd.Timestamp(year=2020, month=2, day=1))


@pytest.fixture()  # type: ignore
def data_output() -> DataOutput:
    internal_database = Database(DatabaseType.internal)
    dsx_write_database = Database(DatabaseType.dsx_write)
    internal_database.is_disabled = lambda: True  # type: ignore
    return DataOutput(RUNTIME_CONFIG, internal_database, dsx_write_database)


@pytest.fixture()  # type: ignore
def data_loader() -> DataLoader:
    internal_database = Database(DatabaseType.internal)
    dsx_read_database = Database(DatabaseType.dsx_read)
    internal_database.is_disabled = lambda: True  # type: ignore
    return DataLoader(internal_database, dsx_read_database)


def test_store_result_as_csv(data_output: DataOutput, tmp_path: Path) -> None:
    input_path = tmp_path / "test_csv_file.csv"
    df = pd.DataFrame(TEST_DATA)

    result_path = data_output.store_result(input_path, df)

    assert input_path == result_path
    assert EXPECTED_DATA == result_path.read_text()


def test_store_result_as_csv_overwrite_existing(data_output: DataOutput, tmp_path: Path) -> None:
    input_path = tmp_path / "test_csv_file.csv"
    input_path.write_text("old_text")
    df = pd.DataFrame(TEST_DATA)

    result_path = data_output.store_result(input_path, df)

    assert input_path == result_path
    assert EXPECTED_DATA == result_path.read_text()


def test_store_result_as_csv_and_ensure_correct_extension(data_output: DataOutput, tmp_path: Path) -> None:
    input_path = tmp_path / "test_csv_file.xlsx"
    df = pd.DataFrame(TEST_DATA)

    result_path = data_output.store_result(input_path, df)

    assert tmp_path / "test_csv_file.csv" == result_path
    assert EXPECTED_DATA == result_path.read_text()


def test_store_result_as_xlsx_and_ensure_correct_extension(data_output: DataOutput, tmp_path: Path) -> None:
    input_path = tmp_path / "test_csv_file.csv"
    df = pd.DataFrame(TEST_DATA)
    runtime_config_excel = RuntimeConfig(EngineRunType.development, output_format=OutputFormat.xlsx)
    data_output._runtime_config = runtime_config_excel

    result_path = data_output.store_result(input_path, df)

    excel_table = pd.read_excel(result_path)

    assert tmp_path / "test_csv_file.xlsx" == result_path
    assert_frame_equal(pd.DataFrame(TEST_DATA), excel_table)


def test_store_result_create_parent_directories(data_output: DataOutput, tmp_path: Path) -> None:
    input_path = tmp_path / "intermediate_directory" / "test_csv_file.csv"
    df = pd.DataFrame(TEST_DATA)

    result_path = data_output.store_result(input_path, df)

    assert input_path.parent / "test_csv_file.csv" == result_path
    assert EXPECTED_DATA == result_path.read_text()


def test_store_result_raises_if_path_is_directory(data_output: DataOutput, tmp_path: Path) -> None:
    input_path = tmp_path / "test_directory"
    input_path.mkdir()
    df = pd.DataFrame(TEST_DATA)

    with pytest.raises(AttributeError, match="Path for saving is an existing directory!"):
        data_output.store_result(input_path, df)


@pytest.mark.parametrize(
    "params,expected",
    [
        ({"some": "hyper", "params": 1.2, "int": -1}, '{\n    "int": -1,\n    "params": 1.2,\n    "some": "hyper"\n}'),
        ({}, "{}"),
    ],
)  # type: ignore
def test_store_hyper_parameters(
    data_output: DataOutput, tmp_path: Path, params: Dict[str, object], expected: str
) -> None:
    path = tmp_path / "test_directory"
    saved_path = data_output.store_optimized_hyperparameters(path, params)

    assert saved_path.read_text() == expected
    assert saved_path.name == "optimized_hyperparameters.json"


def test_convert_forecast_data_account_1(data_output: DataOutput, data_loader: DataLoader) -> None:
    model_config_account_1 = ModelConfigAccount1(runtime_config=RUNTIME_CONFIG, data_loader=data_loader,)
    account_data = data_loader.load_account_data(model_config_account_1, -1)

    actual = data_output._convert_forecast_data(
        model_config=model_config_account_1,
        account_data=account_data,
        forecast_raw=TEST_ACCOUNT_1_RAW_DATA,
        forecast_post=TEST_ACCOUNT_1_POST_DATA,
        actuals_newest_month=datetime(2019, 12, 1, 0, 0),
    )

    expected_size_after_aggregation_by_primary_key = 11
    assert (
        len(actual) == expected_size_after_aggregation_by_primary_key
    ), "Not the expected size of dataframe after aggregation by primary key"

    assert_frame_equal(actual.reset_index(drop=True), TEST_ACCOUNT_1_EXPECTED_FORECAST_DATA.reset_index(drop=True))


@pytest.mark.freeze_time(TEST_FREEZE_TIME)  # type: ignore
def test_convert_dsx_data_account_1(data_output: DataOutput, data_loader: DataLoader) -> None:
    model_config_account_1 = ModelConfigAccount1(runtime_config=RUNTIME_CONFIG, data_loader=data_loader)
    account_data = data_loader.load_account_data(model_config_account_1, -1)

    actual = data_output._convert_dsx_output_data(
        model_config=model_config_account_1, account_data=account_data, forecast_post=TEST_ACCOUNT_1_POST_DATA,
    )

    assert len(actual) == len(
        TEST_ACCOUNT_1_EXPECTED_DSX_OUTPUT_DATA
    ), "Not the expected size of dataframe after aggregation by model grouping and wesco master number"

    assert_frame_equal(actual, TEST_ACCOUNT_1_EXPECTED_DSX_OUTPUT_DATA)


def test_store_forecast_with_disabled_database(data_output: DataOutput, caplog: LogCaptureFixture) -> None:
    data_output._internal_database._is_disabled = True

    dummy_forecast = pd.DataFrame()
    model_run = ForecastModelRun()

    with caplog.at_level(logging.DEBUG):
        returned_model_run = data_output._store_forecast_in_internal_database(
            forecast=dummy_forecast, model_run=model_run
        )
    assert returned_model_run is model_run
    assert "Skip storing forecast in forecast_data table because of disabled internal database" in caplog.messages


def test_not_store_backward_in_database(
    data_loader: DataLoader, caplog: LogCaptureFixture, monkeypatch: MonkeyPatch, tmp_path: Path,
) -> None:
    internal_database = Database(DatabaseType.internal)
    dsx_write_database = Database(DatabaseType.dsx_write)
    data_output = DataOutput(RUNTIME_CONFIG, internal_database, dsx_write_database)

    model_config_account_1 = ModelConfigAccount1(runtime_config=RUNTIME_CONFIG, data_loader=data_loader)
    account_data = data_loader.load_account_data(model_config_account_1, -1)
    model_run = ForecastModelRun()
    model_config_account_1.forecast_path = tmp_path
    with caplog.at_level(logging.DEBUG):
        returned_model_run = data_output.store_forecast(
            model_config=model_config_account_1,
            model_run=model_run,
            account_data=account_data,
            forecast_raw=TEST_ACCOUNT_1_RAW_DATA,
            forecast_post=TEST_ACCOUNT_1_POST_DATA,
            actuals_newest_month=datetime(2019, 10, 1, 0, 0),
        )
    assert RUNTIME_CONFIG.engine_run_type == EngineRunType.backward
    assert returned_model_run is model_run
    assert "Skip storing forecast in internal database for backward run." in caplog.messages


def test_store_cleaned_data_in_database() -> None:
    first_run_id = -100
    second_run_id = first_run_id + 1

    query_relevant_cleaned_data = Query(CleanedData).filter(
        CleanedData.c.run_id.in_([first_run_id, second_run_id])
    )  # type: ignore

    delete_test_data(query_relevant_cleaned_data)  # Cleanup data from previously failed/cancelled test run

    cleaned_data = pd.DataFrame(
        [
            {
                "Project_ID": "Test_Project",
                "Contract_ID": "Contract_store_cleaned_data",
                "Wesco_Master_Number": "Test_Master_Number",
                "Date": datetime(2019, 12, 1, 0, 0),
                "Date_YYYYMM": 201912,
                "Item_ID": -1,
                "Unit_Cost": 2.0,
                "Order_Quantity": 10.0,
                "Order_Cost": 20.0,
            },
            {
                "Project_ID": "Test_Project",
                "Contract_ID": "Contract_store_cleaned_data",
                "Wesco_Master_Number": "Test_Master_Number",
                "Date": datetime(2020, 1, 1, 0, 0),
                "Date_YYYYMM": 202001,
                "Item_ID": -1,
                "Unit_Cost": 0.0,
                "Order_Quantity": 0.0,
                "Order_Cost": 0.0,
            },
        ]
    )
    first_expected_cleaned_data = cleaned_data.assign(run_id=first_run_id).to_dict(orient="records")
    second_expected_cleaned_data = cleaned_data.assign(run_id=second_run_id).to_dict(orient="records")

    internal_database = Database(DatabaseType.internal)

    def _assert_expected_cleaned_data(expected_cleaned_data: List[Dict[str, Any]], message: str) -> None:
        with internal_database.transaction_context() as session:
            result = [row._asdict() for row in query_relevant_cleaned_data.with_session(session).all()]
            assert expected_cleaned_data == result, message

    data_output = DataOutput(RUNTIME_CONFIG, internal_database, Database(DatabaseType.dsx_write))
    _assert_expected_cleaned_data([], "Expect empty cleaned data at beginning of test")

    data_output.store_cleaned_data(cleaned_data, first_run_id)
    _assert_expected_cleaned_data(first_expected_cleaned_data, "Expect only first cleaned data run")

    data_output.store_cleaned_data(cleaned_data, second_run_id)  # This should cleanup the first_expected_cleaned_data
    _assert_expected_cleaned_data(second_expected_cleaned_data, "Expect only second cleaned data run")

    data_output.store_cleaned_data(cleaned_data, first_run_id)  # This should not cleanup newer second run data
    _assert_expected_cleaned_data(first_expected_cleaned_data + second_expected_cleaned_data, "Don't delete newer data")

    delete_test_data(query_relevant_cleaned_data)  # Cleanup data from test run
    _assert_expected_cleaned_data([], "Expect empty cleaned data at end of test")


def test_store_exogenous_features() -> None:
    first_run_id = -90
    second_run_id = first_run_id + 1

    query_relevant_data = Query(ExogenousFeature).filter(
        ExogenousFeature.c.run_id.in_([first_run_id, second_run_id])
    )  # type: ignore

    delete_test_data(query_relevant_data)  # Cleanup data from previously failed/cancelled test run

    exogenous_features = pd.DataFrame(
        [
            {
                "Periodic_Data_Stream": "Test_Data",
                "Airframe": "Test_Airframe",
                "Contract_ID": "Contract_exogenous_feature",
                "Project_ID": "Test_Project",
                "Date": datetime(2019, 12, 1, 0, 0),
                "Value": 20.0,
            },
            {
                "Periodic_Data_Stream": "Test_Data",
                "Airframe": "Test_Airframe",
                "Contract_ID": "Contract_exogenous_feature",
                "Project_ID": "Test_Project",
                "Date": datetime(2020, 1, 1, 0, 0),
                "Value": 0.1,
            },
        ]
    )
    first_expected_data = exogenous_features.assign(run_id=first_run_id).to_dict(orient="records")
    second_expected_data = exogenous_features.assign(run_id=second_run_id).to_dict(orient="records")

    internal_database = Database(DatabaseType.internal)

    def _assert_expected_exogenous_features(expected_exogenous_feature: List[Dict[str, Any]], message: str) -> None:
        with internal_database.transaction_context() as session:
            result = [row._asdict() for row in query_relevant_data.with_session(session).all()]
            assert expected_exogenous_feature == sorted(result, key=itemgetter("run_id")), message

    data_output = DataOutput(RUNTIME_CONFIG, internal_database, Database(DatabaseType.dsx_write))
    _assert_expected_exogenous_features([], "Expect empty exogenous feature data at beginning of test")

    data_output.store_exogenous_features(exogenous_features, first_run_id)
    _assert_expected_exogenous_features(first_expected_data, "Expect only first exogenous feature data run")

    data_output.store_exogenous_features(exogenous_features, second_run_id)
    # This should cleanup the previously inserted exogenous feature data
    _assert_expected_exogenous_features(second_expected_data, "Expect only second exogenous feature data run")

    data_output.store_exogenous_features(exogenous_features, first_run_id)
    # This should not cleanup newer second run data
    _assert_expected_exogenous_features(first_expected_data + second_expected_data, "Don't delete newer data")

    delete_test_data(query_relevant_data)  # Cleanup data from test run
    _assert_expected_exogenous_features([], "Expect empty exogenous feature data at end of test")
