import gzip
import logging
from datetime import datetime
from pathlib import Path
from typing import (
    Any,
    Iterator,
    List,
)
from unittest.mock import Mock

import numpy as np
import pandas as pd
import pytest
from _pytest.logging import LogCaptureFixture
from _pytest.monkeypatch import MonkeyPatch
from forecasting_platform import master_config
from forecasting_platform.internal_schema import (
    CleanedData,
    ExogenousFeature,
)
from forecasting_platform.model_config_scripts import BaseModelConfig
from forecasting_platform.static import (
    CLEANED_DATA_TABLE,
    EXOGENOUS_FEATURE_TABLE,
    DatabaseType,
    DataException,
    EngineRunType,
    ExogenousFeatures,
    InternalFeatures,
)
from forecasting_platform.test_utils import delete_test_data
from owforecasting import TimeSeries
from owforecasting.features import default_features
from pandas.testing import assert_frame_equal
from sqlalchemy.orm import Query

from .data_loader import (
    DSX_EXOGENOUS_DATA_COLUMN_MAPPING,
    DataLoader,
)
from .database import Database
from .runtime_config import RuntimeConfig

TEST_ACCOUNT = __name__.split(".")[-1]
TEST_DATA = {
    "Project_ID": ["Project_1", "Project_2"],
    "Contract_ID": ["Contract_1", "Contract_2"],
    "Wesco_Master_Number": ["Wesco_Master_Number_1", "Wesco_Master_Number_2"],
    "Date": ["2013-01-01", "2019-10-01"],
    "Date_YYYYMM": [201301, 201301],
    "Item_ID": ["1", "2"],
    "Unit_Cost": ["1.0", "2.0"],
    "Order_Quantity": ["0.0", "1.0"],
    "Order_Cost": ["0.0", "2.0"],
}

EXPECTED_DATA = {
    "Project_ID": ["Project_1", "Project_2"],
    "Contract_ID": ["Contract_1", "Contract_2"],
    "Wesco_Master_Number": ["Wesco_Master_Number_1", "Wesco_Master_Number_2"],
    "Date": [np.datetime64("2013-01-01"), np.datetime64("2019-10-01")],
    "Date_YYYYMM": [201301, 201301],
    "Item_ID": [1, 2],
    "Unit_Cost": [1.0, 2.0],
    "Order_Quantity": [0.0, 1.0],
    "Order_Cost": [0.0, 2.0],
}


DUMMY_CLEANED_DATA_RUN_ID = 13337  # High integer to not interfere with other cleaning tests during integration testing
DUMMY_EXOGENOUS_FEATURE_RUN_ID = -123


def generate_exogenous_feature_data(periodic_data_stream: str) -> pd.DataFrame:
    return pd.DataFrame(
        [
            [periodic_data_stream, "Airframe_1", "Contract_1", "Project_1", datetime(2015, 1, 1), 25.60],
            [periodic_data_stream, "Airframe_1", "Contract_1", "Project_1", datetime(2016, 1, 1), 25.60],
        ],
        columns=DSX_EXOGENOUS_DATA_COLUMN_MAPPING.values(),
    )


class ModelConfigAccountTestDummy(BaseModelConfig):
    MODEL_NAME = "Account_Data_Loader_Dummy"
    CONTRACTS = ["Contract_1", "Contract_2"]
    DEFAULT_FEATURES = default_features()
    POSTPROCESS_DEPTH = 1
    TRAINING_START = pd.Timestamp(2001, 1, 1)
    WEIGHTING = 1

    def preprocess_account_data(
        self, sales_raw: pd.DataFrame, grouping: List[str], internal_features: InternalFeatures
    ) -> pd.DataFrame:
        return pd.DataFrame()

    def prepare_training_data(
        self, sales: pd.DataFrame, grouping: List[str], exo_features: ExogenousFeatures,
    ) -> TimeSeries:
        dummy_time_series = TimeSeries(
            pd.DataFrame(data={"date": [datetime.utcnow()], "response": [1]}), "date", "response"
        )
        return dummy_time_series

    def postprocess_forecast(
        self, ts: TimeSeries, ts_pred: TimeSeries, sales: pd.DataFrame, grouping: List[str]
    ) -> pd.DataFrame:
        return pd.DataFrame()


@pytest.fixture()  # type: ignore
def prepare_database() -> Iterator[None]:
    expected_dataframe = pd.DataFrame(EXPECTED_DATA)
    expected_dataframe["run_id"] = DUMMY_CLEANED_DATA_RUN_ID

    cleanup_query = Query(CleanedData).filter(CleanedData.c.run_id == DUMMY_CLEANED_DATA_RUN_ID)  # type: ignore
    delete_test_data(cleanup_query)  # Cleanup in case of previously failed test

    internal_database = Database(DatabaseType.internal)
    with internal_database.transaction_context() as session:
        cleaned_data_count = (
            session.query(CleanedData).filter(CleanedData.c.run_id == DUMMY_CLEANED_DATA_RUN_ID).count()  # type: ignore
        )
    assert cleaned_data_count == 0

    with internal_database.transaction_context() as session:
        expected_dataframe.to_sql(
            CLEANED_DATA_TABLE,
            session.connection(),
            schema=internal_database._database_schema,
            if_exists="append",
            index=False,
        )

    yield

    assert delete_test_data(cleanup_query) == len(expected_dataframe)


@pytest.fixture()  # type: ignore
def model_config() -> BaseModelConfig:
    runtime_config = RuntimeConfig(EngineRunType.backward)
    internal_database = Database(DatabaseType.internal)
    dsx_read_database = Database(DatabaseType.dsx_read)
    data_loader = DataLoader(internal_database, dsx_read_database)

    return ModelConfigAccountTestDummy(runtime_config, data_loader)


@pytest.mark.parametrize(
    "account_processed_data_path", [master_config.account_processed_data_path, "ensure other path can be configured",]
)  # type: ignore
def test_load_account_data_if_internal_database_disabled(
    tmp_path: Path, monkeypatch: MonkeyPatch, account_processed_data_path: str, caplog: LogCaptureFixture
) -> None:
    caplog.set_level(logging.INFO)

    runtime_config = RuntimeConfig(EngineRunType.backward)
    internal_database = Database(DatabaseType.internal)
    dsx_read_database = Database(DatabaseType.dsx_read)
    internal_database.is_disabled = lambda: True  # type: ignore
    data_loader = DataLoader(internal_database, dsx_read_database)

    monkeypatch.setattr(master_config, "account_processed_data_path", account_processed_data_path)
    monkeypatch.setattr(master_config, "default_data_loader_location", tmp_path)

    model_config = ModelConfigAccountTestDummy(runtime_config, data_loader)

    account_data_paths = []
    for contract in model_config.CONTRACTS:
        account_data_path = Path(account_processed_data_path) / f"DSX_{contract}_Data.csv.gz"
        (tmp_path / account_processed_data_path).mkdir(exist_ok=True, parents=True)
        with gzip.open(tmp_path / account_data_path, "wt") as f:
            df = pd.DataFrame(TEST_DATA)
            f.write(df.loc[df["Contract_ID"] == contract].to_csv(None, index=False))
        account_data_paths.append(account_data_path)

    account_data = data_loader.load_account_data(model_config, -1)

    expected_data = pd.DataFrame(EXPECTED_DATA)
    expected_data["Item_ID"] = expected_data["Item_ID"].astype(np.int32)
    assert_frame_equal(expected_data, account_data)
    assert (
        "Internal database connection disabled in master_config. Loading account data from csv file." in caplog.messages
    )
    assert f'Loaded account data with 1 lines from "{account_data_paths[0]}"' in caplog.messages
    assert f'Loaded account data with 1 lines from "{account_data_paths[1]}"' in caplog.messages


def test_load_account_data(
    prepare_database: Iterator[None], model_config: BaseModelConfig, caplog: LogCaptureFixture
) -> None:
    caplog.set_level(logging.INFO)
    account_data = model_config._data_loader.load_account_data(model_config, DUMMY_CLEANED_DATA_RUN_ID)

    expected_df = pd.DataFrame(EXPECTED_DATA)
    assert_frame_equal(expected_df, account_data)
    assert "Loaded account data with 2 rows from internal database" in caplog.messages


def test_load_account_data_raises_if_data_is_not_in_database(model_config: BaseModelConfig) -> None:
    model_config.CONTRACTS = ["This_contract_is_not_in_cleaned_data_table"]  # type: ignore

    with pytest.raises(DataException, match="Account data for Account_Data_Loader_Dummy is empty"):
        model_config._data_loader.load_account_data(model_config, DUMMY_CLEANED_DATA_RUN_ID)


@pytest.mark.parametrize("feature_name", ["Build Rate", "Build Rate 202001"])  # type: ignore
def test_load_exogenous_feature_with_database_disabled(
    feature_name: str, tmp_path: Path, monkeypatch: MonkeyPatch, caplog: LogCaptureFixture
) -> None:
    caplog.set_level(logging.INFO)

    test_data = pd.DataFrame(
        [
            ["EF", "Project_1", 0, 0, "Contract_1", "EF", "Airframe_1", None, "Build Rate", "2015-01-01", 25.60],
            ["EF", "Project_1", 0, 0, "Contract_1", "EF", "Airframe_1", None, "Build Rate 202001", "2015-01-01", 25.60],
            ["EF", "Project_1", 0, 0, "Contract_1", "EF", "Airframe_1", None, "Build Rate", "2016-01-01", 25.60],
            ["EF", "Project_1", 0, 0, "Contract_1", "EF", "Airframe_1", None, "Build Rate 202001", "2016-01-01", 25.60],
        ],
        columns=[
            "binid",
            "projectid",
            "ship to",
            "branch",
            "contractid",
            "custmiscinfo",
            "airframe",
            "program",
            "periodicdatastream",
            "perioddate",
            "value",
        ],
    )

    data_loader = DataLoader(
        internal_database=Mock(spec=Database, is_disabled=lambda: True), dsx_read_database=Mock(spec=Database)
    )

    dsx_exogenous_data_path = "01 Raw data/Test_DSX_exogenous_features.csv"
    absolute_dsx_exogenous_data_path = tmp_path / dsx_exogenous_data_path
    absolute_dsx_exogenous_data_path.parent.mkdir(parents=True)
    pd.DataFrame(test_data).to_csv(absolute_dsx_exogenous_data_path, index=False)

    monkeypatch.setattr(master_config, "dsx_exogenous_data_path", dsx_exogenous_data_path)
    monkeypatch.setattr(master_config, "default_data_loader_location", tmp_path)

    exogenous_data = data_loader.load_exogenous_feature(feature_name=feature_name, run_id=-1)

    assert_frame_equal(generate_exogenous_feature_data(feature_name), exogenous_data)

    assert (
        f'Loaded exogenous feature for Periodic_Data_Stream "{feature_name}" '
        f'with 2 lines from "{dsx_exogenous_data_path}"' in caplog.messages
    )


def test_load_exogenous_data_from_internal_database_empty(caplog: LogCaptureFixture) -> None:
    caplog.set_level(logging.INFO)

    data_loader = DataLoader(
        internal_database=Database(DatabaseType.internal), dsx_read_database=Database(DatabaseType.dsx_read)
    )

    with pytest.raises(DataException, match='Exogenous feature data is empty for Periodic_Data_Stream "Not found"'):
        data_loader.load_exogenous_feature("Not found", DUMMY_EXOGENOUS_FEATURE_RUN_ID)

    assert "Loaded exogenous data with 0 rows from internal database" in caplog.messages


@pytest.fixture()  # type: ignore
def with_exogenous_data_in_internal_database() -> Iterator[None]:
    cleanup_data_query = Query(ExogenousFeature).filter(
        ExogenousFeature.c.run_id == DUMMY_EXOGENOUS_FEATURE_RUN_ID
    )  # type: ignore
    delete_test_data(cleanup_data_query)  # Cleanup in case of previously failed test

    for periodic_data_stream in ["Stream-DB-Test1", "Stream-DB-Test2", "Stream-DB-Test3"]:
        test_data = generate_exogenous_feature_data(periodic_data_stream)
        Database(DatabaseType.internal).insert_data_frame(
            test_data.assign(run_id=DUMMY_EXOGENOUS_FEATURE_RUN_ID), EXOGENOUS_FEATURE_TABLE
        )

    yield

    assert delete_test_data(cleanup_data_query) == 6


def test_load_exogenous_data_from_internal_database(
    caplog: LogCaptureFixture, with_exogenous_data_in_internal_database: None
) -> None:
    caplog.set_level(logging.INFO)

    data_loader = DataLoader(
        internal_database=Database(DatabaseType.internal), dsx_read_database=Database(DatabaseType.dsx_read)
    )

    result = data_loader.load_exogenous_feature("Stream-DB-Test2", DUMMY_EXOGENOUS_FEATURE_RUN_ID)

    pd.testing.assert_frame_equal(result, generate_exogenous_feature_data("Stream-DB-Test2"))
    assert "Loaded exogenous data with 2 rows from internal database" in caplog.messages


def test_load_cleaning_input_data(caplog: LogCaptureFixture, monkeypatch: MonkeyPatch) -> None:
    caplog.set_level(logging.WARNING)

    def mock_load_csv(*args: Any, **kwargs: Any) -> pd.DataFrame:
        return pd.DataFrame(
            [
                # this one will be kept after cleaning
                ["Account_18", "Project_0", "Contract_0", 1, pd.Timestamp("2020-02-01"), 1.0, 1.0, "mn0"],
                # this one will be removed due to adhoc contract in dsx data workaround in TODO FSC-371
                ["Account_18", "Project_0", "Contract_730", 1, pd.Timestamp("2020-02-01"), 1.0, 1.0, "mn0"],
                # this one will be removed due to wrong days in dsx data workaround in TODO FSC-318
                ["Account_18", "Project_0", "Contract_0", 1, pd.Timestamp("2020-02-28"), 1.0, 1.0, "mn0"],
            ],
            columns=[
                "lowlevelcust",
                "projectid",
                "contractid",
                "shortid",
                "perioddate",
                "Cost",
                "Adjusted History",
                "masterpart",
            ],
        )

    monkeypatch.setattr(DataLoader, "load_csv", mock_load_csv)
    internal_database = Database(DatabaseType.internal)
    dsx_read_database = Database(DatabaseType.dsx_read)
    dsx_read_database.is_disabled = lambda: True  # type: ignore
    data_loader = DataLoader(internal_database, dsx_read_database)

    assert data_loader.load_cleaning_input_data().equals(
        pd.DataFrame(
            [
                # this one will be kept after cleaning
                ["Account_18", "Project_0", "Contract_0", 1, pd.Timestamp("2020-02-01"), 1.0, 1.0, "mn0"]
            ],
            columns=[
                "lowlevelcust",
                "projectid",
                "contractid",
                "shortid",
                "perioddate",
                "Cost",
                "Adjusted History",
                "masterpart",
            ],
        )
    )
    assert "Removed 1 rows with invalid dates." in caplog.messages
    assert "Removed 1 unexpected rows of adhoc contract." in caplog.messages
