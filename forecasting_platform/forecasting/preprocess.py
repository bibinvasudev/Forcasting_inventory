from __future__ import annotations

from datetime import datetime
from logging import getLogger
from typing import (
    TYPE_CHECKING,
    List,
    Optional,
    cast,
)

import numpy as np
import pandas as pd
from forecasting_platform.internal_schema import (
    CleanedData,
    ForecastData,
    ForecastRun,
)
from forecasting_platform.static import (
    PREDICT_RESULT_TYPE,
    PREDICTION_MONTH_FORMAT,
    TRAIN_RESULT_TYPE,
    ConfigurationException,
    DataException,
    ForecastRunStatus,
)
from sqlalchemy import (
    distinct,
    func,
)

from .outliers import adjust_outliers_to_standard_deviation
from .reporting import compute_accuracy_as_sql

if TYPE_CHECKING:
    from forecasting_platform.services import Database

logger = getLogger("preprocess")

DSX_INPUT_COLUMN_MAPPING = {
    "lowlevelcust": "Low_Level_Customer",  # TODO FSC-371 Remove lowlevelcust again (should no longer be needed)
    "projectid": "Project_ID",
    "contractid": "Contract_ID",
    "shortid": "Item_ID",
    "perioddate": "Date",
    "Cost": "Unit_Cost",
    "Adjusted History": "Order_Quantity",
    "masterpart": "Wesco_Master_Number",
}

SALES_COLUMNS = [
    "Project_ID",
    "Contract_ID",
    "Item_ID",
    "Date",
    "Date_YYYYMM",
    "Unit_Cost",
    "Order_Cost",
    "Order_Quantity",
    "Wesco_Master_Number",
]


def validate_input_data(raw_dsx_input: pd.DataFrame) -> pd.DataFrame:  # pragma: no cover
    """Validate Raw DSX input data.

    Args:
        raw_dsx_input: Raw DSX input data.

    Returns:
        Raw, validated DSX input data.

    Raises:
        DataException:
            - If input data has negative entries for 'Cost'.
            - If non nullable columns have invalid NULL values.
            - If input data has entries larger than the maximum 32-bit integer value in 'Cost' or 'Adjusted History'.
            - If dates are not the first day of the month.

    """
    not_nullable_columns = list(DSX_INPUT_COLUMN_MAPPING)
    for col in not_nullable_columns:
        invalid_null_entries = raw_dsx_input.loc[raw_dsx_input[col].isnull()]
        if len(invalid_null_entries) > 0:
            raise DataException(
                f"Data from DSX have unallowed NULL entries in column '{col}' in following rows: "
                f"{invalid_null_entries.index.tolist()}"
            )

    negative_costs = raw_dsx_input.loc[(raw_dsx_input["Cost"] < 0)]
    if len(negative_costs) > 0:
        raise DataException(
            f"Data from DSX have {len(negative_costs)} entries with negative costs in following rows: "
            f"{negative_costs.index.tolist()}"
        )

    int32_max_value = 2 ** 31 - 1
    invalid_big_values = raw_dsx_input.loc[
        (raw_dsx_input["Cost"] > int32_max_value) | (raw_dsx_input["Adjusted History"] > int32_max_value)
    ]
    if len(invalid_big_values) > 0:
        raise DataException(
            f"Data from DSX have {len(invalid_big_values)} entries with values exceeding 32 bit integer maximum value "
            f"in following rows: {invalid_big_values.index.tolist()}"
        )

    invalid_dates_data = raw_dsx_input.loc[raw_dsx_input["perioddate"].dt.day != 1]
    if len(invalid_dates_data) > 0:
        raise DataException(
            f"Data from DSX have {len(invalid_dates_data)} entries with invalid dates in following rows: "
            f"{invalid_dates_data.index.tolist()}"
        )

    return raw_dsx_input


def clean_input_data(input_data: pd.DataFrame) -> pd.DataFrame:
    """Clean DSX input data, including type conversion, column name mapping and generic filtering for undefined entries.

    Args:
        input_data: Raw DSX input data.

    Returns:
        Cleaned sales data to be used for model forecasts.

    """
    logger.info(f"Starting cleaning of {len(input_data)} rows of raw input data")

    if set(DSX_INPUT_COLUMN_MAPPING.keys()) != set(input_data.columns):
        logger.warning(
            f"DSX input data columns mismatch: "
            f"expected {set(DSX_INPUT_COLUMN_MAPPING.keys())} but got {set(input_data.columns)}"
        )

    sales_raw = input_data.rename(columns=DSX_INPUT_COLUMN_MAPPING)

    # Add sales month as an integer to enable pure SQL based operations with other tables
    sales_raw["Date_YYYYMM"] = sales_raw["Date"].dt.year * 100 + sales_raw["Date"].dt.month

    # Set negative sales to zero
    negative_sales_len = len(sales_raw.loc[sales_raw["Order_Quantity"] < 0])
    if negative_sales_len > 0:
        logger.info(f"Set {negative_sales_len} negative sales to zero")
        sales_raw.loc[sales_raw["Order_Quantity"] < 0, "Order_Quantity"] = 0

    # Adding Order Costs
    sales_raw = compute_order_cost(sales_raw)

    sales = sales_raw[SALES_COLUMNS].copy()

    logger.info(f"Finished cleaning of {len(sales)} rows of sales data")
    return sales


def compute_order_cost(sales: pd.DataFrame) -> pd.DataFrame:
    """Return sales data with additional ``Order_Cost`` column.

    Args:
        sales: Sales data to process.

    Returns:
        :class:`~pandas.DataFrame` including "Order_Cost" column.

    """
    return sales.assign(Order_Cost=sales["Order_Quantity"] * sales["Unit_Cost"])


def RR_NR_Flag(sales: pd.DataFrame, ltm_start: pd.Timestamp, ltm_end: pd.Timestamp) -> pd.DataFrame:
    """Create Runner and Repeater flag.

    Args:
        sales: Sales data filtered to ADHOC parts.
        ltm_start: Start date of last 12 months.
        ltm_end: End date of last 12 months.

    Returns:
        :class:`~pandas.DataFrame` containing "Is_Runner" flag column.

    """
    mask = sales["Date"] >= ltm_start
    mask &= sales["Date"] <= ltm_end

    sales_ltm = sales[mask]
    sales_ltm = sales_ltm.groupby(["Item_ID", "Date"])["Order_Quantity"].sum().reset_index()

    # Criteria 1: RR have more than 4 month of sales in the ltm

    sales_ltm_Months = sales_ltm.copy()
    sales_ltm_Months = sales_ltm_Months.groupby(["Item_ID"])["Date"].nunique().reset_index()
    sales_ltm_Months = sales_ltm_Months.rename(columns={"Date": "N_hits"})

    # Store output
    out = sales_ltm_Months
    out["Is_Runner"] = np.where(
        (out["N_hits"] >= 4),
        # & (out["diff"] != out["N_hits"]),
        True,
        False,
    )
    return out[["Item_ID", "Is_Runner"]]


def preprocess_outliers(
    input_data: pd.DataFrame, grouping: List[str], fc_start: pd.Timestamp, depth: int = 4,
) -> pd.DataFrame:
    """Preprocess sales data to restrict outliers which exceed the standard deviation.

    Args:
        input_data: :class:`~pandas.DataFrame` containing the sales data.
        grouping: List containing all column names that define granularity of preprocessed forecast values.
        fc_start: Start Date of Forecast.
        depth: Integer defining the considered time period in months when performing preprocessing.

    Returns:
        Adjusted forecast values in same format as input_data (but only training data).

    """
    train_data = input_data[input_data["type"] == TRAIN_RESULT_TYPE].copy()
    test_data = input_data[(input_data["type"] == "test") | (input_data["type"] == PREDICT_RESULT_TYPE)]

    grouping_inp = ["inp_" + column for column in grouping]

    # Filter for time period
    train_data["Date"] = pd.to_datetime(train_data["inp_Date"], format="%Y-%m-%d")
    mask = train_data["Date"] >= fc_start - pd.DateOffset(months=depth)
    mask &= train_data["Date"] <= fc_start - pd.DateOffset(months=1)
    train_filtered = train_data.loc[mask]
    train_filtered = train_filtered.drop(columns=["Date"])
    train_filtered = train_filtered.reset_index(drop=True)

    # Group and aggregate order quantity to variance and mean
    train_filtered_grouped = (
        train_filtered.groupby(grouping_inp, observed=True)
        .agg(var=pd.NamedAgg("inp_Order_Quantity", np.var), value=pd.NamedAgg("inp_Order_Quantity", np.mean),)
        .reset_index()
    )

    # Left merge of order quantity that hasn't changed on predict_data
    train_preprocessed = train_data.merge(train_filtered_grouped, how="left", on=grouping_inp, validate="many_to_one")

    train_preprocessed = adjust_outliers_to_standard_deviation(train_preprocessed, "inp_Order_Quantity")

    # Drop value and var columns not necessary anymore
    train_preprocessed.drop(columns=["value", "var", "Date", "period", "type"], inplace=True)
    test_data = test_data.drop(columns=["type", "period"])
    out = train_preprocessed.append(test_data, ignore_index=True)

    out.columns = grouping + ["Order_Quantity", "Date"]

    return out


def filter_sales(
    sales: pd.DataFrame,
    exclude_projects: Optional[List[str]] = None,
    only_include_projects: Optional[List[str]] = None,
    exclude_items: Optional[List[int]] = None,
    only_include_items: Optional[List[int]] = None,
) -> pd.DataFrame:
    """Exclude or include only sales for relevant projects or items and return as a new :class:`~pandas.DataFrame`.

    Args:
        sales: Sales data to filter.
        exclude_projects: Projects that shall be excluded from the forecasting data.
        only_include_projects: Projects that shall be included in the forecasting data.
        exclude_items: Items that shall be excluded from the forecasting data.
        only_include_items: Items that shall be included in the forecasting data.

    Returns:
        :class:`~pandas.DataFrame` filtered based on the given criteria.

    """
    sales = sales.copy()

    assert not (exclude_projects and only_include_projects), "Cannot include and exclude projects of same account"
    assert not (exclude_items and only_include_items), "Cannot include and exclude items of same account"

    if exclude_projects:
        sales = sales.loc[~sales["Project_ID"].isin(exclude_projects)]

    if only_include_projects:
        sales = sales.loc[sales["Project_ID"].isin(only_include_projects)]

    if exclude_items:
        sales = sales.loc[~sales["Item_ID"].isin(exclude_items)]

    if only_include_items:
        sales = sales.loc[sales["Item_ID"].isin(only_include_items)]

    return sales


def group_sales(sales: pd.DataFrame, grouping: List[str], unit_cost_aggregation: str) -> pd.DataFrame:
    """Group sales data to provided granularity.

    Args:
        sales: Sales data to process.
        grouping: Grouping granularity to be used.
        unit_cost_aggregation: Aggregation operator to use for "Unit_Cost".

    Returns:
        :class:`~pandas.DataFrame` with grouped sales data.

    """
    return (
        sales.groupby(grouping + ["Date"])
        .aggregate({"Order_Cost": "sum", "Order_Quantity": "sum", "Unit_Cost": unit_cost_aggregation})
        .reset_index()
    )


def get_grouping_columns(grouping: List[str], internal_features: List[str]) -> List[str]:
    """Group sales data to provided granularity.

    Args:
        grouping: Grouping granularity to be used.
        internal_features: List of account specific internal features.

    Returns:
        Combined list of grouping columns.

    """
    return grouping + internal_features


def preprocess_grouped_sales(
    sales: pd.DataFrame,
    fc_start: pd.Timestamp,
    test_periods: int,
    train_start: pd.Timestamp,
    grouping: List[str],
    sales_min_period: int,
) -> pd.DataFrame:
    """Select time frames used in forecasting.

    - Apply train_start and fc_end time cut-off
    - Filter on forecastable data (last sales >5 months ago)

    Args:
        sales: Sales data to process.
        fc_start: Time to start the forecasting from.
        test_periods: Periods to predict in sample.
        train_start: Time to start training data at.
        grouping: Granularity used for forecasting.
        sales_min_period: Number of months to filter forecastable sales.

    Returns:
        Preprocessed :class:`~pandas.DataFrame`.

    """
    # Cut-off dates before ``train_start`` and after ``fc_start`` + `test_periods`
    cutoff = fc_start + pd.DateOffset(months=(test_periods - 1))
    fltr = (sales["Date"] >= train_start) & (sales["Date"] <= cutoff)
    sales = sales.loc[fltr].copy()

    # Check if enough data are available
    if sales.empty:
        raise ConfigurationException("No training data available for chosen forecast period.")
    sales_max_date = sales["Date"].max()
    assert sales_max_date <= cutoff
    if sales_max_date < cutoff:
        raise ConfigurationException("Not enough training data available for chosen forecast period.")

    # Filter sales to forecastable part
    fltr = sales["Date"] < (fc_start - pd.DateOffset(months=sales_min_period))
    fltr &= sales["Order_Quantity"] >= 0
    relevant = sales.loc[fltr, grouping].drop_duplicates().copy()
    sales = sales.merge(relevant, how="inner", on=grouping, validate="many_to_one")
    return sales


def update_forecast_data_with_cleaned_data_sales(
    internal_database: Database, cleaned_data_run_id: Optional[int], cleaned_data_newest_month: Optional[datetime]
) -> None:
    """Update actual values for all previous forecasts with newest cleaned data up to newest date in cleaned data.

    Actual values with NULL are set to zero if date is before or equal to the newest date in cleaning data.
    Any dates after newest date are left unchanged.

    Args:
        internal_database: Service to access internal database.
        cleaned_data_run_id: ID of latest successful platform run, which included cleaning.
        cleaned_data_newest_month: Newest month in cleaned data table associated with the ``cleaned_data_run_id``.
    """
    if internal_database.is_disabled():
        logger.warning("Skipping update of previous forecasts due to disabled database")
        return

    assert cleaned_data_run_id is not None, "Invalid program state: Expected cleaned_data_run_id to exist"
    with internal_database.transaction_context() as session:
        updated_existing = session.execute(
            ForecastData.update()
            .where(CleanedData.c.run_id == cleaned_data_run_id)
            .where(ForecastData.c.Contract_ID == CleanedData.c.Contract_ID)
            .where(ForecastData.c.Item_ID == CleanedData.c.Item_ID)
            .where(ForecastData.c.Predicted_Month == CleanedData.c.Date_YYYYMM)
            .values(
                {
                    "Actual": CleanedData.c.Order_Quantity,
                    "Accuracy": compute_accuracy_as_sql(CleanedData.c.Order_Quantity, ForecastData.c.Prediction_Post),
                }
            )
        )

    logger.info(
        f"Updated {updated_existing.rowcount} rows of forecast_data with old actual values to"
        f" newest actual values from cleaned_data"
    )

    assert cleaned_data_newest_month, "Invalid program state: Expected cleaned_data_newest_month to exist"
    newest_month = int(cleaned_data_newest_month.strftime(PREDICTION_MONTH_FORMAT))
    with internal_database.transaction_context() as session:
        updated_nulls = session.execute(
            ForecastData.update()
            .where(ForecastData.c.Predicted_Month <= newest_month)
            .where(ForecastData.c.Actual == None)  # noqa: E711
            .values({"Actual": 0, "Accuracy": compute_accuracy_as_sql(0, ForecastData.c.Prediction_Post)})
        )

    logger.info(
        f"Updated {updated_nulls.rowcount} rows of forecast_data without actual values to"
        f" newest actual values from cleaned_data"
    )


def get_newest_cleaned_data_month(internal_database: Database, cleaned_data_run_id: int) -> datetime:
    """Returns most recent month from cleaned data table of internal database.

    Args:
        internal_database: Service to access internal database.
        cleaned_data_run_id: ID of latest successful platform run, which included cleaning.
    """
    with internal_database.transaction_context() as session:
        return cast(
            datetime,
            session.query(func.max(CleanedData.c.Date))  # type: ignore
            .filter(CleanedData.c.run_id == cleaned_data_run_id)
            .scalar(),
        )


def get_last_successful_cleaning_run_id(internal_database: Database) -> int:
    """Returns last successful and completed cleaning run ID.

    Args:
        internal_database: Service to access internal database.
    """
    with internal_database.transaction_context() as session:
        cleaning_run_id = (
            session.query(func.max(ForecastRun.id))  # type: ignore
            .filter(ForecastRun.status == ForecastRunStatus.COMPLETED)
            .filter(ForecastRun.includes_cleaning == True)
            .scalar()
        )
        logger.debug(f"Found highest completed cleaning run ID: {cleaning_run_id}")

        cleaned_data_ids = session.query(distinct(CleanedData.c.run_id)).all()  # type: ignore
        logger.debug(f"Found cleaned data IDs in internal database: {cleaned_data_ids}")

        if not cleaning_run_id or len(cleaned_data_ids) != 1 or cleaned_data_ids[0][0] != cleaning_run_id:
            raise DataException(
                "Cannot determine valid cleaning data, "
                "please re-run cleaning steps, "
                'e.g. by providing the "--force-reload" parameter'
            )
        return int(cleaning_run_id)
