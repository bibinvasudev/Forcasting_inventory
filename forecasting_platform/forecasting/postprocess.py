import logging
import random
from typing import (
    Callable,
    List,
)

import numpy as np
import pandas as pd
from forecasting_platform.static import (
    PREDICT_RESULT_TYPE,
    TRAIN_RESULT_TYPE,
)
from owforecasting.scoring import TimeSeriesScoring
from owforecasting.timeseries import TimeSeries

from .outliers import adjust_outliers_to_standard_deviation

logger = logging.getLogger("postprocess")


def postprocess_forecast_results(
    pred: pd.DataFrame, grouping: List[str], fc_start: pd.Timestamp, depth: int = 4, is_adhoc_forecast: bool = False
) -> pd.DataFrame:
    """Postprocess raw prediction, including filtering and grouping.

    Args:
        pred: Output :class:`~pandas.DataFrame` from ts_pred.result_data.
        grouping: List containing all column names that define granularity of postprocessed forecast values.
        fc_start: Start Date of Forecast.
        depth: Integer defining the considered time period in months when performing postprocessing.
        is_adhoc_forecast: Adjust forecast based on the standard deviation of the predictions.

    Returns:
        :class:`~pandas.DataFrame` with post-processed results.

    """
    # get training data
    train_data = pred.loc[pred["type"] == TRAIN_RESULT_TYPE]
    predict_data = pred.loc[pred["type"] == PREDICT_RESULT_TYPE]

    # filter for last depth month of training data
    mask = pred["Date"] >= fc_start - pd.DateOffset(months=depth)
    mask &= pred["Date"] <= fc_start - pd.DateOffset(months=1)
    pred_filtered = pred.loc[mask]
    # reset index
    pred_filtered = pred_filtered.reset_index(drop=True)

    # groupby and aggregate order quantity to variance and mean
    pred_filtered_grouped = (
        pred_filtered.groupby(grouping, observed=True)
        .agg(var=pd.NamedAgg("Order_Quantity", np.var), value=pd.NamedAgg("Order_Quantity", np.mean))
        .reset_index()
    )

    if is_adhoc_forecast:
        pred_postprocessed = _postprocess_adhoc_forecast(grouping, pred_filtered_grouped, predict_data)
    else:
        pred_postprocessed = _postprocess_forecast(grouping, pred_filtered_grouped, predict_data)

    # drop value and var column. not necessary anymore
    pred_postprocessed.drop(columns=["value", "var"], inplace=True)

    # append processed prediction data to train_data again
    result = train_data.append(pred_postprocessed, ignore_index=True)

    logger.info(f"Postprocessed {len(result)} rows of forecast result")

    return result


def _postprocess_forecast(
    grouping: List[str], pred_filtered_grouped: pd.DataFrame, predict_data: pd.DataFrame
) -> pd.DataFrame:
    """Set forecast to fixed historic sales value, if historic sales variance is 0."""
    # get subset of data that has variance of 0
    pred_filtered_grouped = pred_filtered_grouped.loc[
        pred_filtered_grouped["var"] == 0, grouping + ["value", "var"]
    ].reset_index(drop=True)

    # left merge of order quantity that hasn't changed on predict_data
    pred_postprocessed = predict_data.merge(pred_filtered_grouped, how="left", on=grouping, validate="many_to_one")

    # In predict_data do: if value != NaN: use value, else: use forecast value
    pred_postprocessed["Order_Quantity"] = pred_postprocessed["value"].combine_first(
        pred_postprocessed["Order_Quantity"]
    )

    return pred_postprocessed


def _postprocess_adhoc_forecast(
    grouping: List[str], pred_filtered_grouped: pd.DataFrame, predict_data: pd.DataFrame
) -> pd.DataFrame:
    # left merge of order quantity that hasn't changed on predict_data
    pred_postprocessed = predict_data.merge(pred_filtered_grouped, how="left", on=grouping, validate="many_to_one")

    pred_postprocessed = adjust_outliers_to_standard_deviation(pred_postprocessed, "Order_Quantity")

    return pred_postprocessed


# TODO FSC-51 Remove and do not use in model_configs, this function shall not be part of final handover
def postprocess_exclude_items(
    prediction: pd.DataFrame,
    costs: pd.DataFrame,
    ts: TimeSeries,
    grouping: List[str],
    filter_function: Callable[[pd.DataFrame], pd.DataFrame.mask],
    threshold: float,
) -> pd.DataFrame:  # pragma: no cover
    """Filter items from prediction during post-processing, excluding items based on the MAPE score.

    Args:
        prediction: Raw prediction data.
        costs: Cost per item.
        ts: Forecasted TimeSeries.
        grouping: Grouping for the MAPE calculation.
        filter_function: Function defining the range in which forecasts are compared.
        threshold: Filter items which are above this MAPE threshold.

    Returns:
        Prediction in the same format, excluding the filtered items.

    """
    pred = prediction.copy()

    # create scoring object for additional scope
    score_item = TimeSeriesScoring(
        ts, weights=costs, pos_weight="Unit_Cost", neg_weight="Unit_Cost", compare_range=filter_function,
    )
    score_item.add_forecast("mape", pred)

    # get MAPE on item level
    mape_item_level = score_item.calculate_mape(
        percentage_level=["Item_ID"], reporting_level=["Item_ID"], absolute_level=grouping
    )

    # convert to DataFrame
    mape_item_level = pd.DataFrame(mape_item_level).reset_index()

    # merge column with MAPE to prediction data
    pred = pred.merge(mape_item_level, how="left", on=["Item_ID"], validate="many_to_one")

    # exclude items that are above MAPE threshold
    pred_keep = pred.loc[(pred["mape"] <= threshold) | (pred["mape"].isnull() == True)]

    # delete column with MAPE again
    pred_keep = pred_keep.drop(columns=["mape"])

    return pred_keep


def _random_forecast_extract(df: pd.DataFrame, limit: int) -> pd.DataFrame:
    return random.sample(range(1, limit + 1), df)


# TODO FSC-320 This is a strong candidate for clean-up/refactoring
def reduce_hits(
    post: pd.DataFrame, runner_flag: pd.DataFrame, grouping: List[str], fc_start: pd.Timestamp
) -> pd.DataFrame:
    """Reduce forecast result, by dropping prediction months to 0, while keeping the overall expected sales frequency.

    Args:
        post: Post-processed ts_pred.result_data.
        runner_flag: :class:`~pandas.DataFrame` mapping ItemIDs to the "Is_Runner" flag (True/False).
        grouping: List containing all column names that define granularity of postprocessed forecast values.
        fc_start: Start date of Forecast.

    Returns:
        :class:`~pandas.DataFrame` with reduced number of months with sales per contract/item combination.

    """
    post = post.groupby(grouping + ["Date"]).agg({"Order_Quantity": "sum", "type": "first"})
    post = post.reset_index()

    post = post.merge(runner_flag, on="Item_ID", how="left")
    post["Is_Runner"] = post["Is_Runner"].fillna(False)
    post["Order_Quantity"] = post["Order_Quantity"].fillna(0)

    post_RR = post[post["Is_Runner"]]
    post_NRR = post[~post["Is_Runner"]]

    post_train = post_RR[post_RR["type"] == TRAIN_RESULT_TYPE].copy()
    post_predict = post_RR[(post_RR["type"] == PREDICT_RESULT_TYPE) | (post_RR["type"] == "test")].copy()

    unique_forecasts = post_predict["Date"].nunique()

    mask = post_RR["Date"] >= fc_start - pd.DateOffset(months=12)
    mask &= post_RR["Date"] < fc_start
    post_filtered = post_RR.loc[mask]

    # Step 1: Reduce number of hits

    post_ltm_hits = (
        post_filtered[post_filtered["Order_Quantity"] > 0].groupby(grouping)["Order_Quantity"].count().reset_index()
    )
    post_ltm_hits["ratio_hits"] = post_ltm_hits["Order_Quantity"] / 12

    post_ltm_hits_forecast = (
        post_predict[post_predict["Order_Quantity"] > 0].groupby(grouping)["Order_Quantity"].count().reset_index()
    )
    post_ltm_hits_forecast.rename(columns={"Order_Quantity": "fc_hits"}, inplace=True)
    post_ltm_hits_forecast["ratio_hits_fc"] = post_ltm_hits_forecast["fc_hits"] / unique_forecasts
    post_ltm_hits_forecast = post_ltm_hits_forecast.merge(
        post_ltm_hits[grouping + ["ratio_hits"]], on=grouping, how="left"
    )
    post_ltm_hits_forecast["new_hits"] = np.round(
        post_ltm_hits_forecast["fc_hits"]
        * post_ltm_hits_forecast["ratio_hits"]
        / post_ltm_hits_forecast["ratio_hits_fc"]
    )

    post_predict["rank"] = post_predict.groupby(grouping)["Date"].rank().sample(frac=1).astype(int)
    post_predict = post_predict.merge(post_ltm_hits_forecast[grouping + ["new_hits"]], on=grouping, how="left")

    post_predict["new_hits"].fillna(0, inplace=True)
    post_predict_list = post_predict.groupby(grouping)["new_hits"].max().reset_index()
    post_predict_list["new_hits"] = post_predict_list["new_hits"].map(int)
    post_predict_list["list"] = post_predict_list["new_hits"].apply(_random_forecast_extract, limit=unique_forecasts)

    post_predict = post_predict.merge(post_predict_list[grouping + ["list"]], on=grouping, how="left")

    post_predict["keep"] = post_predict.apply(lambda df: df["rank"] in df["list"], axis=1)
    post_predict["Order_Quantity"] = np.where(post_predict["keep"], post_predict["Order_Quantity"], 0)

    # Step 2: Adjust output in cases we have reduced it too much by reducing hits
    post_mean_sales = post_filtered.copy()
    post_mean_sales["mean_sales"] = post_mean_sales["Order_Quantity"]
    post_mean_sales["std_sales"] = post_mean_sales["Order_Quantity"]
    post_mean_sales = post_mean_sales.groupby(grouping).agg({"mean_sales": np.mean, "std_sales": np.std}).reset_index()

    predict_mean_sales = post_predict.groupby(grouping)["Order_Quantity"].sum().reset_index()
    predict_mean_sales.rename(columns={"Order_Quantity": "sum_predict"}, inplace=True)

    compare_output = predict_mean_sales.merge(post_mean_sales, on=grouping, how="left")
    compare_output["excessiveReduction"] = np.where(
        compare_output["sum_predict"]
        < unique_forecasts * compare_output["mean_sales"] - unique_forecasts * compare_output["std_sales"],
        True,
        False,
    )

    post_predict = post_predict.merge(
        compare_output[grouping + ["excessiveReduction", "mean_sales"]], on=grouping, how="left",
    )
    post_predict["Order_Quantity"] = np.where(
        (post_predict["excessiveReduction"]) & (post_predict["rank"] <= post_predict["new_hits"]),
        post_predict["mean_sales"],
        post_predict["Order_Quantity"],
    )

    post_predict = post_predict.drop(
        columns=["rank", "new_hits", "excessiveReduction", "mean_sales", "list", "keep", "Is_Runner"]
    )

    post_train = post_train.drop(columns=["Is_Runner"])
    post_NRR = post_NRR.drop(columns=["Is_Runner"])

    out = post_NRR.append(post_predict, ignore_index=True).append(post_train, ignore_index=True)
    return out
