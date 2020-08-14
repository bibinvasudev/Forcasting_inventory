import logging
from typing import Tuple

import numpy as np
import pandas as pd
from forecasting_platform.static import (
    ExogenousFeatures,
    InternalFeatures,
    Weights,
)

logger = logging.getLogger("feature_engineering")


def _decrease_weights(original_weights: pd.DataFrame, factor: float) -> Weights:
    """Decrease weights during recovery from a market shock.

    Args:
        original_weights: Non-empty :class:`~pandas.DataFrame` containing original weights.
        factor: Factor to decrease weights on monthly basis.

    Return:
        Adjusted weights in :class:`~pandas.DataFrame`.

    """
    updated_weights = original_weights.copy()
    start_value = updated_weights.loc[updated_weights.index[0], "Weight"]
    updated_weights.loc[updated_weights.index[0], "Weight"] = factor * start_value

    for i in range(1, len(updated_weights)):
        updated_weights.loc[updated_weights.index[i], "Weight"] = (
            factor * updated_weights.loc[updated_weights.index[i - 1], "Weight"]
        )

    return updated_weights


def calculate_weights_shock(
    train_start: pd.Timestamp,
    forecast_end: pd.Timestamp,
    first_shock_month: pd.Timestamp,
    last_shock_month: pd.Timestamp,
    decrease_factor: float = 0.7,
) -> Weights:
    """Calculate weighting feature for market shock.

    Args:
        train_start: Start period for training.
        forecast_end: End period for forecasting.
        first_shock_month: Month of market shock.
        last_shock_month: Last month BEFORE market recovery starts.
        decrease_factor: Optional factor to decrease weights on monthly basis during recovery from a market shock.

    Returns:
        :class:`~pandas.DataFrame` containing the external weighting feature for market shock.
    """
    assert first_shock_month <= last_shock_month, "First and last shock month are required to be set chronologically."

    assert (
        train_start <= last_shock_month
    ), "Last shock month is required to be set chronologically after training start."

    weights = pd.DataFrame({"Date": pd.date_range(start=train_start, end=forecast_end, freq="MS")})

    # Definition of market shock weight: For dates before the shock, weight is 1 and after it is 1000
    before_shock_weight = 1
    shock_weight = 1000

    mask = weights["Date"] < first_shock_month
    weights.loc[mask, "Weight"] = before_shock_weight

    mask = weights["Date"] >= first_shock_month
    mask &= weights["Date"] <= last_shock_month
    weights.loc[mask, "Weight"] = shock_weight

    mask = weights["Date"] > last_shock_month
    if len(weights.loc[mask]) > 0:
        weights.loc[mask, "Weight"] = shock_weight
        weights.loc[mask, "Weight"] = _decrease_weights(weights.loc[mask], decrease_factor)

    logger.info(f"Calculated weights based on market shock event from: {first_shock_month}")

    return weights


def generate_market_shock_feature(
    train_start: pd.Timestamp, first_shock_month: pd.Timestamp, impact_duration: int = 1199,
) -> pd.DataFrame:
    """Generate exogenous feature to add to training data in case of a market shock event (i.e. COVID-19).

    Args:
        train_start: First month of training data.
        first_shock_month: Timestamp indicating first month affected by market shock.
        impact_duration: Number of months impacted by market shock, defaults to 100 years

    Returns:
        :class:`~pandas.DataFrame` with column 'Date' to merge on and column 'Shock_Feature'
        containing the binary feature

    """
    start = train_start
    end = max(train_start, first_shock_month) + pd.DateOffset(months=impact_duration)

    dates = pd.date_range(start=start, end=end, freq="MS")
    shock_feature = pd.DataFrame({"Date": dates})
    shock_feature["Shock_Feature"] = np.nan
    mask = shock_feature["Date"] < first_shock_month
    mask |= shock_feature["Date"] > first_shock_month + pd.DateOffset(months=impact_duration)

    # Values: 0 for dates before the shock, 1 for dates during the shock
    shock_feature.loc[mask, "Shock_Feature"] = 0
    shock_feature.loc[~mask, "Shock_Feature"] = 1

    logger.info(f"Generated exogenous feature: Shock_Feature with {len(shock_feature)} rows")

    return shock_feature


def add_sales_features(sales: pd.DataFrame, features: InternalFeatures) -> pd.DataFrame:
    """Add account specific internal features to sales data.

    Args:
        sales: Sales data to process.
        features: Dictionary of internal features to calculate on sales data.

    Returns:
        ``sales`` data with added feature.

    """
    sales = sales.copy()

    # Iterate over all features to be added
    for name, func in features.items():
        sales = func(sales, name)

    return sales


def generate_build_rates_features(input_build_rates: pd.DataFrame) -> Tuple[InternalFeatures, ExogenousFeatures]:
    """Generate Internal and Exogenous features with build rates.

    Args:
        input_build_rates: Build rates as loaded from DSX

    Returns:
        Tuple of Internal and Exogenous features. If used as a return value of
        :func:`~forecasting_platform.model_config_scripts.BaseModelConfig.configure_features`,
        it will add build rate as exogenous feature for model forecast.

    """
    unprocessed_build_rates = input_build_rates.copy()
    build_rates_groupby = unprocessed_build_rates.groupby(["Contract_ID", "Airframe", "Date"])

    build_rates_max = build_rates_groupby["Value"].max()
    build_rates_min = build_rates_groupby["Value"].min()
    if not build_rates_max.equals(build_rates_min):
        logger.warning(
            "Aggregation of build rates by Contract_ID, Airframe and Date is done under assumption that "
            "the build rate value for different Project_IDs stays the same, "
            "but this assumption does not hold and can affect accuracy (proceeding with mean build rate value)"
        )

    build_rates = build_rates_groupby.mean().reset_index()
    build_rates = build_rates[["Contract_ID", "Airframe", "Date", "Value"]]

    def add_airframe_flag(sales: pd.DataFrame, name: str) -> pd.DataFrame:
        airframe_mapping = unprocessed_build_rates[["Airframe", "Project_ID", "Contract_ID"]].drop_duplicates()
        sales = sales.merge(airframe_mapping, how="left", on=["Project_ID", "Contract_ID"]).reset_index(drop=True)
        sales.rename(columns={"Airframe": name}, inplace=True)
        sales[name] = sales[name].fillna("No Airframe")

        logger.info(f"Generated internal feature: {name} with {len(sales)} rows")

        return sales

    logger.info(f"Generated exogenous feature: Build_Rate with {len(build_rates)} rows")

    return {"Airframe": add_airframe_flag}, {"Build_Rate": (build_rates, 1)}


def generate_airframe_feature(airframes: pd.DataFrame) -> InternalFeatures:
    """Generate Internal Feature with name and callback for airframe mapping.

    Args:
        airframes: Airframe mapping as loaded from DSX

    Returns:
        Internal feature. Can be used to provide the InternalFeature part of return value of
        :func:`~forecasting_platform.model_config_scripts.BaseModelConfig.configure_features`.
    """
    unprocessed_airframes = airframes.copy()

    def add_airframe_feature(sales: pd.DataFrame, name: str) -> pd.DataFrame:
        airframe_mapping = unprocessed_airframes[["Airframe", "Project_ID", "Contract_ID"]].drop_duplicates()
        sales = sales.merge(airframe_mapping, how="left", on=["Project_ID", "Contract_ID"]).reset_index(drop=True)
        sales.rename(columns={"Airframe": name}, inplace=True)
        sales[name] = sales[name].fillna("No Airframe")

        logger.info(f"Generated internal feature: {name} with {len(sales)} rows")

        return sales

    return {"Airframe_Feature": add_airframe_feature}


def calculate_default_weights(train_start: pd.Timestamp, fc_end: pd.Timestamp, weighting: int) -> Weights:
    """Calculate weighting feature.

    Args:
        train_start: Start period for training.
        fc_end: End period for forecasting.
        weighting: Weighting to use for training.

    Returns:
        :class:`~pandas.DataFrame` including a column "Weight" with the weighting.

    """
    fc_dates = pd.date_range(start=train_start, end=fc_end, freq="MS")
    weights = pd.DataFrame({"Date": fc_dates})
    weights["Weight"] = (weights["Date"].dt.year - train_start.year + 1) ** 2
    weights["Weight"] *= weighting

    logger.info(f"Calculated default weights with weighting factor: {weighting}")

    return weights


def extract_cost_info(sales: pd.DataFrame) -> pd.DataFrame:
    """Extract cost per item from sales data.

    Args:
        sales: Sales to extract cost from.

    Returns:
        :class:`~pandas.DataFrame` containing the average unit cost per item.

    """
    cost = sales.groupby("Item_ID")["Unit_Cost"].mean().reset_index()
    return cost
