from typing import Optional

import click
import pandas as pd
from forecasting_platform import master_config
from forecasting_platform.static import (
    PREDICTION_MONTH_FORMAT,
    OutputFormat,
)


def validate_forecast_periods(ctx: click.Context, param: click.Parameter, value: int) -> int:
    """Validate forecast_periods CLI parameter."""
    if 0 < value <= 50:
        return value
    raise click.BadParameter("forecast periods needs to be an integer in range 1 - 50")


def validate_prediction_start_month(ctx: click.Context, param: click.Parameter, value: str) -> pd.Timestamp:
    """Validate prediction_month CLI parameter."""
    try:
        prediction_start_month = pd.to_datetime(value, format=PREDICTION_MONTH_FORMAT)
    except ValueError:
        raise click.BadParameter(f"'{value}' does not match the format YYYYMM")
    if (
        pd.Timestamp(year=2000, month=1, day=1)
        <= prediction_start_month
        <= pd.to_datetime("today") + pd.DateOffset(months=1)
    ):
        return prediction_start_month
    raise click.BadParameter(
        "prediction start month needs to be a month between years 2000 and one month after today's month."
    )


def validate_prediction_end_month(ctx: click.Context, param: click.Parameter, value: str) -> pd.Timestamp:
    """Validate prediction_end_month CLI parameter."""
    try:
        prediction_end_month = pd.to_datetime(value, format=PREDICTION_MONTH_FORMAT)
    except ValueError:
        raise click.BadParameter(f"'{value}' does not match the format YYYYMM")
    if pd.Timestamp(year=2000, month=1, day=1) <= prediction_end_month <= pd.to_datetime("today"):
        return prediction_end_month
    raise click.BadParameter("prediction end month needs to be a month between years 2000 and today.")


def validate_output_format(ctx: click.Context, param: click.Parameter, value: str) -> OutputFormat:
    """Validate output_format CLI parameter."""
    try:
        return OutputFormat(value)
    except ValueError:
        raise click.BadParameter("Value must be 'csv' or 'xlsx''")


def validate_model_config(ctx: click.Context, param: click.Parameter, value: Optional[str]) -> Optional[str]:
    """Validate model configs used as CLI parameters."""
    if value is None:
        return None

    valid_model_configs = [config for config in master_config.model_configs if config == value]

    if len(valid_model_configs) == 1:
        return valid_model_configs[0]

    raise click.BadParameter(
        "Invalid model config name, expected one of: " + ", ".join([config for config in master_config.model_configs])
    )
