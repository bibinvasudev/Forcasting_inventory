from functools import wraps
from typing import (
    Any,
    Callable,
    TypeVar,
    cast,
)

import click
from forecasting_platform import master_config
from forecasting_platform.static import EngineRunType

from .validation import (
    validate_forecast_periods,
    validate_model_config,
    validate_output_format,
    validate_prediction_end_month,
    validate_prediction_start_month,
)

# Generic to preserve original function signature
# see https://mypy.readthedocs.io/en/stable/generics.html#decorator-factories
F = TypeVar("F", bound=Callable[..., None])


def forecast_options(engine_run_type: EngineRunType) -> Callable[[F], F]:
    """Decorate a forecast command to add CLI options.

    Args:
        engine_run_type: Specifies forward/backward run and type of data output (file vs. database).

    Returns:
        A decorator method to add forecast options.

    """
    options = [
        click.option(
            "--forecast-periods",
            callback=validate_forecast_periods,
            default=master_config.default_forecast_periods,
            show_default=True,
            help="Number of months to use for forecasting.",
        ),
        click.option(
            "--output-location",
            default=master_config.default_output_location,
            show_default=True,
            type=click.Path(exists=True),
            help="Directory used to store resulting files.",
        ),
        click.option(
            "--only-model-config",
            callback=validate_model_config,
            default=None,
            help="Optional name of single model config to run (e.g. ModelConfigAccount1).",
        ),
        click.option(
            "--exclude-model-config",
            callback=validate_model_config,
            default=None,
            help="Optional name of model config to exclude (e.g. ModelConfigAccount1).",
        ),
        click.option(
            "--output-format",
            callback=validate_output_format,
            default=master_config.default_output_format.value,
            show_default=True,
            help="Choose 'csv' or 'xlsx' file output format.",
        ),
        click.option(
            "--optimize-hyperparameters",
            is_flag=True,
            default=False,
            show_default=True,
            help="Determine optimal hyper-parameters, "
            "and store them in a new JSON file next to the forecast results",
        ),
    ]
    if engine_run_type is EngineRunType.backward:
        options += [
            click.option(
                "--prediction-end-month",
                "prediction_month",
                callback=validate_prediction_end_month,
                default=master_config.default_prediction_start_month,
                help="Last month of backward prediction in format YYYYMM (e.g. '202002' for February 2020).",
                show_default="last month",
            )
        ]
    else:
        options += [
            click.option(
                "--prediction-start-month",
                "prediction_month",
                callback=validate_prediction_start_month,
                default=master_config.default_prediction_start_month,
                help="First month of prediction in format YYYYMM (e.g. '202002' for February 2020).",
                show_default="current month",
            )
        ]

    def decorator(function: F) -> F:
        for option in options:
            function = option(function)

        @wraps(function)
        def wrapper(*args: Any, **kwargs: Any) -> None:
            function(*args, **kwargs)

        return cast(F, wrapper)

    return decorator
