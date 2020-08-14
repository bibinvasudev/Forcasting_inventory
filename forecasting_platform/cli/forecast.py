import logging
import sys
from typing import (
    Optional,
    Type,
    Union,
)

import click
import pandas as pd
from forecasting_platform.model_config_scripts import BaseModelConfig
from forecasting_platform.services import initialize
from forecasting_platform.static import (
    ConfigurationException,
    EngineRunType,
    OutputFormat,
)

from .options import forecast_options

logger = logging.getLogger("forecast")


@click.command()
@forecast_options(EngineRunType.backward)
@click.option(
    "--force-reload",
    is_flag=True,
    default=False,
    show_default=True,
    help="Include import and cleaning of DSX data to internal database.",
)
def backward(
    forecast_periods: int,
    output_location: str,
    prediction_month: pd.Timestamp,
    optimize_hyperparameters: bool,
    force_reload: bool,
    output_format: OutputFormat,
    only_model_config: Optional[str],
    exclude_model_config: Optional[str],
) -> None:
    """Run back-testing forecast saved into files."""
    run_forecast(
        engine_run_type=EngineRunType.backward,
        forecast_periods=forecast_periods,
        output_location=output_location,
        prediction_month=prediction_month,
        output_format=output_format,
        optimize_hyperparameters=optimize_hyperparameters,
        force_reload=force_reload,
        only_model_config=only_model_config,
        exclude_model_config=exclude_model_config,
    )


@click.command()
@forecast_options(EngineRunType.development)
def development(
    forecast_periods: int,
    output_location: str,
    prediction_month: pd.Timestamp,
    optimize_hyperparameters: bool,
    output_format: OutputFormat,
    only_model_config: Optional[str],
    exclude_model_config: Optional[str],
) -> None:
    """Run forward-looking forecast saved into internal database."""
    run_forecast(
        engine_run_type=EngineRunType.development,
        forecast_periods=forecast_periods,
        output_location=output_location,
        prediction_month=prediction_month,
        output_format=output_format,
        optimize_hyperparameters=optimize_hyperparameters,
        only_model_config=only_model_config,
        exclude_model_config=exclude_model_config,
    )


@click.command()
@forecast_options(EngineRunType.production)
def production(
    forecast_periods: int,
    output_location: str,
    prediction_month: pd.Timestamp,
    optimize_hyperparameters: bool,
    output_format: OutputFormat,
    only_model_config: Optional[str],
    exclude_model_config: Optional[str],
) -> None:
    """Run forward-looking forecast saved into internal and DSX database."""
    run_forecast(
        engine_run_type=EngineRunType.production,
        forecast_periods=forecast_periods,
        output_location=output_location,
        prediction_month=prediction_month,
        output_format=output_format,
        optimize_hyperparameters=optimize_hyperparameters,
        only_model_config=only_model_config,
        exclude_model_config=exclude_model_config,
    )


# CAUTION: These parameters are also used in FC_Account_Template.ipynb, try to avoid breaking changes!
def run_forecast(
    engine_run_type: EngineRunType,
    forecast_periods: int,
    output_location: str,
    prediction_month: pd.Timestamp,
    output_format: OutputFormat,
    optimize_hyperparameters: bool = False,
    force_reload: bool = False,
    only_model_config: Optional[Union[Type[BaseModelConfig], str]] = None,
    exclude_model_config: Optional[str] = None,
) -> None:
    """Run a complete forecast pipeline. Can also be used from Jupyter notebook to develop new accounts.

    Args:
        engine_run_type: Type of engine run (i.e. backward or forward) and type of data output (file vs. database).
        forecast_periods: Number of periods to predict.
        output_location: Location to write the outputs to.
        prediction_month: Number to start the prediction (forward) or end the prediction (backward).
        output_format: File format for outputting files.
        optimize_hyperparameters: Activate hyper-parameter optimization during training.
        force_reload: Force re-loading of data from DSX.
        only_model_config: Only run forecast for given model config.
        exclude_model_config: Exclude given model config from forecasting.
    """
    try:
        with initialize(
            engine_run_type=engine_run_type,
            forecast_periods=forecast_periods,
            output_location=output_location,
            prediction_start_month=prediction_month,
            output_format=output_format,
            optimize_hyperparameters=optimize_hyperparameters,
            force_reload=force_reload,
            only_model_config=only_model_config,
            exclude_model_config=exclude_model_config,
        ) as services:
            services.runtime_config.log_config()

            services.orchestrator.run()

            model_configs = services.runtime_config.model_configs
            logger.info(f"Successfully ran forecast for {len(model_configs)} account(s).")
            click.echo(click.style(f"Successfully ran forecast for {len(model_configs)} account(s).", fg="green"))

    except KeyboardInterrupt:
        logger.error("Exiting because user terminated the process.")
        click.echo(click.style("Exiting because user terminated the process.", fg="red"), err=True)
        sys.exit(1)
    except ConfigurationException as e:
        logger.error(f"Configuration error: {e}")
        click.echo(click.style("Exiting because of configuration error.", fg="red"), err=True)
        sys.exit(1)
