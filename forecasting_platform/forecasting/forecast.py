from __future__ import annotations

import logging
from typing import TYPE_CHECKING

import h2o
from forecasting_platform.static import ConfigurationException
from owforecasting import TimeSeries
from owforecasting.models import (
    H2OGradientBoostingModel,
    optimize_bayes,
)

if TYPE_CHECKING:
    from forecasting_platform.model_config_scripts import BaseModelConfig
    from forecasting_platform.services import DataOutput

logger = logging.getLogger("forecast")


def forecast_model(
    model_config: BaseModelConfig, time_series: TimeSeries, data_output: DataOutput, optimize_hyperparameters: bool
) -> TimeSeries:
    """Forecast the given :class:`~owforecasting.timeseries.TimeSeries` with a newly trained model.

    Uses :class:`~owforecasting.models.H2OGradientBoostingModel`.

    Args:
        model_config: :class:`~forecasting_platform.model_config_scripts.BaseModelConfig` with
            model specific configuration.
        time_series: :class:`~owforecasting.timeseries.TimeSeries` to forecast.
        data_output: Service for outputting data.
        optimize_hyperparameters: Optionally enable hyper-parameter optimization via
            :func:`~owforecasting.models.optimize_bayes`.

    Returns:
        :class:`~owforecasting.timeseries.TimeSeries` containing forecasted values

    """
    hyper_params = {}
    hyper_params.update(model_config.DEFAULT_HYPER_PARAMS)
    hyper_params.update(model_config.OVERRIDE_HYPER_PARAMS)

    model = H2OGradientBoostingModel(time_series, hyper_params=hyper_params, hyper_space=model_config.HYPER_SPACE)

    if optimize_hyperparameters:
        _optimize_hyperparameters(model_config, model)
        data_output.store_optimized_hyperparameters(model_config.forecast_path, model.hyper_params)  # type: ignore

    try:
        model.train()
    except ValueError as e:
        raise ConfigurationException(
            str(e)[:120] + "...\n" "Please check configuration of --prediction-start-month or --prediction-end-month."
        ) from e

    logger.info(f"Forecasting model: {model.estimator.key}")

    ts_pred = model.predict(time_series)

    logger.debug(f"Model {model.estimator.key} params: {model.estimator.params}")
    logger.debug(f"Model {model.estimator.key} variable importance: {model.estimator.varimp()}")

    _cleanup_h2o_model(model)

    return ts_pred


def _cleanup_h2o_model(model: H2OGradientBoostingModel) -> None:
    """Cleanup H2O server by removing no longer needed model and training data to free memory."""
    model_id = model.estimator.model_id
    model_params = h2o.get_model(model_id).params

    if "training_frame" not in model_params or "actual" not in model_params["training_frame"]:
        logger.info(f"Could not cleanup H2O training frame for {model_id}, which may impact memory usage")
        return

    frame_id = model_params["training_frame"]["actual"]["name"]

    try:
        all_frames = h2o.frames()["frames"]
    except h2o.exceptions.H2OResponseError as error:
        logger.debug(f"Minor H2O server error during model cleanup, may impact memory usage: {error}")
        all_frames = []

    related_frames = [f["frame_id"]["name"] for f in all_frames if model_id in f["frame_id"]["name"]]

    logger.debug(f"Cleanup model {model_id}, training frame {frame_id} and related frames {related_frames}")

    h2o.remove(model_id)
    h2o.remove(frame_id)
    for frame in related_frames:
        h2o.remove(frame)

    # https://docs.h2o.ai/h2o/latest-stable/h2o-docs/rest-api-reference.html#route-%2F3%2FGarbageCollect
    h2o.api("POST /3/GarbageCollect")


def _optimize_hyperparameters(model_config: BaseModelConfig, model: H2OGradientBoostingModel) -> None:
    logger.info(
        f"Starting hyper-parameter optimization for {model_config} with initial hyper_params={model.hyper_params}"
    )
    optimize_bayes(
        model,
        n_calls=model_config.OPTIMIZE_HYPER_PARAMETERS_N_CALLS,
        plot=False,
        verbose=False,
        gp_args=model_config._OPTIMIZE_BAYES_GP_ARGS,
    )
    logger.info(f"Finished hyper-parameter optimization for {model_config}: {model.hyper_params}")
