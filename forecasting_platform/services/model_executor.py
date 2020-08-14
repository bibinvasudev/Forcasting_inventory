import logging
from concurrent.futures import ProcessPoolExecutor
from multiprocessing import Queue
from multiprocessing.context import SpawnContext
from typing import (
    Callable,
    Sequence,
)

from forecasting_platform import master_config

from .logging import initialize_subprocess_logging
from .runtime_config import RuntimeConfig
from .signal_handler import initialize_subprocess_termination_signal

logger = logging.getLogger("orchestrator")


def execute_models(
    runtime_config: RuntimeConfig,
    multiprocessing_context: SpawnContext,
    log_queue: "Queue[logging.LogRecord]",
    execution_function: Callable[..., None],
    execution_functions_args: Sequence[object],
) -> None:  # pragma: no cover
    """Execute a parallelized or sequential model run, depending on current configuration.

    If only a single model is executed or :data:`~forecasting_platform.master_config.max_parallel_models` is 1,
    then no multi-processing is used and the models are run within the main process.

    Args:
        runtime_config: Current RuntimeConfig.
        multiprocessing_context: Context to use for :class:`ProcessPoolExecutor`.
        log_queue: Logging queue to collect log messages from sub-processes.
        execution_function: Function to call with model config class as the first parameter.
        execution_functions_args: Other parameters for ``execution_function``.
    """
    assert master_config.max_parallel_models > 0, "Expected master_config.max_parallel_models to be greater than 0."

    if master_config.max_parallel_models <= 1 or len(runtime_config.model_configs) <= 1:
        logger.info(
            f"Skipping multiprocessing "
            f"(max_parallel_models={master_config.max_parallel_models}, "
            f"number of model_configs={len(runtime_config.model_configs)})"
        )
        for model_config_class in runtime_config.model_configs:
            execution_function(model_config_class, *execution_functions_args)
        return

    with ProcessPoolExecutor(
        max_workers=master_config.max_parallel_models,
        mp_context=multiprocessing_context,
        initializer=_initialize_worker,
        initargs=(log_queue,),
    ) as executor:
        results = [
            executor.submit(execution_function, model_config_class, *execution_functions_args)
            for model_config_class in runtime_config.model_configs
        ]

    last_error = None
    for result in results:
        if error := result.exception():
            logger.error(f"Error during multiprocessing: {error}")
            last_error = error

    if last_error:
        raise last_error


def _initialize_worker(log_queue: "Queue[logging.LogRecord]") -> None:  # pragma: no cover
    initialize_subprocess_termination_signal()
    initialize_subprocess_logging(log_queue)
