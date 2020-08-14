import logging
from typing import (
    List,
    cast,
)

from forecasting_platform import master_config
from forecasting_platform.dsx_write_schema import DsxOutput
from forecasting_platform.internal_schema import (
    ForecastData,
    ForecastModelRun,
    ForecastRun,
)
from forecasting_platform.services import Database
from forecasting_platform.static import (
    EngineRunType,
    ForecastModelRunStatus,
    ForecastRunStatus,
)
from sqlalchemy import desc

from .forecast_structure import (
    ExpectedForecastStructureParameters,
    get_expected_forecast_structure,
)

logger = logging.getLogger("compare_structure_database")


def assert_dsx_output_total_sum(dsx_write_database: Database, expected_total_sum: int) -> None:
    """Assert count of values in dsx write database."""
    with dsx_write_database.transaction_context() as session:
        dsx_total_sum = sum([int(result.Value) for result in session.query(DsxOutput.c.Value).all()])  # type: ignore

    assert dsx_total_sum == expected_total_sum, (
        f"Total sum of values ({dsx_total_sum}) in {DsxOutput.name} table of {dsx_write_database} "
        f"does not match the expectation ({expected_total_sum})"
    )
    logger.info(f"Asserted total sum of values ({expected_total_sum}) in {DsxOutput.name} " f"of {dsx_write_database}")


def assert_dsx_output_count(dsx_write_database: Database, expected_count: int) -> None:
    """Assert count of of values saved in dsx write database."""
    with dsx_write_database.transaction_context() as session:
        dsx_data_count = session.query(DsxOutput).count()  # type: ignore

    assert dsx_data_count == expected_count, (
        f"Total count of  values ({dsx_data_count}) in {DsxOutput.name} table of {dsx_write_database} "
        f"does not match the expectation ({expected_count})"
    )
    logger.info(f"Asserted total count of values ({expected_count}) in {DsxOutput.name} " f"of {dsx_write_database}")


def assert_number_of_completed_model_runs(model_run_ids: List[int]) -> None:
    """Compare length of model run list against :data:`~forecasting_platform.master_config.model_configs`."""
    assert len(model_run_ids) == len(master_config.model_configs), (
        f"Number of completed model runs ({len(model_run_ids)}) "
        f"does not match the number of model_configs in master_config.py ({len(master_config.model_configs)})"
    )
    logger.info(f"Asserted number of completed model_runs ({len(model_run_ids)})")


def assert_forecast_data_for_model_run(internal_database: Database, model_run_id: int, run: ForecastRun) -> None:
    """Assert count of rows in forecast data table of internal database against expected structure."""
    with internal_database.transaction_context() as session:
        model_name = (
            session.query(ForecastModelRun)  # type: ignore
            .filter(ForecastModelRun.id == model_run_id)
            .first()
            .model_name
        )

    run_parameters = ExpectedForecastStructureParameters(
        account_name=model_name, forecast_periods=run.forecast_periods, prediction_month=run.prediction_start_month,
    )
    expected_forecast_structure = get_expected_forecast_structure(run_parameters)
    expected_number_of_rows = expected_forecast_structure.shape[0]
    forecast_data_count = get_forecast_data_count_for_model_run(internal_database, model_run_id)

    assert forecast_data_count == expected_number_of_rows, (
        f"Forecast data count ({forecast_data_count}) for {model_name} (model_run={model_run_id}) "
        f"does not match the expectation ({expected_number_of_rows}) defined by {run_parameters})"
    )
    logger.info(f"Asserted forecast data count ({forecast_data_count}) for {model_name} (model_run={model_run_id})")


def get_last_successful_production_run(internal_database: Database) -> ForecastRun:
    """Return last successful production run as detached object."""
    with internal_database.transaction_context() as session:
        session.expire_on_commit = False
        forecast_run = (
            session.query(ForecastRun)  # type: ignore
            .filter(ForecastRun.status == ForecastRunStatus.COMPLETED)
            .filter(ForecastRun.run_type == EngineRunType.production)
            .order_by(desc(ForecastRun.id))
            .limit(1)
            .first()
        )

    logger.debug(f"Found last completed production forecast run: {forecast_run}")
    return cast(ForecastRun, forecast_run)


def get_model_run_ids_for_forecast_run(internal_database: Database, forecast_run_id: int) -> List[int]:
    """Return successfully completed model run IDs associated with the forecast run."""
    with internal_database.transaction_context() as session:
        model_run_ids = [
            result.id
            for result in (
                session.query(ForecastModelRun)  # type: ignore
                .filter(ForecastModelRun.run_id == forecast_run_id)
                .filter(ForecastModelRun.status == ForecastModelRunStatus.COMPLETED)
                .all()
            )
        ]
    logger.debug(
        f"Found {len(model_run_ids)} completed model run(s) (id={model_run_ids}) "
        f"associated with forecast run ID: {forecast_run_id}"
    )
    return model_run_ids


def get_forecast_data_count_for_model_run(internal_database: Database, model_run_id: int) -> int:
    """Return count of forecast_data entries associated with model run."""
    with internal_database.transaction_context() as session:
        forecast_data_count = (
            session.query(ForecastData)  # type: ignore
            .filter(ForecastData.c.model_run_id == model_run_id)
            .count()
        )

    logger.debug(f"Found {forecast_data_count} forecast data rows associated with forecast model run: {model_run_id}")
    return cast(int, forecast_data_count)
