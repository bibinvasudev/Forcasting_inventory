import logging
import sys
from contextlib import contextmanager
from typing import Iterator

import click
from forecasting_platform.helpers.compare_structure_database import (
    assert_dsx_output_count,
    assert_dsx_output_total_sum,
    assert_forecast_data_for_model_run,
    assert_number_of_completed_model_runs,
    get_last_successful_production_run,
    get_model_run_ids_for_forecast_run,
)
from forecasting_platform.services import initialize
from forecasting_platform.static import EngineRunType

EXPECTED_DSX_OUTPUT_COUNT = 214789
EXPECTED_DSX_OUTPUT_TOTAL_SUM = 33861844


logger = logging.getLogger("compare_structure_database")


@click.command(hidden=True, name="compare-structure-database")
def compare_structure_database_command() -> None:
    """Validate database data for last successful production run against expected metrics."""
    with initialize(EngineRunType.development) as services:
        if any([services.internal_database.is_disabled(), services.dsx_write_database.is_disabled()]):
            error_msg = "Cannot validate database tables, because database connection is not available"
            logger.error(error_msg)
            click.echo(click.style(error_msg, fg="red"))
            sys.exit(1)

        run = get_last_successful_production_run(services.internal_database)
        if not run:
            error_msg = "Could not determine last successful production run"
            logger.error(error_msg)
            click.echo(click.style(error_msg, fg="red"))
            sys.exit(1)

        model_run_ids = get_model_run_ids_for_forecast_run(services.internal_database, run.id)

        failed = False

        @contextmanager
        def log_assertion() -> Iterator[None]:
            """Log all assertion errors before exiting to avoid stopping after the first error is encountered."""
            try:
                yield
            except AssertionError as e:
                nonlocal failed
                failed = True
                logger.error(e)
                click.echo(click.style(str(e), fg="red"))

        # assert internal database

        with log_assertion():
            assert_number_of_completed_model_runs(model_run_ids)

        for model_run_id in model_run_ids:
            with log_assertion():
                assert_forecast_data_for_model_run(services.internal_database, model_run_id, run)

        # assert DSX database

        with log_assertion():
            assert_dsx_output_count(services.dsx_write_database, EXPECTED_DSX_OUTPUT_COUNT)

        with log_assertion():
            assert_dsx_output_total_sum(services.dsx_write_database, EXPECTED_DSX_OUTPUT_TOTAL_SUM)

        if failed:
            sys.exit(1)

    click.echo(click.style("All database entries for last production run have valid structure", fg="green"))
