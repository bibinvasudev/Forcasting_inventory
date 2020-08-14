from typing import (
    Optional,
    cast,
)

from forecasting_platform.internal_schema import ForecastRun
from forecasting_platform.services import Orchestrator


def get_forecast_run(orchestrator: Orchestrator) -> Optional[ForecastRun]:
    """Returns ForecastRun object associated with current platform run based on given Orchestrator testing instance.

    Args:
        orchestrator: Service for orchestrating platform run.
    """
    with orchestrator._internal_database.transaction_context() as session:
        results = (
            session.query(ForecastRun)  # type: ignore
            .filter(ForecastRun.id == orchestrator._forecast_run_id)
            .all()
        )

        if not results:
            return None

        assert len(results) == 1
        forecast_run = results[0]
        session.expunge(forecast_run)  # type: ignore
        return cast(ForecastRun, forecast_run)
