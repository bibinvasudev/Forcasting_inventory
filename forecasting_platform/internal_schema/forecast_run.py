from __future__ import annotations

from datetime import datetime
from typing import TYPE_CHECKING

from forecasting_platform.static import (
    FORECAST_RUN_TABLE,
    EngineRunType,
    ForecastRunStatus,
    SqlAlchemyEnum,
)
from sqlalchemy import (
    Boolean,
    Column,
    DateTime,
    Integer,
)
from sqlalchemy.ext.declarative import DeferredReflection
from sqlalchemy.orm import relationship

from .forecast_model_run import ForecastModelRun
from .internal_schema_base import InternalSchemaBase

if TYPE_CHECKING:
    from forecasting_platform.services import RuntimeConfig


class ForecastRun(DeferredReflection, InternalSchemaBase):
    """Represent a forecast run in the internal database, with relevant configuration and status information."""

    __tablename__ = FORECAST_RUN_TABLE

    id = Column(Integer, primary_key=True, autoincrement=True, nullable=False)

    start = Column(DateTime, nullable=False)
    end = Column(DateTime, nullable=True)

    run_type = Column(SqlAlchemyEnum(EngineRunType), nullable=False)
    includes_cleaning = Column(Boolean, nullable=False)

    status = Column(SqlAlchemyEnum(ForecastRunStatus), nullable=False)
    forecast_periods = Column(Integer, nullable=False)
    prediction_start_month = Column(DateTime, nullable=False)

    model_runs = relationship(ForecastModelRun, passive_deletes=True, back_populates="run")

    def __str__(self) -> str:
        return f"<ForecastRun(id={self.id}, status={self.status})>"

    @staticmethod
    def create(runtime_config: RuntimeConfig) -> ForecastRun:
        """Construct a newly initialized :class:`ForecastRun` instance."""
        return ForecastRun(
            start=datetime.utcnow(),
            status=ForecastRunStatus.INITIALIZED,
            run_type=runtime_config.engine_run_type,
            includes_cleaning=runtime_config.includes_cleaning,
            forecast_periods=runtime_config.full_forecast_periods,
            prediction_start_month=runtime_config.prediction_month,
        )
