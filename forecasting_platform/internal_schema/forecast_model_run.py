from __future__ import annotations

from datetime import datetime
from typing import TYPE_CHECKING

from forecasting_platform.static import (
    FORECAST_MODEL_RUN_TABLE,
    FORECAST_RUN_TABLE,
    ForecastModelRunStatus,
    SqlAlchemyEnum,
)
from sqlalchemy import (
    VARCHAR,
    Column,
    DateTime,
    ForeignKey,
    Integer,
)
from sqlalchemy.ext.declarative import DeferredReflection
from sqlalchemy.orm import relationship

from .internal_schema_base import InternalSchemaBase

if TYPE_CHECKING:
    from .forecast_run import ForecastRun  # noqa: F401


class ForecastModelRun(DeferredReflection, InternalSchemaBase):
    """Represent an account-specific model run in the internal database, including the updated status."""

    __tablename__ = FORECAST_MODEL_RUN_TABLE

    id = Column(Integer, primary_key=True, autoincrement=True, nullable=False)

    run_id = Column(Integer, ForeignKey(f"{FORECAST_RUN_TABLE}.id", ondelete="CASCADE"), nullable=False)
    run = relationship("ForecastRun", back_populates="model_runs")

    model_name = Column(VARCHAR(length=100), nullable=False)

    start = Column(DateTime, nullable=False)
    end = Column(DateTime, nullable=True)

    status = Column(SqlAlchemyEnum(ForecastModelRunStatus), nullable=False)

    def __str__(self) -> str:
        return (
            f"<ForecastModelRun("
            f"id={self.id}, "
            f"run_id={self.run_id}, "
            f"model_name={self.model_name}, "
            f"status={self.status}"
            f")>"
        )

    @staticmethod
    def create(forecast_run_id: int, model_name: str) -> ForecastModelRun:
        """Construct a newly initialized :class:`ForecastModelRun` instance."""
        return ForecastModelRun(
            run_id=forecast_run_id,
            start=datetime.utcnow(),
            status=ForecastModelRunStatus.INITIALIZED,
            model_name=model_name,
        )
