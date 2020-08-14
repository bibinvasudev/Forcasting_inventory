from __future__ import annotations

from enum import (
    Enum,
    auto,
    unique,
)
from typing import Set


@unique
class DatabaseType(Enum):
    """Internal or DSX read/write database."""

    internal = auto()
    dsx_read = auto()
    dsx_write = auto()


@unique
class OutputFormat(Enum):
    """Output format for backward forecast."""

    csv = "csv"
    xlsx = "xlsx"


@unique
class EngineRunType(Enum):
    """Type of forecast executed by the :py:class:`~forecasting_platform.services.Orchestrator`."""

    development = "development"
    production = "production"
    backward = "backward"


@unique
class ForecastRunStatus(Enum):
    """All possible states of a :py:class:`~forecasting_platform.internal_schema.ForecastRun` instance."""

    #: Start state (default)
    INITIALIZED = "INITIALIZED"

    # Process states (only defined transitions intended)
    PREPROCESS = "PREPROCESS"
    RUN_MODELS = "RUN_MODELS"

    #: End state (final, no further state transitions allowed)
    CANCELLED = "CANCELLED"
    #: End state (final, no further state transitions allowed)
    COMPLETED = "COMPLETED"
    #: End state (final, no further state transitions allowed)
    FAILED = "FAILED"

    def is_end_state(self) -> bool:
        """Check if this enum value is final and therefore no transitions to other states are allowed.

        Returns
        -------
            ``True`` if this is an end-state, ``False`` otherwise.
        """
        return self in {ForecastRunStatus.CANCELLED, ForecastRunStatus.COMPLETED, ForecastRunStatus.FAILED}


@unique
class ForecastModelRunStatus(Enum):
    """All possible states of a :py:class:`~forecasting_platform.internal_schema.ForecastModelRun` instance."""

    #: Start state (default)
    INITIALIZED = "INITIALIZED"

    # Process states (only defined transitions intended)
    LOAD_DATA = "LOAD_DATA"
    PREPROCESS = "PREPROCESS"
    PREPARE_TRAINING = "PREPARE_TRAINING"
    FORECAST = "FORECAST"
    POSTPROCESS = "POSTPROCESS"

    #: End state (final, no further state transitions allowed)
    CANCELLED = "CANCELLED"
    #: End state (final, no further state transitions allowed)
    COMPLETED = "COMPLETED"
    #: End state (final, no further state transitions allowed)
    FAILED = "FAILED"

    def is_end_state(self) -> bool:
        """Check if this enum value is final and therefore no transitions to other states are allowed.

        Returns
        -------
            ``True`` if this is an end-state, ``False`` otherwise.
        """
        return self in self.get_end_states()

    @staticmethod
    def get_end_states() -> Set[ForecastModelRunStatus]:
        """Set of all end states for this enum."""
        return {
            ForecastModelRunStatus.CANCELLED,
            ForecastModelRunStatus.COMPLETED,
            ForecastModelRunStatus.FAILED,
        }
