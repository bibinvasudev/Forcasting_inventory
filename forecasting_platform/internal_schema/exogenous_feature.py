from forecasting_platform.static import EXOGENOUS_FEATURE_TABLE
from sqlalchemy import (
    VARCHAR,
    Column,
    DateTime,
    Float,
    Integer,
    Table,
)

from .internal_schema_base import InternalSchemaBase

#: Table schema definition for storing exogenous feature data imported from DSX in the internal database.
ExogenousFeature = Table(
    EXOGENOUS_FEATURE_TABLE,
    InternalSchemaBase.metadata,
    # ``run_id`` references the FORECAST_RUN_TABLE to avoid inconsistencies when cleaning is accidentally done in
    # different runs at the same time. It is also used for deleting the previous exogenous feature data.
    Column("run_id", Integer, nullable=False),
    # Following columns are used for the actual exogenous feature data
    Column("Periodic_Data_Stream", VARCHAR(length=64), nullable=False),
    Column("Airframe", VARCHAR(length=128), nullable=False),
    Column("Contract_ID", VARCHAR(length=30), nullable=False),
    Column("Project_ID", VARCHAR(length=30), nullable=False),
    Column("Date", DateTime, nullable=True),  # NULL in case of Airframes
    Column("Value", Float, nullable=True),  # NULL in case of Airframes
)
