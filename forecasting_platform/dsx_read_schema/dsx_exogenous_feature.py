from forecasting_platform.static import DSX_EXOGENOUS_FEATURE_TABLE
from sqlalchemy import (
    NUMERIC,
    VARCHAR,
    Column,
    DateTime,
    Integer,
    Table,
)

from .dsx_read_schema_base import DsxReadSchemaBase

#: Table schema definition for reading exogenous features inputs from dsx_read database.
DsxExogenousFeature = Table(
    DSX_EXOGENOUS_FEATURE_TABLE,
    DsxReadSchemaBase.metadata,
    Column("enditemid", Integer, nullable=True),
    Column("binid", VARCHAR(30), nullable=True),
    Column("projectid", VARCHAR(30), nullable=True),
    Column("ship to", Integer, nullable=True),
    Column("branch", Integer, nullable=True),
    Column("contractid", VARCHAR(30), nullable=True),
    Column("custmiscinfo", VARCHAR(30), nullable=True),
    Column("airframe", VARCHAR(30), nullable=True),
    Column("program", VARCHAR(30), nullable=True),
    Column("periodicdatastream", VARCHAR(50), nullable=True),
    Column("perioddate", DateTime, nullable=True),
    Column("value", NUMERIC(precision=18, scale=4), nullable=True),
)
