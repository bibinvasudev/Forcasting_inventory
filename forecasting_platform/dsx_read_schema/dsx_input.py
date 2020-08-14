from forecasting_platform.static import DSX_INPUT_TABLE
from sqlalchemy import (
    DECIMAL,
    NVARCHAR,
    Column,
    DateTime,
    Integer,
    Table,
)

from .dsx_read_schema_base import DsxReadSchemaBase

#: Table schema definition for reading raw inputs from dsx_read database.
DsxInput = Table(
    DSX_INPUT_TABLE,
    DsxReadSchemaBase.metadata,
    Column("enditemid", Integer, nullable=False),
    Column("itemname", NVARCHAR(4000), nullable=False),
    Column("Ship To", NVARCHAR(63), nullable=True),
    Column("projectid", NVARCHAR(63), nullable=True),
    Column("binid", NVARCHAR(63), nullable=True),
    Column("branch", NVARCHAR(63), nullable=True),
    Column("shortid", NVARCHAR(63), nullable=True),
    Column("masterpart", NVARCHAR(63), nullable=True),
    Column("Cost", NVARCHAR(1000), nullable=True),
    Column("contractid", NVARCHAR(63), nullable=True),
    Column("projecttype", NVARCHAR(63), nullable=True),
    Column("binstatus", NVARCHAR(63), nullable=True),
    Column("lowlevelcust", NVARCHAR(63), nullable=True),
    Column("perioddate", DateTime, nullable=False),
    Column("Adjusted History", DECIMAL(precision=38, scale=7), nullable=True),
)
