from forecasting_platform.static import DSX_OUTPUT_TABLE
from sqlalchemy import (
    NVARCHAR,
    Boolean,
    Column,
    DateTime,
    Integer,
    Table,
)

from .dsx_write_schema_base import DsxWriteSchemaBase

#: Table schema definition for storing forecast result data in the dsx_write database.
DsxOutput = Table(
    DSX_OUTPUT_TABLE,
    DsxWriteSchemaBase.metadata,
    Column("STG_Import_Periodic_ML_RowId", Integer, nullable=True),
    Column("Item Name", NVARCHAR(1024), nullable=True),
    Column("Ship To", NVARCHAR(1024), nullable=True),
    Column("projectid", NVARCHAR(1024), nullable=True),
    Column("binid", NVARCHAR(1024), nullable=True),
    Column("branch", NVARCHAR(1024), nullable=True),
    Column("PeriodicDataElementType", NVARCHAR(1024), nullable=True),
    Column("PeriodicDataElement", NVARCHAR(1024), nullable=True),
    Column("PeriodDate", NVARCHAR(1024), nullable=True),
    Column("Value", NVARCHAR(1024), nullable=True),
    Column("NewItemFlag", Boolean, nullable=False, default=False),
    Column("CreatedDateTime", DateTime, nullable=False),
)
