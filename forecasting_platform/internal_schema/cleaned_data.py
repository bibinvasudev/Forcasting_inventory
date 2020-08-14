from forecasting_platform.static import CLEANED_DATA_TABLE
from sqlalchemy import (
    VARCHAR,
    Column,
    DateTime,
    Float,
    Integer,
    PrimaryKeyConstraint,
    Table,
)

from .internal_schema_base import InternalSchemaBase

#: Table schema definition for storing cleaned (pre-processed) data in the internal database.
CleanedData = Table(
    CLEANED_DATA_TABLE,
    InternalSchemaBase.metadata,
    # ``run_id`` references the FORECAST_RUN_TABLE to avoid inconsistencies when cleaning is accidentally done in
    # different runs at the same time. It is also used for deleting the previous cleaned data.
    Column("run_id", Integer, nullable=False),
    # Following columns are used for the actual cleaned data
    Column("Project_ID", VARCHAR(length=30), nullable=False),
    Column("Contract_ID", VARCHAR(length=30), nullable=False, index=True),
    Column("Wesco_Master_Number", VARCHAR(length=30), nullable=False),
    Column("Date", DateTime, nullable=False),
    Column("Date_YYYYMM", Integer, nullable=False),
    Column("Item_ID", Integer, nullable=False),
    Column("Unit_Cost", Float, nullable=False),
    Column("Order_Quantity", Float, nullable=False),
    Column("Order_Cost", Float, nullable=False),
    PrimaryKeyConstraint(
        "run_id", "Contract_ID", "Project_ID", "Date", "Item_ID", "Order_Quantity", mssql_clustered=True,
    ),
)
