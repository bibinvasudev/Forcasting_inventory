from forecasting_platform.static import (
    FORECAST_DATA_TABLE,
    FORECAST_MODEL_RUN_TABLE,
)
from sqlalchemy import (
    VARCHAR,
    Column,
    Float,
    ForeignKey,
    Integer,
    Table,
)

from .internal_schema_base import InternalSchemaBase

#: Table schema definition for storing forecast result data in the internal database.
ForecastData = Table(
    FORECAST_DATA_TABLE,
    InternalSchemaBase.metadata,
    Column("model_run_id", Integer, ForeignKey(f"{FORECAST_MODEL_RUN_TABLE}.id"), nullable=False, primary_key=True),
    # Following are columns extracted/computed from the forecast dataframes:
    Column("Contract_ID", VARCHAR(length=100), nullable=False, primary_key=True),
    Column("Item_ID", Integer, nullable=False, primary_key=True),
    Column("Prediction_Start_Month", Integer, nullable=False, primary_key=True),
    Column("Predicted_Month", Integer, nullable=False, primary_key=True),
    Column("Prediction_Months_Delta", Integer, nullable=False),
    Column("Prediction_Raw", Float, nullable=False),
    Column("Prediction_Post", Float, nullable=False),
    Column("Actual", Integer, nullable=True),
    Column("Accuracy", Float, nullable=True),
)
