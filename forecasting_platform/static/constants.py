PREDICTION_MONTH_FORMAT = "%Y%m"

PREDICT_RESULT_TYPE = "predict"
TRAIN_RESULT_TYPE = "train"

#: List of Microsoft SQL Server information values to be logged during initialization.
#: For all available information types in MS SQL Server, see:
#: https://docs.microsoft.com/en-us/sql/odbc/reference/syntax/sqlgetinfo-function#information-types
MS_SQL_DATABASE_INFOS = [
    "SQL_ODBC_VER",
    "SQL_DBMS_NAME",
    "SQL_DBMS_VER",
    "SQL_DM_VER",
    "SQL_USER_NAME",
    "SQL_DATABASE_NAME",
    "SQL_DEFAULT_TXN_ISOLATION",
    "SQL_TXN_ISOLATION_OPTION",
    "SQL_CURSOR_COMMIT_BEHAVIOR",
    "SQL_CURSOR_ROLLBACK_BEHAVIOR",
]

#: Entry for each CLI run
FORECAST_RUN_TABLE = "forecast_run"
#: Account model config run (within a CLI run)
FORECAST_MODEL_RUN_TABLE = "forecast_model_run"
#: Common prediction data for all accounts and forecast runs
FORECAST_DATA_TABLE = "forecast_data"
#: Cleaned data for all accounts
CLEANED_DATA_TABLE = "cleaned_data"
#: Exogenous feature data imported from DSX during cleaning
EXOGENOUS_FEATURE_TABLE = "exogenous_feature"
#: Raw input data for all accounts and forecast runs stored in DSX compliant format
DSX_INPUT_TABLE = "DSX_PERIODICDATA_HISTORY"
#: Prediction data for all accounts and forecast runs stored in DSX compliant format
DSX_OUTPUT_TABLE = "STG_Import_Periodic_ML"
#: Exogenous feature data imported from DSX during cleaning
DSX_EXOGENOUS_FEATURE_TABLE = "exogenous_factors_TEMPORARILY_USE_CSV_FALLBACK"  # TODO FSC-405 switch to actual DSX

#: Default log scope if no specific account context is available
LOG_DEFAULT_ACCOUNT_CONTEXT = "global"
