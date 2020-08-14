from forecasting_platform import master_config
from forecasting_platform.static import DatabaseType
from sqlalchemy import MetaData
from sqlalchemy.ext.declarative import declarative_base

#: Documentation:
#: https://docs.sqlalchemy.org/en/13/orm/extensions/declarative/api.html#sqlalchemy.ext.declarative.DeferredReflection
DsxWriteSchemaBase = declarative_base(metadata=MetaData(schema=master_config.database_schema[DatabaseType.dsx_write]))
