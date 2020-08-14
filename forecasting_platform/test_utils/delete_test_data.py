import logging

from forecasting_platform.services import Database
from forecasting_platform.static import DatabaseType
from sqlalchemy.exc import DBAPIError
from sqlalchemy.orm import Query

logger = logging.getLogger("delete_test_data")


def delete_test_data(db_query: Query, retry: bool = True) -> int:
    """Delete database objects based on user query. Intended to be used only for integration tests cleanup.

    Args:
        db_query: sqlalchemy Query object that defines the rows to delete
        retry: Attempt retry in case of deadlock errors due to parallel runs of tests

    Returns:
        Number of deleted rows.

    """
    try:
        with Database(DatabaseType.internal).transaction_context() as session:
            return int(db_query.with_session(session).delete(synchronize_session=False))  # type: ignore
    except DBAPIError as e:  # pragma: no cover
        if retry:  # Retry in case of deadlock error from the database when running tests in parallel
            logger.warning(f"Retrying error: {e}")
            return delete_test_data(db_query, retry=False)
        else:
            raise
