"""Setup Python :mod:warnings module."""

import warnings

from sqlalchemy.exc import SAWarning


def initialize_warnings() -> None:
    """Initialize filters for expected or unavoidable warnings."""
    warnings.filterwarnings("ignore", category=PendingDeprecationWarning, module="h2o")
    warnings.filterwarnings("ignore", category=ResourceWarning, module="h2o")
    warnings.filterwarnings("ignore", category=DeprecationWarning, module="h2o")
    warnings.filterwarnings("ignore", category=SyntaxWarning, module="h2o")  # '"is" with a literal. Did you mean "=="?'

    warnings.filterwarnings(
        "ignore", message="No driver name specified", category=SAWarning, module="sqlalchemy.connectors.pyodbc"
    )
