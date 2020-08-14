class ConfigurationException(Exception):
    """CLI configuration error. Indicates CLI parameters are inconsistent or incompatible with the input data."""

    pass


class DatabaseConnectionFailure(Exception):
    """Database cannot be accessed or network connection cannot be established."""

    pass


class DataException(Exception):
    """Could not find data, data is empty or failed validation."""

    pass
