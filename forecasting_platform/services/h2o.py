import logging
import traceback
from typing import (
    Any,
    List,
)

from forecasting_platform import master_config
from h2o import H2OFrame
from h2o import __version__ as h2o_client_version
from h2o import connect as connect_h2o
from h2o import connection as connection_h2o
from h2o import init as init_h2o
from h2o.backend import H2OConnection
from h2o.exceptions import H2OConnectionError

logger = logging.getLogger("h2o")


def initialize_h2o_connection(urls: List[str], fallback_port: int) -> H2OConnection:
    """Initialize a new H2O connection, and optionally start a new H2O server process.

    Args:
        urls: List of H2O server urls to try connecting to, in specified order.
            Use an empty list to avoid connections to external H2O servers.
        fallback_port: If given urls cannot be connected or list if empty, start and connect to a new H2O server on
            localhost.

    Returns:
        H2OConnection object.

    """
    logger.debug(f"Using h2o client in version {h2o_client_version}.")

    _patch_h2o_upload_python_object()

    try:
        connection = _connect_h2o_server(urls)
    except H2OConnectionError:
        connection = _init_h2o_server(fallback_port)

    _verify_h2o_version(connection)

    return connection


def _connect_h2o_server(urls: List[str]) -> H2OConnection:
    """Connect to a list of H2O server urls, first successful connection is returned.

    Args:
        urls: List of urls to try sequentially for connection.

    Returns:
        H2OConnection object.

    """
    connection = None
    connection_error = None

    for url in urls:
        try:
            logger.debug(f"Attempting connection to {url}")
            connection = connect_h2o(url=url, verbose=False)
        except H2OConnectionError as e:
            logger.warning(f"Failed connection attempt to {url}")
            connection_error = e
            continue
        else:
            logger.debug(f"Successfully connected to {url}")
            connection_error = None
            break

    if connection_error:
        raise connection_error

    if not connection:
        raise H2OConnectionError("Failed to connect to h2o server for unknown reason")

    return connection


def _verify_h2o_version(connection: H2OConnection) -> None:
    h2o_server_version = connection.cluster.version
    if h2o_server_version != h2o_client_version:
        logger.warning(f"h2o server uses different version {h2o_server_version} than client {h2o_client_version}")


def _init_h2o_server(port: int) -> H2OConnection:
    """Connect to a local H2O server running on a given port, if not successful start a new server and connect to it.

    Args:
        port: Local port to connect and start a new H2O server process.

    Returns:
        H2OConnection object.

    """
    logger.info(f"Attempting connection and starting new local server on port {port}")
    init_h2o(port=port, max_mem_size=master_config.fallback_h2o_max_mem_size_GB)
    logger.info(f"Successfully connected to local server on port {port}")

    connection = connection_h2o()
    if not connection:
        raise H2OConnectionError("Failed to connect to h2o server for unknown reason")

    return connection


def _patch_h2o_upload_python_object() -> None:
    """Workaround a rare race-condition which has been observed under high-load on Windows.

    If our specific error case was not found, H2O will just behave as usual.
    """
    original_upload_python_object = H2OFrame._upload_python_object

    def wrapped_upload_python_object(*args: Any, **kwargs: Any) -> Any:
        try:
            return original_upload_python_object(*args, **kwargs)
        except PermissionError as error:
            # Ugly workaround to ignore PermissionError from the temporary file cleanup,
            # see https://github.com/h2oai/h2o-3/blob/master/h2o-py/h2o/frame.py#L150
            if traceback.extract_tb(error.__traceback__)[1].line == "os.remove(tmp_path)  # delete the tmp file":
                logger.warning(f"Ignoring error from H2O temporary file cleanup, with unknown root-cause: {error}")
            else:
                raise

    H2OFrame._upload_python_object = wrapped_upload_python_object
