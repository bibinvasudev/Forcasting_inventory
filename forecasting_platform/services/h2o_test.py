from contextlib import closing
from unittest.mock import Mock

import pytest
from _pytest.monkeypatch import MonkeyPatch
from h2o.backend import (
    H2OCluster,
    H2OConnection,
)
from h2o.exceptions import H2OConnectionError

from . import h2o
from .h2o import (
    _connect_h2o_server,
    _init_h2o_server,
    initialize_h2o_connection,
)

FALLBACK_H20_PORT = 55555
VALID_LOCAL_URL = f"http://localhost:{FALLBACK_H20_PORT}"
VALID_URL = "http://localhost:54321"
INVALID_URL = "http://test.invalid:54321"


def get_h2o_connection_mock(cluster_is_running: bool = True) -> Mock:
    cluster_mock = Mock(spec=H2OCluster)
    cluster_mock.is_running.return_value = cluster_is_running

    connection_mock = Mock(spec=H2OConnection)
    connection_mock.cluster = cluster_mock
    connection_mock.base_url = VALID_LOCAL_URL
    return connection_mock


def test_connect_h2o_connects_for_single_valid_url() -> None:
    with closing(_connect_h2o_server(urls=[VALID_URL])) as connection:
        assert connection.cluster.is_running()


def test_connect_h2o_fails_for_only_invalid_url() -> None:
    with pytest.raises(H2OConnectionError):
        with closing(_connect_h2o_server(urls=[INVALID_URL])) as connection:
            assert connection is None


def test_connect_h2o_connects_to_first_valid_url() -> None:
    with closing(_connect_h2o_server(urls=[INVALID_URL, VALID_URL])) as connection:
        assert connection.cluster.is_running()


def test_connect_h2o_connects_fails_for_empty_urls() -> None:
    with pytest.raises(H2OConnectionError):
        with closing(_connect_h2o_server(urls=[])) as connection:
            assert connection is None


def test_init_h2o_connects(monkeypatch: MonkeyPatch) -> None:
    monkeypatch.setattr(h2o, "connection_h2o", get_h2o_connection_mock)
    monkeypatch.setattr(h2o, "init_h2o", Mock())
    with closing(_init_h2o_server(port=FALLBACK_H20_PORT)) as connection:
        assert connection.cluster.is_running() is True


def test_init_h2o_fails_if_there_is_connection_error(monkeypatch: MonkeyPatch) -> None:
    def raise_h2o_connection_error() -> H2OConnectionError:
        raise H2OConnectionError

    monkeypatch.setattr(h2o, "connection_h2o", raise_h2o_connection_error)
    monkeypatch.setattr(h2o, "init_h2o", Mock())
    with pytest.raises(H2OConnectionError):
        with closing(_init_h2o_server(port=FALLBACK_H20_PORT)) as connection:
            assert connection is None


def test_initialize_h2o_connection_connects_to_first_valid_url() -> None:
    with closing(
        initialize_h2o_connection(urls=[INVALID_URL, VALID_URL], fallback_port=FALLBACK_H20_PORT)
    ) as connection:
        assert connection.cluster.is_running()
        assert VALID_URL == connection.base_url


def test_initialize_h2o_connection_connects_to_locally_started_server(monkeypatch: MonkeyPatch) -> None:
    monkeypatch.setattr(h2o, "connection_h2o", get_h2o_connection_mock)
    monkeypatch.setattr(h2o, "init_h2o", Mock())
    with closing(initialize_h2o_connection(urls=[], fallback_port=FALLBACK_H20_PORT)) as connection:
        assert connection.cluster.is_running()
        assert VALID_LOCAL_URL == connection.base_url


def test_initialize_h2o_connection_fails_if_can_not_connect_to_locally_started_server(monkeypatch: MonkeyPatch) -> None:
    monkeypatch.setattr(h2o, "connection_h2o", lambda: None)
    monkeypatch.setattr(h2o, "init_h2o", Mock())
    with pytest.raises(H2OConnectionError):
        with closing(initialize_h2o_connection(urls=[], fallback_port=FALLBACK_H20_PORT)) as connection:
            assert connection is None
