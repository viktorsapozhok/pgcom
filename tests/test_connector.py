from unittest.mock import patch

from pgcom import Connector, Commuter
from .conftest import conn_params

commuter = Commuter(**conn_params)


def _ping(cls, conn):
    return False


def test_connection():
    with commuter.connector.open_connection() as conn:
        assert conn is not None


def test_connection_keywords():
    _commuter = Commuter(**conn_params, sslmode='allow', schema='public')
    with _commuter.connector.open_connection() as conn:
        assert conn is not None
    del _commuter


def test_connection_options():
    _commuter = Commuter(
        **conn_params, sslmode='allow', schema='model',
        options='-c log_connections=yes')
    assert _commuter.connector._kwargs['options'] == \
           '-c log_connections=yes --search_path=model'
    assert len(str(_commuter)) > 2
    del _commuter


def test_multiple_connection():
    n_conn = commuter.get_connections_count()
    assert n_conn > 0

    for i in range(100):
        with commuter.connector.open_connection() as conn:
            assert conn is not None
    assert commuter.get_connections_count() - n_conn < 10


def test_pool_connection():
    _commuter = Commuter(**conn_params, pool_size=20)
    with _commuter.connector.open_connection() as conn:
        assert conn is not None
    del _commuter


def test_multiple_pool_connection():
    _commuter = Commuter(**conn_params, pool_size=20)
    n_conn = _commuter.get_connections_count()
    assert n_conn > 0

    for i in range(100):
        with _commuter.connector.open_connection() as conn:
            assert conn is not None
    assert commuter.get_connections_count() - n_conn < 10
    del _commuter


def test_reconnect():
    _commuter = Commuter(**conn_params, pre_ping=True, max_reconnects=2)
    with _commuter.connector.open_connection() as conn:
        assert conn is not None

    with patch.object(Connector, 'ping', new=_ping):
        with _commuter.connector.open_connection() as conn:
            assert conn is not None

    del _commuter
