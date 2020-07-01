from pgcom import Commuter
from .conftest import conn_params

commuter = Commuter(**conn_params)


def test_connection():
    with commuter.connector.open_connection() as conn:
        assert conn is not None


def test_connection_keywords():
    _commuter = Commuter(**conn_params, sslmode='allow', schema='public')
    with _commuter.connector.open_connection() as conn:
        assert conn is not None
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
