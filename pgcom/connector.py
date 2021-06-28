__all__ = ["Connector"]

from contextlib import contextmanager
import random
import time
from typing import Any, Iterator, Mapping, Sequence, Union

import psycopg2
from psycopg2 import pool

from .base import BaseConnector

QueryParams = Union[Sequence[Any], Mapping[str, Any]]


class Connector(BaseConnector):
    """Setting a connection with database.

    Besides the basic connection parameters any other
    connection parameter supported by
    `psycopg2.connect <https://www.psycopg.org/docs/module.html>`_
    can be passed as a keyword.

    Args:
        pool_size:
            The maximum amount of connections the pool will support.
        pre_ping:
            If True, the pool will emit a "ping" on the connection to
            test if the connection is alive. If not, the connection will
            be reconnected.
        max_reconnects:
            The maximum amount of reconnects, defaults to 3.
    """

    _pool: pool.SimpleConnectionPool

    def __init__(
        self,
        pool_size: int = 20,
        pre_ping: bool = False,
        max_reconnects: int = 3,
        **kwargs: str
    ) -> None:
        super().__init__(**kwargs)

        self.pool_size = pool_size
        self.pre_ping = pre_ping
        self.max_reconnects = max_reconnects

        self._pool = self.make_pool()

    @contextmanager
    def open_connection(self) -> Iterator[psycopg2.connect]:
        """Generate a free connection from the pool.

        If ``pre_ping`` is True, then the connection is tested
        whether its alive or not. If not, then reconnect.
        """

        conn = self._pool.getconn()

        if self.pre_ping:
            for n in range(self.max_reconnects):
                if not self.ping(conn):
                    if n > 0:
                        time.sleep(self._back_off_time(n - 1))
                    self._pool = self.restart_pool()
                    conn = self._pool.getconn()
                else:
                    break
        try:
            yield conn
        finally:
            self._pool.putconn(conn)

    def restart_pool(self) -> pool.SimpleConnectionPool:
        """Close all the connections and create a new pool."""

        self.close_all()
        return self.make_pool()

    def close_all(self) -> None:
        """Close all the connections handled by the pool."""

        if not self._pool.closed:
            self._pool.closeall()

    def make_pool(self) -> pool.SimpleConnectionPool:
        """Create a connection pool.

        A connection pool that can't be shared
        across different threads.
        """

        return pool.SimpleConnectionPool(
            minconn=1, maxconn=self.pool_size, **self._kwargs
        )

    @staticmethod
    def ping(conn: psycopg2.connect) -> bool:
        """Ping the connection for liveness.

        Implements a ping ("SELECT 1") on the connection.
        Return True if the connection is alive, otherwise False.

        Args:
            conn:
                The connection object to ping.
        """

        is_alive = False
        with conn.cursor() as cur:
            cur.execute("SELECT 1")
            if cur.description is not None:
                fetched = cur.fetchall()
                try:
                    is_alive = fetched[0][0] == 1
                except IndexError:
                    pass
        return is_alive

    @staticmethod
    def _back_off_time(n: int) -> int:
        return (2 ** n) + (random.randint(0, 1000) / 1000)
