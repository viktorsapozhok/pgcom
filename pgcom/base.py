__all__ = [
    'BaseConnector',
    'BaseCommuter',
]

from contextlib import contextmanager
from typing import (
    Any,
    Iterator,
    List,
    Mapping,
    Optional,
    Sequence,
    Tuple,
    Union
)

import abc

import numpy as np
import psycopg2
from psycopg2 import sql
from psycopg2.extensions import register_adapter, AsIs
from psycopg2.extras import execute_batch

from . import exc

QueryParams = Union[Sequence[Any], Mapping[str, Any]]
register_adapter(np.int64, AsIs)
register_adapter(np.float64, AsIs)


class BaseConnector(abc.ABC):
    """Base class for all connectors.

    Args:
        schema:
            If schema is specified,
            then setting a connection to the schema only.
    """

    def __init__(self, schema: Optional[str] = None, **kwargs: str) -> None:
        self.schema = schema
        self._kwargs = kwargs

        if 'db_name' in self._kwargs:
            self._kwargs['dbname'] = self._kwargs.pop('db_name')

        if self.schema is not None:
            if 'options' in self._kwargs:
                self._kwargs['options'] += f' --search_path={self.schema}'
            else:
                self._kwargs['options'] = f'--search_path={self.schema}'

    def __del__(self) -> None:
        self.close_all()

    def __repr__(self) -> str:
        desc = '('
        for key in ['host', 'user', 'dbname']:
            if key in self._kwargs.keys():
                desc += key + '=' + self._kwargs[key] + ' '
        if self.schema is not None:
            desc += 'schema=' + self.schema + ')'
        else:
            desc += 'schema=None)'
        return desc

    @abc.abstractmethod
    @contextmanager
    def open_connection(self) -> Iterator[psycopg2.connect]:
        """Generates a new connection.
        """

        raise NotImplementedError

    @abc.abstractmethod
    def close_all(self) -> None:
        """Close all active connections.
        """

        raise NotImplementedError


class BaseCommuter:
    """Base class for all commuters.

    Args:
        connector:
            Instance of connection handler, any subclass
            inherited from :class:`~pgcom.base.BaseConnector`.
    """

    def __init__(self, connector: BaseConnector) -> None:
        self.connector = connector

    def __repr__(self) -> str:
        return repr(self.connector)

    def execute(
            self,
            cmd: Union[str, sql.Composed],
            values: Optional[QueryParams] = None
    ) -> None:
        """Execute a database operation (query or command).

        Args:
            cmd:
                SQL query to be executed.
            values:
                Query parameters.

        Returns:
            List of rows of a query result and list of column names.
            Two empty lists are returned if there is no records to fetch.

        Raises:
            QueryExecutionError: if execution fails.
        """

        self._execute(cmd=cmd, values=values)

    def _execute(
            self,
            cmd: Union[str, sql.Composed],
            values: Optional[QueryParams] = None,
            commit: Optional[bool] = True,
            batch: Optional[bool] = False
    ) -> Tuple[List[Any], List[str]]:
        """Execute a database operation, query or command.

        Args:
            cmd:
                SQL command.
            values:
                Query parameters.
            commit:
                Commit the results if True.
            batch:
                Use execute_batch method if True.

        Returns:
            List of rows of a query result and list of column names.
            Two empty lists are returned if there is no records to fetch.

        Raises:
            QueryExecutionError: if execution fails.
        """

        fetched = []
        columns = []

        with self.connector.open_connection() as conn:
            try:
                with conn.cursor() as cur:
                    if batch:
                        execute_batch(cur, cmd, values)
                    else:
                        if values is None:
                            cur.execute(cmd)
                        else:
                            cur.execute(cmd, values)

                    if cur.description is not None:
                        fetched = cur.fetchall()
                        columns = [desc[0] for desc in cur.description]

                if commit:
                    conn.commit()
            except Exception as e:
                try:
                    conn.rollback()
                except Exception as ex:
                    exc.raise_with_traceback(
                        exc.QueryExecutionError(
                            f'Execution failed on sql: {cmd}\n{ex}\n '
                            f'unable to rollback'))

                exc.raise_with_traceback(
                    exc.QueryExecutionError(
                        f'Execution failed on sql: {cmd}\n{e}\n'))

        return fetched, columns

    def _get_schema(
            self,
            table_name: str,
            schema: Optional[str] = None
    ) -> Tuple[str, str]:
        """Return schema and table names.
        """

        names = str.split(table_name, '.')

        if len(names) == 2:
            return names[0], names[1]

        if schema is not None:
            _schema = schema
        else:
            if self.connector.schema is not None:
                _schema = self.connector.schema
            else:
                _schema = 'public'

        return _schema, table_name
