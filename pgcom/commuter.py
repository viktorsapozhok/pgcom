__all__ = [
    'Connector',
    'Commuter'
]

from contextlib import contextmanager
from datetime import datetime
from functools import wraps
from io import StringIO
from typing import (
    Any,
    Callable,
    Iterator,
    List,
    Optional,
    Sequence,
    Tuple,
    Union
)

import numpy as np
import pandas as pd
import psycopg2
from psycopg2 import pool
from psycopg2.extras import execute_batch

from . import exc, queries


def fix_schema(func: Callable) -> Callable:
    """Unifies schema definitions.

    It applies `Commuter._get_schema()` method before calling the wrapped
    function and propagates the resulting `table_name` and `schema`
    to the wrapper.

    It allows to call wrappers as it is shown in the following examples.
    In the last example, the schema `model` should be defined earlier
    when instance of the `Commuter` object has been created.

    Examples:

        .. code::

            >>> func(table_name='people', schema='model')
            >>> func(table_name='model.people')
            >>> func(table_name='people')
    """

    @wraps(func)
    def wrapped(  # type: ignore
            self,
            table_name: str,
            *args: Any,
            schema: Optional[str] = None,
            **kwargs: Any
    ) -> Any:
        schema, table_name = self._get_schema(
            table_name=table_name,
            schema=schema)

        return func(self, table_name, *args, schema=schema, **kwargs)

    return wrapped


class Connector:
    """Setting a connection with database.

    Besides the basic connection parameters any other
    connection parameter supported by `psycopg2.connect`
    can be passed as a keyword.

    Args:
        host:
            Database host address.
        port:
            Connection port number.
        user:
            User name.
        password:
            User password.
        db_name:
            The database name.
        pool_size:
            ToDo
        schema:
            If schema is specified,
            then setting a connection to the schema only.
    """

    def __init__(
            self,
            host: str,
            port: str,
            user: str,
            password: str,
            db_name: str,
            pool_size: Optional[int] = None,
            schema: Optional[str] = None,
            **kwargs: Any
    ) -> None:
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.db_name = db_name
        self.schema = schema
        self._kwargs = kwargs

        self._pool = None

        if self.schema is not None:
            self._kwargs['options'] = f'--search_path={self.schema}'

        if pool_size is not None:
            self._pool = self.make_pool(max_conn=pool_size)

    def __del__(self) -> None:
        self.close_all()

    def __repr__(self) -> str:
        schema = 'public' if self.schema is None else self.schema
        return f'(' \
               f'host={self.host}, ' \
               f'user={self.user}, ' \
               f'dbname={self.db_name}, ' \
               f'schema={schema})'

    @property
    def closed(self) -> bool:
        """ToDo
        """

        if self._pool is None:
            return True
        else:
            return self._pool.closed

    @contextmanager
    def open_connection(self) -> Iterator[psycopg2.connect]:
        """ToDo
        """

        if not self.closed:
            conn = self._pool.getconn()
        else:
            conn = self.make_connection()

        try:
            yield conn
        finally:
            if not self.closed:
                self._pool.putconn(conn)
            else:
                conn.close()

    def make_pool(self, max_conn: int) -> pool.SimpleConnectionPool:
        """ToDO
        """

        return pool.SimpleConnectionPool(
            minconn=1, maxconn=max_conn,
            user=self.user, password=self.password,
            host=self.host, port=self.port, dbname=self.db_name,
            **self._kwargs)

    def make_connection(self) -> psycopg2.connect:
        """Setting `psycopg2` connection.
        """

        return psycopg2.connect(
            user=self.user, password=self.password,
            host=self.host, port=self.port, dbname=self.db_name,
            **self._kwargs)

    def close_all(self) -> None:
        """ToDo
        """

        if not self.closed:
            self._pool.closeall()

    @staticmethod
    def ping(conn) -> bool:
        """ToDo
        """

        sta = False
        with conn.cursor() as cur:
            cur.execute('SELECT 1')
            if cur.description is not None:
                fetched = cur.fetchall()
                try:
                    sta = fetched[0][0] == 1
                except IndexError:
                    pass
        return sta


class Commuter:
    """PostgreSQL communication agent.

    Args:
        host:
            Database host address.
        port:
            Connection port number.
        user:
            User name.
        password:
            User password.
        db_name:
            The database name.
        pre_ping:
            ToDo

    Keyword Args:
        pool_size:
            ToDo
        schema:
            If schema is specified,
            then setting a connection to the schema only.
    """

    def __init__(
            self,
            host: str,
            port: str,
            user: str,
            password: str,
            db_name: str,
            pre_ping: bool = False,
            **kwargs: Any
    ) -> None:
        self.pre_ping = pre_ping
        self.connector = Connector(
            host, port, user, password, db_name, **kwargs)

    def __repr__(self) -> str:
        return repr(self.connector)

    def select(self, cmd: str) -> pd.DataFrame:
        """Reads SQL query into a DataFrame.

        Args:
            cmd:
                string SQL query to be executed.

        Returns:
            Pandas.DataFrame.
        """

        records, columns = self._execute(cmd)
        df = pd.DataFrame.from_records(records, columns=columns)
        return df

    def select_one(
            self,
            cmd: str,
            default: Optional[Any] = None
    ) -> Any:
        """Select the first element of returned DataFrame.

        Args:
            cmd:
                string SQL query to be executed.
            default:
                If query result is empty, then return default value.
        """

        fetched, _ = self._execute(cmd)

        try:
            value = fetched[0][0]

            if value is None:
                value = default
        except IndexError:
            value = default

        return value

    @fix_schema
    def insert(
            self,
            table_name: str,
            data: pd.DataFrame,
            schema: str = 'public'
    ) -> None:
        """Write records stored in a DataFrame to database.

        Args:
            table_name:
                Name of the destination table.
            data:
                Pandas.DataFrame with the data to be inserted.
            schema:
                Name of the database schema.
        """

        table_name = self._table_name(table_name, schema)
        columns = ','.join(list(data.columns))
        values = 'VALUES({})'.format(','.join(['%s' for _ in data.columns]))
        cmd = 'INSERT INTO {} ({}) {}'.format(table_name, columns, values)

        self._execute(cmd=cmd, values=data.values, batch=True)

    def execute(
            self,
            cmd: str,
            values: Optional[Sequence[Any]] = None
    ) -> None:
        """Execute a database operation (query or command).
        """

        self._execute(cmd=cmd, values=values)

    def execute_script(
            self,
            path2script: str
    ) -> None:
        with open(path2script, 'r') as fh:
            cmd = fh.read()

        self._execute(cmd=cmd)

    @fix_schema
    def insert_row(
            self,
            table_name: str,
            schema: str = 'public',
            return_id: Optional[str] = None,
            **kwargs: Any
    ) -> Union[int, None]:
        """Implements `INSERT INTO ... VALUES ...` command.

        Args:
            table_name:
                Name of the destination table.
            schema:
                Name of the database schema.
            return_id:
                Name of the returned serial key.
        """

        sid = None
        keys = ', '.join(kwargs.keys())
        values = ''

        for key in kwargs.keys():
            if len(values) > 0:
                values += ','

            value = kwargs[key]

            if isinstance(value, datetime):
                values += f'\'{value}\''
            elif isinstance(value, str):
                values += f'\'{value}\''
            else:
                values += str(value)

        table_name = self._table_name(table_name, schema)
        cmd = 'INSERT INTO ' + table_name + ' (' + keys + ') '
        cmd += 'VALUES (' + values + ')'

        if return_id is not None:
            sid = self.insert_return(cmd, return_id=return_id)
        else:
            self._execute(cmd=cmd)

        return sid

    def insert_return(
            self,
            cmd: str,
            values: Optional[Sequence[Any]] = None,
            return_id: Optional[str] = None
    ) -> int:
        """Insert a new row to the table and
        return the serial key of the newly inserted row.

        Args:
            cmd:
                `INSERT INTO` command.
            values:
                Inserted values.
            return_id:
                Name of the returned serial key.
        """

        if return_id is not None:
            cmd += 'RETURNING ' + return_id

        fetched, _ = self._execute(cmd, values)

        try:
            sid = fetched[0][0]
        except IndexError:
            sid = 0

        return sid

    @fix_schema
    def copy_from(
            self,
            table_name: str,
            data: pd.DataFrame,
            schema: str = 'public',
            format_data: bool = False,
            where: Optional[str] = None
    ) -> None:
        """Places DataFrame to buffer and apply copy_from method.

        Args:
            table_name:
                Name of the table where to insert.
            data
                DataFrame from where to insert.
            schema:
                Name of the schema.
            format_data:
                Reorder columns and adjust dtypes wrt to table metadata
                from information_schema.
            where:
                WHERE clause used to specify a condition while deleting
                data from the table before applying copy_from,
                DELETE command is not executed if not specified.
        """

        df = data.copy()

        if format_data:
            df = self._format_data(df, table_name, schema)

        table_name = self._table_name(table_name, schema)

        with self.connector.open_connection() as conn:
            try:
                with conn.cursor() as cur:
                    if where is not None:
                        cur.execute(f'DELETE FROM {table_name} WHERE {where}')

                    # DataFrame to buffer
                    s_buf = StringIO()
                    df.to_csv(s_buf, index=False, header=False)
                    s_buf.seek(0)

                    cur.copy_from(
                        file=s_buf,
                        table=table_name,
                        sep=',',
                        null='',
                        columns=df.columns)

                conn.commit()
            except Exception as e:
                try:
                    conn.rollback()
                except Exception as ex:
                    exc.raise_with_traceback(
                        exc.CopyError(f'{ex}\n unable to rollback'))

                exc.raise_with_traceback(exc.CopyError(f'{e}\n'))

    @fix_schema
    def is_table_exist(
            self,
            table_name: str,
            schema: str = 'public'
    ) -> bool:
        """Return True if table exists, otherwise False.
        """

        df = self.select(queries.is_table_exist(table_name, schema))
        return bool(len(df) > 0)

    def get_connections_count(self) -> int:
        """Returns the amount of active connections.
        """

        return self.select_one(
            cmd='SELECT SUM(numbackends) FROM pg_stat_database',
            default=0)

    @fix_schema
    def resolve_primary_conflicts(
            self,
            table_name: str,
            data: pd.DataFrame,
            schema: str = 'public',
            where: Optional[str] = None
    ) -> pd.DataFrame:
        """Resolve primary key conflicts in DataFrame.

        This method selects data from `table_name` and removes all
        rows from the given DataFrame, which violate primary key
        constraint in the selected data.

        Parameter `where` is used to reduce the amount of querying data.
        It specifies the `WHERE` clause in the `SELECT` query.

        Args:
            table_name:
                Name of the table.
            data:
                DataFrame with primary key conflicts.
            schema:
                Name of the schema.
            where:
                User defined `WHERE` clause used when selecting
                data from `table_name`.

        Returns:
            pd.DataFrame without primary key conflicts.
        """

        p_key = self._primary_key(table_name, schema)
        p_key = p_key['column_name'].to_list()

        df = data.copy()

        if len(p_key) > 0:
            table_name = self._table_name(table_name, schema)

            cmd = 'SELECT * FROM ' + table_name
            if where is not None:
                cmd += ' WHERE ' + where

            table_data = self.select(cmd)

            if not table_data.empty:
                df.set_index(p_key, inplace=True)
                table_data.set_index(p_key, inplace=True)

                # remove rows which are in table data index
                df = df[~df.index.isin(table_data.index)]
                # reset index and sort columns
                df = df.reset_index(level=p_key)
                df = df[data.columns]

        return df

    @fix_schema
    def resolve_foreign_conflicts(
            self,
            table_name: str,
            parent_name: str,
            data: pd.DataFrame,
            schema: str = 'public',
            where: Optional[str] = None,
            parent_schema: Optional[str] = None
    ) -> pd.DataFrame:
        """Resolve foreign key conflicts in DataFrame.

        This method selects data from `parent_table` and removes all
        rows from the given DataFrame, which violate foreign key
        constraint in the selected data.

        Parameter `where` is used to reduce the amount of querying data.
        It specifies the `WHERE` clause in the `SELECT` query.

        Args:
            table_name:
                Name of the child table, where data needs to be inserted.
            parent_name:
                Name of the parent table.
            data:
                DataFrame with foreign key conflicts.
            schema:
                Name of the child table schema.
            where:
                User defined `WHERE` clause used when selecting
                data from `table_name`.
            parent_schema:
                Name of the parent table schema.

        Returns:
            pd.DataFrame without foreign key conflicts.
        """

        df = data.copy()

        parent_schema, parent_name = self._get_schema(
            table_name=parent_name,
            schema=parent_schema)

        foreign_key = self._foreign_key(
            table_name, parent_name, schema, parent_schema)

        if len(foreign_key) > 0:
            parent_table = self._table_name(parent_name, parent_schema)

            cmd = 'SELECT * FROM ' + parent_table
            if where is not None:
                cmd += ' WHERE ' + where

            parent_data = self.select(cmd)

            if not parent_data.empty:
                df.set_index(
                    foreign_key['child_column'].to_list(), inplace=True)
                parent_data.set_index(
                    foreign_key['parent_column'].to_list(), inplace=True)

                # remove rows which are not in parent index
                df = df[df.index.isin(parent_data.index)]
                # reset index and sort columns
                df = df.reset_index(level=foreign_key['child_column'].to_list())
                df = df[data.columns]
            else:
                df = pd.DataFrame()

        return df

    def _execute(
            self,
            cmd: str,
            values: Optional[Sequence[Any]] = None,
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
                Commit results if True.
            batch:
                Use execute_batch method if True.

        Returns:
            List of rows of a query result and list of column names.
            Two empty lists are returned if there is no records to fetch.

        Raises:
            QueryExecutionError if execution fails.
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

    def _table_columns(
            self,
            table_name: str,
            schema: str
    ) -> pd.DataFrame:
        """Return columns attributes of the given table.

        Args:
            table_name:
                Name of the table.
            schema:
                Name of the schema.

        Returns:
            Pandas.DataFrame with the names and data types of all
            columns for the given table.
        """

        return self.select(queries.column_names(table_name, schema))

    def _get_schema(
            self,
            table_name: str,
            schema: Optional[str] = None
    ) -> Tuple[str, str]:
        """Return schema and table names.

        Examples:

            .. code::

            >>> self._get_schema(table_name='my_schema.my_table')
            ('my_schema', 'my_table')
            >>> self._get_schema(table_name='my_table')
            ('public', 'my_table')
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

    def _primary_key(
            self,
            table_name: str,
            schema: str
    ) -> pd.DataFrame:
        """Return names of all columns of the primary key.

        Args:
            table_name:
                Name of the table.
            schema:
                Name of the schema.

        Returns:
            Pandas.DataFrame with the names and data types of all
            columns of the primary key for the given table.
        """

        return self.select(queries.primary_key(table_name, schema))

    def _foreign_key(
            self,
            table_name: str,
            parent_name: str,
            schema: str,
            parent_schema: str
    ) -> pd.DataFrame:
        """Return names of all columns of the foreign key.

        Args:
            table_name:
                Name of the child table.
            parent_name:
                Name of the parent table.
            schema:
                Name of the child schema.
            parent_schema:
                Name of the parent schema.

        Returns:
            Pandas.DataFrame with columns: `child_column`, `parent_column`.
        """

        return self.select(queries.foreign_key(
            table_name, schema, parent_name, parent_schema))

    def _format_data(
            self,
            data: pd.DataFrame,
            table_name: str,
            schema: str
    ) -> pd.DataFrame:
        """Formatting DataFrame before applying copy_from.
        """

        # names of the table columns from information schema
        table_columns = self._table_columns(table_name, schema)

        # intersection of DataFrame and table columns
        columns = []  # type: List[str]

        # adjust dtypes of DataFrame columns
        for row in table_columns.itertuples():
            column = row.column_name

            if column in data.columns:
                columns += [column]

                if row.data_type in ['smallint', 'integer', 'bigint']:
                    if data[column].dtype == np.float:
                        data[column] = data[column].round().astype('Int64')
                elif row.data_type in ['text']:
                    try:
                        data[column] = data[column].str.replace(',', '')
                    except AttributeError:
                        continue
        return data[columns]

    @staticmethod
    def _table_name(
            table_name: str,
            schema: str
    ) -> str:
        """Join schema and table_name to single string.
        """

        if schema == 'public':
            return table_name
        else:
            return schema + '.' + table_name
