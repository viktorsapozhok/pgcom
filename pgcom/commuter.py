__all__ = [
    'Connector',
    'Commuter'
]

from contextlib import contextmanager
from functools import wraps
from io import StringIO
from typing import (
    Any,
    Callable,
    Iterator,
    List,
    Mapping,
    Optional,
    Sequence,
    Tuple,
    Union
)

import numpy as np
import pandas as pd
import psycopg2
from psycopg2 import pool
from psycopg2 import sql
from psycopg2.extras import execute_batch

from . import exc, queries

QueryParams = Union[Sequence[Any], Mapping[str, Any]]


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
        pool_size:
            ToDo.
        pre_ping:
            ToDo
        schema:
            If schema is specified,
            then setting a connection to the schema only.
    """

    _pool: pool.SimpleConnectionPool

    def __init__(
            self,
            pool_size: int = 20,
            pre_ping: bool = False,
            schema: Optional[str] = None,
            **kwargs: str
    ) -> None:
        self.pool_size = pool_size
        self.pre_ping = pre_ping
        self.schema = schema
        self._kwargs = kwargs

        if 'db_name' in self._kwargs:
            self._kwargs['dbname'] = self._kwargs.pop('db_name')

        if self.schema is not None:
            if 'options' in self._kwargs:
                self._kwargs['options'] += f' --search_path={self.schema}'
            else:
                self._kwargs['options'] = f'--search_path={self.schema}'

        self._pool = self.make_pool()

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

    @contextmanager
    def open_connection(self) -> Iterator[psycopg2.connect]:
        """ToDo
        """

        conn = self._pool.getconn()

        if self.pre_ping:
            if not self.ping(conn):
                self._pool = self.restart_pool()
                conn = self._pool.getconn()

        try:
            yield conn
        finally:
            self._pool.putconn(conn)

    def restart_pool(self) -> pool.SimpleConnectionPool:
        """ToDo
        """

        self.close_all()
        return self.make_pool()

    def close_all(self) -> None:
        """ToDo
        """

        if not self._pool.closed:
            self._pool.closeall()

    def make_pool(self) -> pool.SimpleConnectionPool:
        """ToDO
        """

        return pool.SimpleConnectionPool(
            minconn=1, maxconn=self.pool_size, **self._kwargs)

    @staticmethod
    def ping(conn: psycopg2.connect) -> bool:
        """ToDo
        """

        is_alive = False
        with conn.cursor() as cur:
            cur.execute('SELECT 1')
            if cur.description is not None:
                fetched = cur.fetchall()
                try:
                    is_alive = fetched[0][0] == 1
                except IndexError:
                    pass
        return is_alive


class Commuter:
    """Communication agent.

    Args:
        pool_size:
            ToDo.
        pre_ping:
            ToDo
        schema:
            If schema is specified,
            then setting a connection to the schema only.
    """

    connector: Connector

    def __init__(
            self,
            pool_size: int = 20,
            pre_ping: bool = False,
            schema: Optional[str] = None,
            **kwargs: str
    ) -> None:
        self.connector = Connector(
            pool_size=pool_size, pre_ping=pre_ping, schema=schema, **kwargs)

    def __repr__(self) -> str:
        return repr(self.connector)

    def select(
            self,
            cmd: Union[str, sql.Composed],
            values: Optional[QueryParams] = None
    ) -> pd.DataFrame:
        """Reads SQL query into a DataFrame.

        Args:
            cmd:
                string SQL query to be executed.
            values:
                ToDo

        Returns:
            Pandas.DataFrame.
        """

        records, columns = self._execute(cmd, values=values)
        df = pd.DataFrame.from_records(records, columns=columns)
        return df

    def select_one(
            self,
            cmd: Union[str, sql.Composed],
            values: Optional[QueryParams] = None,
            default: Optional[Any] = None
    ) -> Any:
        """Select the first element of returned DataFrame.

        Args:
            cmd:
                string SQL query to be executed.
            values:
                ToDo
            default:
                If query result is empty, then return default value.
        """

        fetched, _ = self._execute(cmd, values=values)

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

        cmd = "INSERT INTO {} ({}) VALUES ({})".format(
            self._table_name(table_name, schema),
            ", ".join(list(data.columns)),
            ", ".join(['%s' for _ in data.columns]))

        self._execute(cmd=cmd, values=data.values, batch=True)

    def execute(
            self,
            cmd: str,
            values: Optional[QueryParams] = None
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
    ) -> Optional[int]:
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
        keys = list(kwargs.keys())

        cmd = sql.SQL("INSERT INTO {} ({}) VALUES ({})").format(
            sql.Identifier(schema, table_name),
            sql.SQL(', ').join(map(sql.Identifier, keys)),
            sql.SQL(', ').join(map(sql.Placeholder, keys)))

        if return_id is not None:
            sid = self.insert_return(cmd, return_id=return_id, values=kwargs)
        else:
            self._execute(cmd=cmd, values=kwargs)

        return sid

    def insert_return(
            self,
            cmd: Union[str, sql.Composed],
            values: Optional[QueryParams] = None,
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
            cmd = sql.Composed([
                sql.SQL(cmd) if isinstance(cmd, str) else cmd,
                sql.SQL(" RETURNING {}").format(sql.Identifier(return_id))])

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
            where: Optional[Union[str, sql.Composed]] = None
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

        with self.connector.open_connection() as conn:
            try:
                with conn.cursor() as cur:
                    if where is not None:
                        if isinstance(where, str):
                            where = sql.SQL(where)
                        cmd = sql.Composed([
                            sql.SQL("DELETE FROM {} WHERE ").format(
                                sql.Identifier(schema, table_name)),
                            where])
                        cur.execute(cmd)

                    table = sql.Identifier(schema, table_name).as_string(conn)

                    # DataFrame to buffer
                    s_buf = StringIO()
                    df.to_csv(s_buf, index=False, header=False)
                    s_buf.seek(0)

                    cur.copy_from(
                        file=s_buf,
                        table=table,
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

        return self.select_one(cmd=queries.conn_count(), default=0)

    @fix_schema
    def resolve_primary_conflicts(
            self,
            table_name: str,
            data: pd.DataFrame,
            schema: str = 'public',
            where: Optional[Union[str, sql.Composed]] = None
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
            if where is not None:
                if isinstance(where, str):
                    where = sql.SQL(where)
                cmd = sql.Composed([
                    sql.SQL("SELECT * FROM {} WHERE ").format(
                        sql.Identifier(schema, table_name)),
                    where])
            else:
                cmd = sql.SQL("SELECT * FROM {}").format(
                    sql.Identifier(schema, table_name))

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
            where: Optional[Union[str, sql.Composed]] = None,
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
            if where is not None:
                if isinstance(where, str):
                    where = sql.SQL(where)
                cmd = sql.Composed([
                    sql.SQL("SELECT * FROM {} WHERE ").format(
                        sql.Identifier(parent_schema, parent_name)),
                    where])
            else:
                cmd = sql.SQL("SELECT * FROM {}").format(
                    sql.Identifier(parent_schema, parent_name))

            parent_data = self.select(cmd)

            if not parent_data.empty:
                df.set_index(
                    foreign_key['child_column'].to_list(), inplace=True)
                parent_data.set_index(
                    foreign_key['parent_column'].to_list(), inplace=True)

                # remove rows which are not in parent index
                df = df[df.index.isin(parent_data.index)]
                # reset index and sort columns
                df = df.reset_index(
                    level=foreign_key['child_column'].to_list())
                df = df[data.columns]
            else:
                df = pd.DataFrame()

        return df

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
