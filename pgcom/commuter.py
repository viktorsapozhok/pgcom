# -*- coding: utf-8 -*-
__all__ = [
    'Connector',
    'Commuter'
]

from collections import defaultdict
from contextlib import contextmanager
from datetime import datetime
from functools import wraps
from io import StringIO
from typing import (
    Any,
    Callable,
    Dict,
    Iterator,
    Optional,
    Sequence,
    Tuple,
    Union
)

import numpy as np
import pandas as pd
import psycopg2
from sqlalchemy import create_engine, exc
from sqlalchemy.engine import Engine

from . import queries


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

    Keyword args:
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
            **kwargs: str
    ) -> None:
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.db_name = db_name
        self.schema = kwargs.get('schema', None)

        self.conn_params = defaultdict()  # type: Dict[str, Any]

        for key in kwargs.keys():
            if key not in ['schema']:
                self.conn_params[key] = kwargs.get(key)

        self.engine = self.make_engine()
        self.conn = None

    def __del__(self) -> None:
        self.close_connection()

    def __repr__(self) -> str:
        schema = 'public' if self.schema is None else self.schema
        return f'(' \
               f'host={self.host}, ' \
               f'user={self.user}, ' \
               f'db_name={self.db_name}, ' \
               f'schema={schema})'

    @contextmanager
    def make_connection(self) -> Iterator[psycopg2.connect]:
        if self.conn is None:
            self.set_connection()

        yield self.conn

        self.close_connection()

    def make_engine(self) -> Engine:
        """Create `SQLAlchemy` engine.
        """

        engine = 'postgresql://' + \
                 self.user + ':' + \
                 self.password + '@' + \
                 self.host + ':' + \
                 self.port + '/' + \
                 self.db_name

        for key in self.conn_params.keys():
            engine += '?' + key + '=' + self.conn_params[key]

        connect_args = defaultdict()  # type: Dict[str, Any]

        if self.schema is not None:
            connect_args['options'] = '-csearch_path=' + self.schema

        return create_engine(engine, connect_args=connect_args)

    def set_connection(self) -> None:
        """Setting `psycopg2` connection.
        """

        conn_params = self.conn_params
        conn_params['host'] = self.host
        conn_params['port'] = self.port
        conn_params['user'] = self.user
        conn_params['password'] = self.password
        conn_params['dbname'] = self.db_name

        if self.schema is not None:
            conn_params['options'] = f'--search_path={self.schema}'

        self.conn = psycopg2.connect(**conn_params)

    def close_connection(self) -> None:
        if self.conn is not None:
            self.conn.close()
            self.conn = None


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

    Keyword args:
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
            **kwargs: str
    ) -> None:
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

        with self.connector.engine.connect() as conn:
            df = pd.read_sql_query(cmd, conn)

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

        df = self.select(cmd)

        try:
            value = df.iloc[0, 0]

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
            schema: str = 'public',
            chunksize: Optional[int] = None
    ) -> None:
        """Write records stored in a DataFrame to database.

        Args:
            table_name:
                Name of the destination table.
            data:
                Pandas.DataFrame with the data to be inserted.
            schema:
                Name of the database schema.
            chunksize:
                Rows will be written in batches of this size at a time.
        """

        with self.connector.engine.connect() as conn:
            try:
                data.to_sql(
                    table_name,
                    con=conn,
                    schema=schema,
                    if_exists='append',
                    index=False,
                    chunksize=chunksize)
            except (ValueError, exc.IntegrityError) as e:
                raise ValueError(e)

    def execute(
            self,
            cmd: str,
            vars: Optional[Sequence[Any]] = None,
            commit: bool = True
    ) -> None:
        """Execute a database operation (query or command).
        """

        with self.connector.make_connection() as conn:
            try:
                with conn.cursor() as cur:
                    if vars is None:
                        cur.execute(cmd)
                    else:
                        cur.execute(cmd, vars)

                if commit:
                    conn.commit()
            except psycopg2.DatabaseError as e:
                # roll back the pending transaction
                if commit:
                    conn.rollback()
                raise e

        self.connector.close_connection()

    def execute_script(
            self,
            path2script: str,
            commit: bool = True
    ) -> None:
        with open(path2script, 'r') as fh:
            script = fh.read()

        with self.connector.make_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(script)

            if commit:
                conn.commit()

        self.connector.close_connection()

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
            self.execute(cmd)

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

        sid = None

        if return_id is not None:
            cmd += 'RETURNING ' + return_id

        with self.connector.make_connection() as conn:
            try:
                with conn.cursor() as cur:
                    if values is None:
                        cur.execute(cmd)
                    else:
                        cur.execute(cmd, values)

                    sid = cur.fetchone()[0]
                    conn.commit()
            except psycopg2.DatabaseError as e:
                # roll back the pending transaction
                conn.rollback()
                raise e

        self.connector.close_connection()

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

        if format_data:
            data = self._format_data(data, table_name, schema)

        table_name = self._table_name(table_name, schema)

        with self.connector.make_connection() as conn:
            with conn.cursor() as cur:
                if where is not None:
                    cur.execute(f'DELETE FROM {table_name} WHERE {where}')

                # DataFrame to buffer
                s_buf = StringIO()
                data.to_csv(s_buf, index=False, header=False)
                s_buf.seek(0)

                # implement insert
                try:
                    cur.copy_from(s_buf, table_name, sep=',', null='')
                except (ValueError,
                        exc.ProgrammingError,
                        psycopg2.ProgrammingError,
                        psycopg2.IntegrityError) as e:
                    raise ValueError(e)

            conn.commit()

        self.connector.close_connection()

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
            cmd=f'SELECT SUM(numbackends) FROM pg_stat_database',
            default=0)

    @fix_schema
    def resolve_primary_conflicts(
            self,
            table_name: str,
            data: pd.DataFrame,
            where: str,
            schema: str = 'public'
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
            where:
                User defined `WHERE` clause used when selecting
                data from `table_name`.
            schema:
                Name of the schema.

        Returns:
            pd.DataFrame without primary key conflicts.
        """

        # extract names of the primary key columns
        p_key = self._primary_key(table_name, schema)
        p_key = p_key['column_name'].to_list()

        df = data.copy()

        if len(p_key) > 0:
            table_name = self._table_name(table_name, schema)

            cmd = f"SELECT * FROM {table_name} WHERE {where}"
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
            where: str,
            schema: str = 'public',
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
            where:
                User defined `WHERE` clause used when selecting
                data from `table_name`.
            schema:
                Name of the child table schema.
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

            cmd = f"SELECT * FROM {parent_table} WHERE {where}"
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

        # select column attributes
        table_columns = self._table_columns(table_name, schema)

        # adjust dtypes of DataFrame columns
        for row in table_columns.itertuples():
            column = row.column_name

            if row.data_type in ['smallint', 'integer', 'bigint']:
                if data[column].dtype == np.float:
                    data[column] = data[column].astype(int)

        # set columns order according to ordinal position
        data = data[table_columns['column_name'].to_list()]

        return data

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
