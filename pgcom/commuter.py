__all__ = [
    'fix_schema',
    'Commuter'
]

from functools import wraps
from io import StringIO
from typing import (
    Any,
    Callable,
    List,
    Mapping,
    Optional,
    Sequence,
    Union
)

import numpy as np
import pandas as pd
from psycopg2 import sql

from . import exc, queries
from .base import BaseCommuter
from .connector import Connector

QueryParams = Union[Sequence[Any], Mapping[str, Any]]


def fix_schema(func: Callable[..., Any]) -> Callable[..., Any]:
    """Unifies schema definitions.

    It applies :func:`~pgcom.commuter.Commuter._get_schema()` method
    before calling the wrapped function and propagates the resulting
    ``table_name` and ``schema`` to the wrapper.

    It allows to call wrappers as it is shown in the following examples.

    Examples:

        .. code::

            >>> func(table_name='people', schema='model')
            >>> func(table_name='model.people')

        In this example, the schema "model" should be defined earlier
        when instance of the commuter has been created.

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


class Commuter(BaseCommuter):
    """Communication agent.

    When creating a new instance of Commuter, the connection pool
    is created and the connection is established. The typical usage
    of Commuter is therefore once per particular database,
    held globally for the lifetime of a single application process.

    Args:
        pool_size:
            The maximum amount of connections the pool will support.
        pre_ping:
            If True, the pool will emit a "ping" on the connection to
            test if the connection is alive. If not, the connection will
            be reconnected.
        max_reconnects:
            The maximum amount of reconnects, defaults to 3.
        schema:
            If schema is specified,
            then setting a connection to the schema only.
    """

    connector: Connector

    def __init__(
            self,
            pool_size: int = 20,
            pre_ping: bool = False,
            max_reconnects: int = 3,
            schema: Optional[str] = None,
            **kwargs: str
    ) -> None:
        super().__init__(
            Connector(pool_size, pre_ping, max_reconnects, schema, **kwargs))

    def __repr__(self) -> str:
        return repr(self.connector)

    def select(
            self,
            cmd: Union[str, sql.Composed],
            values: Optional[QueryParams] = None
    ) -> pd.DataFrame:
        """Read SQL query into a DataFrame.

        Returns a DataFrame corresponding to the result of the query.

        Args:
            cmd:
                string SQL query to be executed.
            values:
                Parameters to pass to execute method.

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
                Parameters to pass to execute method.
            default:
                If query result is empty, then return the default value.
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
        """Write records stored in a DataFrame to a database table.

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

    def execute_script(
            self,
            path2script: str
    ) -> None:
        """Execute query from file.

        Args:
            path2script:
                Path to the file with the query.
        """

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
        """Implements insert command.

        Inserted values are passed through the keyword arguments.

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
                INSERT INTO command.
            values:
                Collection of values to be inserted.
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
        """Places DataFrame to a buffer and apply COPY FROM command.

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

        Raises:
            CopyError: if execution fails.
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

        Args:
            table_name:
                Name of the table where to insert.
            schema:
                Name of the schema.
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

        Remove all the rows from the DataFrame conflicted with
        primary key constraint.

        Parameter ``where`` is used to reduce the amount of querying data.

        Args:
            table_name:
                Name of the table.
            data:
                DataFrame where the primary key conflicts need to be
                resolved.
            schema:
                Name of the schema.
            where:
                WHERE clause used when querying data from the
                ``table_name``.

        Returns:
            DataFrame without primary key conflicts.
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

        Remove all the rows from the DataFrame conflicted with
        foreign key constraint.

        Parameter ``where`` is used to reduce the amount of querying data.

        Args:
            table_name:
                Name of the child table, where the data needs to be inserted.
            parent_name:
                Name of the parent table.
            data:
                DataFrame with foreign key conflicts.
            schema:
                Name of the child table schema.
            where:
                WHERE clause used when querying from the ``table_name``.
            parent_schema:
                Name of the parent table schema.

        Returns:
            DataFrame without foreign key conflicts.
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
            the columns of the given table.
        """

        return self.select(queries.column_names(table_name, schema))

    def _primary_key(
            self,
            table_name: str,
            schema: str
    ) -> pd.DataFrame:
        """Return names of all the columns included to the primary key.

        Args:
            table_name:
                Name of the table.
            schema:
                Name of the schema.

        Returns:
            Pandas.DataFrame with the names and data types of all
            the columns included to the primary key.
        """

        return self.select(queries.primary_key(table_name, schema))

    def _foreign_key(
            self,
            table_name: str,
            parent_name: str,
            schema: str,
            parent_schema: str
    ) -> pd.DataFrame:
        """Return names of all the columns included to the foreign key.

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
            Pandas.DataFrame with columns included to the foreign key.
        """

        return self.select(queries.foreign_key(
            table_name, schema, parent_name, parent_schema))

    def _format_data(
            self,
            data: pd.DataFrame,
            table_name: str,
            schema: str
    ) -> pd.DataFrame:
        """Formatting DataFrame before applying COPY FROM.
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
