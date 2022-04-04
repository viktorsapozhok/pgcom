__all__ = ["Commuter"]

from io import StringIO
from typing import Any, Dict, List, Mapping, Optional, Sequence, Union

import numpy as np
import pandas as pd
from psycopg2 import sql

from . import exc, queries
from .base import BaseCommuter
from .connector import Connector

QueryParams = Union[Sequence[Any], Mapping[str, Any]]


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
    """

    connector: Connector

    def __init__(
        self,
        pool_size: int = 20,
        pre_ping: bool = False,
        max_reconnects: int = 3,
        **kwargs: str,
    ) -> None:
        super().__init__(Connector(pool_size, pre_ping, max_reconnects, **kwargs))

    def __repr__(self) -> str:
        return repr(self.connector)

    def select(
        self, cmd: Union[str, sql.Composed], values: Optional[QueryParams] = None
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
        default: Optional[Any] = None,
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

    def execute_script(self, path2script: str) -> None:
        """Execute query from file.

        Args:
            path2script:
                Path to the file with the query.
        """

        with open(path2script, "r") as fh:
            cmd = fh.read()

        self._execute(cmd=cmd)

    def insert(
        self,
        table_name: str,
        data: pd.DataFrame,
        columns: Optional[List[str]] = None,
        placeholders: Optional[List[str]] = None,
    ) -> None:
        """Write rows from a DataFrame to a database table.

        Args:
            table_name:
                Name of the destination table.
            data:
                Pandas.DataFrame with the data to be inserted.
            columns:
                List of column names used for insert. If not specified
                then all the columns are used. Defaults to None.
            placeholders:
                List of placeholders. If not specified then the default
                placeholders are used. Defaults to None.

        Examples:

            .. code::

                >>> self.insert("people", data)

            Insert two columns, name and age.

            .. code::

                >>> self.insert("people", data, columns=["name", "age"])

            You can customize placeholders to implement advanced insert,
            e.g. to insert geometry data in a database with PostGIS extension.

            .. code::

                >>> self.insert(
                ...     table_name="polygons",
                ...     data=data,
                ...     columns=["name", "geom"],
                ...     placeholders=["%s", "ST_GeomFromText(%s, 4326)"])
        """

        if columns is None:
            columns = list(data.columns)

        if placeholders is None:
            placeholders = sql.Placeholder() * len(columns)
        else:
            placeholders = sql.Composed([sql.SQL(p) for p in placeholders])

        cmd = sql.SQL("INSERT INTO {} ({}) VALUES ({})").format(
            sql.SQL(table_name),
            sql.SQL(", ").join(map(sql.Identifier, columns)),
            sql.SQL(", ").join(placeholders),
        )

        rows = data[columns].to_numpy(na_value=None)

        for values in [tuple(row) for row in rows]:
            self._execute(cmd=cmd, values=values)

    def insert_row(
        self, table_name: str, return_id: Optional[str] = None, **kwargs: Any
    ) -> Optional[int]:
        """Implements insert command.

        Inserted values are passed through the keyword arguments.

        Args:
            table_name:
                Name of the destination table.
            return_id:
                Name of the returned serial key.
        """

        sid = None
        keys = list(kwargs.keys())

        cmd = sql.SQL("INSERT INTO {} ({}) VALUES ({})").format(
            sql.SQL(table_name),
            sql.SQL(", ").join(map(sql.Identifier, keys)),
            sql.SQL(", ").join(map(sql.Placeholder, keys)),
        )

        if return_id is not None:
            sid = self.insert_return(cmd, return_id=return_id, values=kwargs)
        else:
            self._execute(cmd=cmd, values=kwargs)

        return sid

    def insert_return(
        self,
        cmd: Union[str, sql.Composed],
        values: Optional[QueryParams] = None,
        return_id: Optional[str] = None,
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
            cmd = sql.Composed(
                [
                    sql.SQL(cmd) if isinstance(cmd, str) else cmd,
                    sql.SQL(" RETURNING {}").format(sql.Identifier(return_id)),
                ]
            )

        fetched, _ = self._execute(cmd, values)

        try:
            sid = fetched[0][0]
        except IndexError:
            sid = 0

        return sid

    def copy_from(
        self,
        table_name: str,
        data: pd.DataFrame,
        format_data: bool = False,
        sep: str = ",",
        na_value: str = "",
        where: Optional[Union[str, sql.Composed]] = None,
    ) -> None:
        """Places DataFrame to a buffer and apply COPY FROM command.

        Args:
            table_name:
                Name of the table where to insert.
            data
                DataFrame from where to insert.
            format_data:
                Reorder columns and adjust dtypes wrt to table metadata
                from information_schema.
            sep:
                String of length 1. Field delimiter for the output file.
                Defaults to ",".
            na_value:
                Missing data representation, defaults to "".
            where:
                WHERE clause used to specify a condition while deleting
                data from the table before applying copy_from,
                DELETE command is not executed if not specified.

        Raises:
            CopyError: if execution fails.
        """

        df = data.copy()

        if format_data:
            df = self._format_data(df, table_name, sep=sep)

        with self.connector.open_connection() as conn:
            try:
                with conn.cursor() as cur:
                    if where is not None:
                        if isinstance(where, str):
                            where = sql.SQL(where)
                        cmd = sql.Composed(
                            [
                                sql.SQL("DELETE FROM {} WHERE ").format(
                                    sql.SQL(table_name)
                                ),
                                where,
                            ]
                        )
                        cur.execute(cmd)
                    # DataFrame to buffer
                    s_buf = StringIO()
                    df.to_csv(
                        path_or_buf=s_buf,
                        sep=sep,
                        na_rep=na_value,
                        index=False,
                        header=False,
                    )
                    s_buf.seek(0)
                    # copy from buffer
                    columns = ", ".join(df.columns)
                    cmd = (
                        f"COPY {table_name} ({columns}) FROM STDOUT "
                        f"DELIMITER '{sep}' NULL '{na_value}'"
                    )
                    cur.copy_expert(cmd, s_buf)
                conn.commit()
            except Exception as e:
                try:
                    conn.rollback()
                except Exception as ex:
                    exc.raise_with_traceback(
                        exc.CopyError(f"{ex}\n unable to rollback")
                    )

                exc.raise_with_traceback(exc.CopyError(f"{e}\n"))

    def is_table_exist(self, table_name: str) -> bool:
        """Return True if table exists, otherwise False.

        Args:
            table_name:
                Name of the table where to insert.
        """

        _schema, _table_name = self._get_schema(table_name)
        df = self.select(queries.is_table_exist(_table_name, _schema))
        return bool(len(df) > 0)

    def is_entry_exist(self, table_name: str, **kwargs: Any) -> bool:
        """Return True if entry already exists, otherwise return False.

        Implements a simple query to verify if a specific entry exists in
        the table. WHERE clause is created from ``**kwargs``.

        Args:
            table_name:
                Name of the database table.
            **kwargs:
                Parameters to create WHERE clause.

        Examples:
            Implement query ``SELECT 1 FROM people WHERE id=5 AND num=100``.

            .. code::

                >>> self.is_entry_exist("my_table", id=5, num=100)
                True
        """

        cmd = sql.SQL("SELECT 1 FROM {} WHERE {}").format(
            sql.SQL(table_name), self.make_where(list(kwargs.keys()))
        )

        res = self.select_one(cmd=cmd, values=kwargs, default=None)
        return res is not None

    def delete_entry(self, table_name: str, **kwargs: Any) -> None:
        """Delete entry from the table.

        Implements a simple query to delete a specific entry from the table.
        WHERE clause is created from ``**kwargs``.

        Args:
            table_name:
                Name of the database table.
            **kwargs:
                Parameters to create WHERE clause.

        Examples:
            Delete rows with version=100 from the table.

            .. code::

                >>> self.delete_entry("dict_versions", version=100)
        """

        cmd = sql.SQL("DELETE FROM {} WHERE {}").format(
            sql.SQL(table_name), self.make_where(list(kwargs.keys()))
        )

        self._execute(cmd, values=kwargs)

    @staticmethod
    def make_where(keys: List[str]) -> sql.Composed:
        """Build WHERE clause from list of keys.

        Examples:

            .. code::

                >>> self.make_where(["version", "task"])
                "version=%s AND task=%s"
        """

        where = list()  # type: List[Union[sql.Composable]]
        for key in keys:
            if len(where) > 0:
                where += [sql.SQL(" AND ")]
            where += [sql.Identifier(key), sql.SQL("="), sql.Placeholder(key)]
        return sql.Composed(where)

    def get_connections_count(self) -> int:
        """Returns the amount of active connections."""

        return self.select_one(cmd=queries.conn_count(), default=0)

    def resolve_primary_conflicts(
        self,
        table_name: str,
        data: pd.DataFrame,
        where: Optional[Union[str, sql.Composed]] = None,
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
            where:
                WHERE clause used when querying data from the
                ``table_name``.

        Returns:
            DataFrame without primary key conflicts.
        """

        p_key = self.select(queries.primary_key(table_name))
        p_key = p_key["column_name"].to_list()

        df = data.copy()

        if len(p_key) > 0:
            if where is not None:
                if isinstance(where, str):
                    where = sql.SQL(where)
                cmd = sql.Composed(
                    [
                        sql.SQL("SELECT * FROM {} WHERE ").format(sql.SQL(table_name)),
                        where,
                    ]
                )
            else:
                cmd = sql.SQL("SELECT * FROM {}").format(sql.SQL(table_name))

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

    def resolve_foreign_conflicts(
        self,
        table_name: str,
        parent_name: str,
        data: pd.DataFrame,
        where: Optional[Union[str, sql.Composed]] = None,
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
            where:
                WHERE clause used when querying from the ``table_name``.

        Returns:
            DataFrame without foreign key conflicts.
        """

        df = data.copy()

        _schema, _table_name = self._get_schema(table_name)
        _parent_schema, _parent_name = self._get_schema(parent_name)

        foreign_key = self.select(
            queries.foreign_key(_table_name, _schema, _parent_name, _parent_schema)
        )

        if len(foreign_key) > 0:
            if where is not None:
                if isinstance(where, str):
                    where = sql.SQL(where)
                cmd = sql.Composed(
                    [
                        sql.SQL("SELECT * FROM {} WHERE ").format(sql.SQL(parent_name)),
                        where,
                    ]
                )
            else:
                cmd = sql.SQL("SELECT * FROM {}").format(sql.SQL(parent_name))

            parent_data = self.select(cmd)

            if not parent_data.empty:
                df.set_index(foreign_key["child_column"].to_list(), inplace=True)
                parent_data.set_index(
                    foreign_key["parent_column"].to_list(), inplace=True
                )
                # remove rows which are not in parent index
                df = df[df.index.isin(parent_data.index)]
                # reset index and sort columns
                df = df.reset_index(level=foreign_key["child_column"].to_list())
                df = df[data.columns]
            else:
                df = pd.DataFrame()
        return df

    def encode_category(
        self,
        data: pd.DataFrame,
        category: str,
        key: str,
        category_table: str,
        category_name: Optional[str] = None,
        key_name: Optional[str] = None,
        na_value: Optional[str] = None,
    ) -> pd.DataFrame:
        """Encode categorical column.

        Implements writing of all the unique values in categorical column
        given by ``category_name`` to the table given by ``category_table``.

        Replaces all the values in ``category`` column in the original
        DataFrame with the corresponding integer values assigned to categories
        via serial primary key constraint.

        Args:
            data:
                Pandas.DataFrame with categorical column.
            category:
                Name of the categorical column in DataFrame
                the method is applied for.
            key:
                Name of the DataFrame column with encoded values.
            category_table:
                Name of the table with stored categories.
            category_name:
                Name of the categorical column in ``category_table``.
                Defaults to ``category``.
            key_name:
                Name of the column in ``category_table`` contained
                the encoded values. Defaults to ``key``.
            na_value:
                Missing data representation.

        Returns:
            Pandas.DataFrame with encoded category.
        """

        if category_name is None:
            category_name = category
        if key_name is None:
            key_name = key
        if na_value is not None:
            data[category] = data[category].fillna(na_value)

        data[category] = data[category].str.replace(",", "")
        cat = data[[category]].drop_duplicates()
        cat.rename(columns={category: category_name}, inplace=True)

        table_data = self.select(
            sql.SQL("SELECT DISTINCT {} FROM {}").format(
                sql.SQL(category_name), sql.SQL(category_table)
            )
        )

        if not table_data.empty:
            cat = cat[~cat[category_name].isin(table_data[category_name].tolist())]

        if len(cat) > 0:
            self.copy_from(category_table, cat, format_data=True)

        cmd = sql.SQL("SELECT {} AS {}, {} AS {} FROM {}").format(
            sql.Identifier(key_name),
            sql.Identifier(key),
            sql.Identifier(category_name),
            sql.Identifier(category),
            sql.SQL(category_table),
        )

        df = self.select(cmd)
        data[key] = data[category].map(df.set_index(category)[key].to_dict())
        return data

    def encode_composite_category(
        self,
        data: pd.DataFrame,
        categories: Dict[str, str],
        key: str,
        category_table: str,
        key_name: Optional[str] = None,
        na_value: Optional[str] = None,
    ) -> pd.DataFrame:
        """Encode categories represented by multiple columns.

        Implements writing of all the unique combinations given by multiple
        columns in DataFrame to the table given by ``category_table``.

        Dictionary ``categories`` provides a mapping between DataFrame and
        ``category_table`` column names.

        Args:
            data:
                Pandas.DataFrame with categorical columns.
            categories:
                Dictionary provided the mapping between column names. Dict keys
                provide names of columns in ``data`` represented category,
                values represent column names in ``category_table``.
            key:
                Name of the DataFrame column with encoded values.
            category_table:
                Name of the table with stored categories.
            key_name:
                Name of the column in ``category_table`` contained
                the encoded values. Defaults to ``key``.
            na_value:
                Missing data representation.

        Returns:
            Pandas.DataFrame with encoded category.
        """

        if key_name is None:
            key_name = key

        for category in categories.keys():
            if na_value is not None:
                data[category] = data[category].fillna(na_value)

            if data[category].dtype == object and isinstance(
                data[category].iloc[0], str
            ):
                data[category] = data[category].str.replace(",", "")

        cat = data.drop_duplicates(subset=list(categories.keys()))
        cat.rename(columns=categories, inplace=True)

        table_data = self.select(
            sql.SQL("SELECT * FROM {}").format(sql.SQL(category_table))
        )

        composite_key = list(categories.values())

        if not table_data.empty:
            cat.set_index(composite_key, inplace=True)
            table_data.set_index(composite_key, inplace=True)
            cat = cat[~cat.index.isin(table_data.index)]
            cat.reset_index(level=composite_key, inplace=True)

        if len(cat) > 0:
            self.copy_from(category_table, cat, format_data=True)

        df = self.select(sql.SQL("SELECT * FROM {}").format(sql.SQL(category_table)))

        df.rename(columns={v: k for k, v in categories.items()}, inplace=True)
        df.rename(columns={key_name: key}, inplace=True)
        columns = list(categories.keys()) + [key]
        data = data.merge(df[columns], how="inner", on=list(categories.keys()))

        return data

    def _table_columns(self, table_name: str) -> pd.DataFrame:
        """Return columns attributes of the given table.

        Args:
            table_name:
                Name of the table.

        Returns:
            Pandas.DataFrame with the names and data types of all
            the columns of the given table.
        """

        _schema, _table_name = self._get_schema(table_name)
        return self.select(queries.column_names(_table_name, _schema))

    def _format_data(
        self, data: pd.DataFrame, table_name: str, sep: str = ","
    ) -> pd.DataFrame:
        """Formatting DataFrame before applying COPY FROM."""

        table_columns = self._table_columns(table_name)
        columns = []  # type: List[str]

        for row in table_columns.itertuples():
            column = row.column_name

            if column in data.columns:
                columns += [column]

                if row.data_type in ["smallint", "integer", "bigint"]:
                    if data[column].dtype == np.float64:
                        data[column] = data[column].round().astype("Int64")
                elif row.data_type in ["text"]:
                    try:
                        data[column] = data[column].str.replace(sep, "")
                    except AttributeError:
                        continue
        return data[columns]
