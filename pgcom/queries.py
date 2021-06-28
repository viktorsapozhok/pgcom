__all__ = ["primary_key", "foreign_key", "is_table_exist", "column_names"]


def primary_key(table_name: str) -> str:
    """Return column names of the primary key."""

    return f"""
    SELECT
        a.attname AS column_name,
        format_type(a.atttypid, a.atttypmod) AS data_type
    FROM
        pg_index i
    JOIN
        pg_attribute a
    ON
        a.attrelid = i.indrelid AND
        a.attnum = ANY(i.indkey)
    WHERE
        i.indrelid = '{table_name}'::regclass AND
        i.indisprimary
    """


def foreign_key(
    table_name: str, schema: str, parent_name: str, parent_schema: str
) -> str:
    """Return column names (child and parent) of the foreign key."""

    return f"""
    SELECT
        att2.attname as child_column,
        att.attname as parent_column
    FROM
       (SELECT
            unnest(con1.conkey) AS parent,
            unnest(con1.confkey) AS child,
            con1.confrelid,
            con1.conrelid,
            con1.conname,
            ns2.nspname
        FROM
            pg_class cl
            JOIN pg_namespace ns ON cl.relnamespace = ns.oid
            JOIN pg_constraint con1 ON con1.conrelid = cl.oid
            JOIN pg_class cl2 on cl2.oid = con1.confrelid
            JOIN pg_namespace ns2 on ns2.oid = cl2.relnamespace
        WHERE
            cl.relname = '{table_name}' AND
            ns.nspname = '{schema}' AND
            cl2.relname = '{parent_name}' AND
            ns2.nspname = '{parent_schema}' AND
            con1.contype = 'f'
       ) con
       JOIN pg_attribute att ON
            att.attrelid = con.confrelid AND att.attnum = con.child
       JOIN pg_class cl ON
           cl.oid = con.confrelid
       JOIN pg_attribute att2 ON
           att2.attrelid = con.conrelid AND att2.attnum = con.parent
    """


def is_table_exist(table_name: str, schema: str) -> str:
    """Return table name if it exists in database."""

    return f"""
    SELECT
        table_name
    FROM
        information_schema.tables
    WHERE
        table_name = '{table_name}' AND
        table_schema='{schema}'
    LIMIT 1
    """


def column_names(table_name: str, schema: str) -> str:
    """Return column names of the given table."""

    return f"""
    SELECT
        column_name, data_type
    FROM
        information_schema.columns
    WHERE
        table_schema = '{schema}' AND
        table_name = '{table_name}'
    ORDER BY
        ordinal_position;
    """


def conn_count() -> str:
    """Return amount of active connections to database."""

    return "SELECT SUM(numbackends) FROM pg_stat_database"
