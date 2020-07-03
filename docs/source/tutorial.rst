Tutorial
========

Basic Usage
-----------

Here is an interactive session showing the basic commands usage.

.. code-block:: python

    >>> import pandas as pd
    >>> from pgcom import Commuter

    # create commuter
    >>> commuter = Commuter(dbname='test', user='postgres', password='secret', host='localhost')

    # execute a command: this creates a new table
    >>> commuter.execute("CREATE TABLE test (id serial PRIMARY KEY, num integer, data varchar);")

    # write from DataFrame to a table
    >>> df = pd.DataFrame([[100, "abc"], [200, "abc'def"]], columns=['num', 'data'])
    >>> commuter.insert(table_name='test', data=df)

    # read from table to a DataFrame
    >>> commuter.select("SELECT * FROM test")
       id  num     data
    0   1  100      abc
    1   2  200  abc'def

    # pass data to fill a query placeholders
    >>> commuter.select("SELECT * FROM test WHERE data = (%s)", ("abc'def",))
       id  num     data
    0   2  200  abc'def

Writing to a table with COPY FROM
----------------------------------

PostgreSQL `COPY FROM <https://www.postgresql.org/docs/current/sql-copy.html>`_
command copies data from a file-system file to a table (appending the data
to whatever is in the table already).

Commuter's :func:`~pgcom.commuter.Commuter.copy_from` method provides an adaptation
between Pandas DataFrame and COPY FROM, the DataFrame format however must be
compatible with database table (data types, columns order, etc).

.. code-block:: python

    >>> commuter.copy_from(table_name='test', data=df)

A slight adaptation of DataFrame to the table structure can be attained by setting
``format_data`` parameter to ``True``. This enables to use DataFrames with the incomplete
set of columns given in any order.

.. code-block:: python

    >>> df = pd.DataFrame([["abc", 100], ["abc'def", 200]], columns=['data', 'num_2'])

    # DataFrame has column "num_2" not presented in table
    >>> commuter.copy_from('test', df)
    pgcom.exc.CopyError: column "num_2" of relation "test" does not exist

    >>> commuter.copy_from('test', df, format_data=True)
    >>> commuter.select('SELECT * FROM test')
       id   num     data
    0   1  None      abc
    1   2  None  abc'def

UPSERT with COPY FROM
---------------------

If DataFrame contains rows conflicting with table constraints and you need to implement
`UPSERT <https://www.postgresqltutorial.com/postgresql-upsert/>`_, you can specify
``where`` parameter of :func:`~pgcom.commuter.Commuter.copy_from` method.
Then it removes rows from the table before applying COPY FROM.

On the other hand, if you want to sanitize DataFrame and remove conflicts from it (rather than from the table),
you can use either :func:`~pgcom.commuter.Commuter.resolve_primary_conflicts` or :func:`~pgcom.commuter.Commuter.resolve_foreign_conflicts`.

.. code-block:: python

    >>> commuter.execute("CREATE TABLE test (id integer PRIMARY KEY, num integer, data varchar);")
    >>> df_1 = pd.DataFrame([[1, 100, "a"], [2, 200, "b"]], columns=['id', 'num', 'data'])
    >>> commuter.copy_from("test", df_1)

    # df_2 has primary key conflict
    >>> df_2 = pd.DataFrame([[2, 201, "bb"], [3, 300, "c"]], columns=['id', 'num', 'data'])
    >>> commuter.copy_from("test", df_2)
    pgcom.exc.CopyError: duplicate key value violates unique constraint "test_pkey"

    # remove all rows from test table where id >= 2
    >>> commuter.copy_from('test', df_2, where="id >= 2")
    >>> commuter.select("SELECT * FROM test")
       id  num data
    0   1  100    a
    1   2  201   bb
    2   3  300    c

    >>> df_3 = pd.DataFrame([[3, 301, "cc"], [4, 400, "d"]], columns=['id', 'num', 'data'])

    # remove conflicts from the DataFrame
    >>> commuter.resolve_primary_conflicts('test', df_3)
       id  num data
    0   4  400    d

.. note::

    Be careful when resolving conflicts on DataFrame. Since both methods query data from the table,
    if you don't specify ``where`` parameter, the whole table will be queried.

Schema
------

To specify schema, you have two different options. You can either specify the
``schema`` attribute in the constructor, or just pass it directly to the method.

When you create a new :class:`~pgcom.commuter.Commuter` instance with specified
schema, all the methods will use this schema if other is not specified
by the method parameter. By default the public schema is used.

.. code-block:: bash

    >>> print(Commuter(**conn_params))
    (host=localhost, user=postgres, db_name=test_db, schema=public)
    >>>
    >>> print(Commuter(schema='model', **conn_params))
    (host=localhost, user=postgres, db_name=test_db, schema=model)

If you omit setting schema using class constructor and prefer rather pass it
to the methods, you can use any of the following options:

.. code-block:: python

    commuter = Commuter(**conn_params)  # public schema is used by default

    # specify schema in SQL string, if method doesn't present schema argument
    df = commuter.select('SELECT * FROM model.people WHERE age > 55')

    # if method contains `schema` and `table_name` argument
    commuter.insert(table_name='model.people', data=df)

    # or
    commuter.insert(table_name='people', data=df, schema='model')

Select one element
------------------

Use :func:`~pgcom.commuter.Commuter.select_one` method when your query results in a single element.
This method returns a scalar value, not a DataFrame. Specify the ``default``
argument, if you need the default value to be returned in case the query result
is empty, otherwise ``None`` will be returned.

.. code-block:: python

    n_obs = commuter.select_one(
        cmd='SELECT COUNT(*) FROM people WHERE age > 55',
        default=0)

Insert one row and return serial key
------------------------------------

When using a ``SERIAL`` column to provide unique identifiers, you may need to
return the ID assigned to a new row. To obtain this, :func:`~pgcom.commuter.Commuter.insert_return` or
:func:`~pgcom.commuter.Commuter.insert_row` method can be used.

If you use :func:`~pgcom.commuter.Commuter.insert_row` then you need to pass
values using ``kwargs``:

.. code-block:: bash

    >>> commuter.execute(f"""CREATE TABLE people (
    ...     num SERIAL PRIMARY KEY,
    ...     name text,
    ...     age integer)""")
    >>>
    >>> num = commuter.insert_row(
    ...     table_name='people',
    ...     name='Yeltsin',
    ...     age=76,
    ...     return_id='num')
    >>>
    >>> print(num)
    1

Using :func:`~pgcom.commuter.Commuter.insert_return`, you need to specify SQL string:

.. code-block:: bash

    >>> num = commuter.insert_return(
    ...     cmd='INSERT INTO people (name, age) VALUES (%s, %s)',
    ...     values=('Yeltsin', 76),
    ...     return_id='num')
    >>>
    >>> print(num)
    2
