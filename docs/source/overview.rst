Overview
========

Basic Usage
-----------

To initialize a new commuter, you need to set the basic connection parameters:

.. code-block:: python

    conn_params = {
        'host': 'localhost',
        'port': '5432',
        'user': 'postgres',
        'password': 'password',
        'db_name': 'test_db'}

and create a new commuter instance:

.. code-block:: python

    from pgcom import Commuter
    commuter = Commuter(**conn_params)

Any other connection parameter can be passed as a keyword.
The list of the supported parameters
`can be seen here <https://www.postgresql.org/docs/current/libpq-connect.html#LIBPQ-PARAMKEYWORDS>`_.

Basic operations are provided with :func:`~pgcom.commuter.Commuter.execute` and
:func:`~pgcom.commuter.Commuter.select`, :func:`~pgcom.commuter.Commuter.insert` methods.

Execute a database operation (query or command):

.. code-block:: python

    # create table
    commuter.execute(f"""CREATE TABLE IF NOT EXISTS people (
        name text,
        age integer)""")

    # insert to table
    commuter.execute(
        cmd='INSERT INTO people VALUES (%s, %s)',
        vars=('Yeltsin', 76))

    # delete table
    commuter.execute('DROP TABLE people')

Use :func:`~pgcom.commuter.Commuter.select` for reading SQL query into a DataFrame.
This method returns a DataFrame corresponding to the result set of the query string.

.. code-block:: python

    df = commuter.select(f'SELECT * FROM people WHERE age > 55')

To write records stored in a DataFrame to database, you can use
:func:`~pgcom.commuter.Commuter.insert` method.

.. code-block:: python

    import pandas as pd

    df = pd.DataFrame({
        'name': ['Gorbachev', 'Yeltsin'],
        'age': [89, 76]})

    commuter.insert(table_name='people', data=df)

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

Insert with COPY FROM
---------------------

PostgreSQL ``COPY FROM`` command copies data from a file-system file to a table
(appending the data to whatever is in the table already).

Currently no adaptation is provided between Python and PostgreSQL types on COPY:
the file can be any Python file-like object but its format must be in the format
accepted by PostgreSQL COPY command (data format, escaped characters, etc).

The :func:`~pgcom.commuter.Commuter.copy_from` method adapts an interface to
efficient PostgreSQL ``COPY FROM`` command provided by Psycopg ``cursor`` objects
to support writing data stored in a DataFrame.

To see a difference, let's try to insert data from the DataFrame with 1M rows
and two columns using just a basic :func:`~pgcom.commuter.Commuter.insert` method.

.. code-block:: bash

    >>> from time import time
    >>> import pandas as pd
    >>>
    >>> df = pd.DataFrame({
    ...     'name': ['Yeltsin'] * int(1e6),
    ...     'age': [76] * int(1e6)})
    >>>
    >>> start = time()
    >>> commuter.insert(table_name='people', data=df)
    >>> print(f'processing time: {time() - start:.1f} sec')
    processing time: 22.1 sec

Now implementing the same operation with :func:`~pgcom.commuter.Commuter.copy_from`.

.. code-block:: bash

    >>> start = time()
    >>> commuter.copy_from(table_name='people', data=df)
    >>> print(f'processing time: {time() - start:.1f} sec')
    processing time: 1.3 sec

Set the ``format_data`` argument as ``True``, if you need to adjust data before applying
:func:`~pgcom.commuter.Commuter.copy_from`. It will control columns order according
the table information stored in database information schema and
converts float types to integer if needed.

.. code-block:: bash

    >>> df = pd.DataFrame({'age': [76.0], 'name': ['Yeltsin']})
    >>> commuter.copy_from('people', df)
    pgcom.exc.CopyError: invalid input syntax for type integer: "Yeltsin"

Without formatting we caught an error trying to insert a text data into the first table
column, which has an integer type. Now set ``format_data`` as ``True`` and repeat the operation.

.. code-block:: bash

    >>> commuter.copy_from('people', df, format_data=True)
    >>> n_obs = commuter.select_one('SELECT COUNT(*) FROM people')
    >>> print(n_obs)
    1

When table has a constraint and the DataFrame contains rows conflicted
with this constraint, the data cannot be added to the table
with the :func:`~pgcom.commuter.Commuter.copy_from`. It is still possible to
insert the data with the :func:`~pgcom.commuter.Commuter.execute` method,
using for example ``INSERT ON CONFLICT`` statement
([see here for details](https://www.postgresqltutorial.com/postgresql-upsert/)).

Let's create a table with the primary key and insert one row.

.. code-block:: bash

    >>> commuter.execute(f"""CREATE TABLE people (
    ...     name text PRIMARY KEY,
    ...     age integer)""")
    >>>
    >>> commuter.insert_row('people', name='Yeltsin', age=76)

Now, if we try to insert the same row we will catch an error.

.. code-block:: bash

    >>> commuter.copy_from('people', df, format_data=True)
    pgcom.exc.CopyError: duplicate key value violates unique constraint "people_pkey"
    DETAIL:  Key (name)=(Yeltsin) already exists.

Using ``where`` argument, we can specify the ``WHERE`` clause of the ``DELETE`` statement,
which will be executed before calling ``COPY FROM``. This means that all rows, where
age is equal to 76, will be deleted from the table and then ``COPY FROM`` command
will be called.

.. code-block:: bash

    >>> commuter.copy_from('people', df, format_data=True, where='age=76')
    >>> n_obs = commuter.select_one('SELECT COUNT(*) FROM people')
    >>> print(n_obs)
    1

