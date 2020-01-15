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
    commuter.execute(f"""
        CREATE TABLE IF NOT EXISTS people (
            name text,
            age integer)
        """)

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

    >>> cmd = f"""
    ... CREATE TABLE people (
    ...     num SERIAL PRIMARY KEY,
    ...     name text,
    ...     age integer)"""
    >>> commuter.execute(cmd)
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
