Tutorial
========

Basic Usage
-----------

Here is an interactive session showing the basic commands usage.

.. code-block:: python

    >>> import pandas as pd
    >>> from pgcom import Commuter

    # create commuter
    >>> commuter = Commuter(dbname="test", user="postgres", password="secret", host="localhost")

    # execute a command: this creates a new table
    >>> commuter.execute("CREATE TABLE test (id serial PRIMARY KEY, num integer, data varchar);")

    # write from DataFrame to a table
    >>> df = pd.DataFrame([[100, "abc"], [200, "abc'def"]], columns=["num", "data"])
    >>> commuter.insert(table_name="test", data=df)

    # read from table to a DataFrame
    >>> commuter.select("SELECT * FROM test")
       id  num     data
    0   1  100      abc
    1   2  200  abc'def

    # pass data to fill a query placeholders
    >>> commuter.select("SELECT * FROM test WHERE data = (%s)", ("abc'def",))
       id  num     data
    0   2  200  abc'def

Writing to a table with copy from
----------------------------------

PostgreSQL `COPY FROM <https://www.postgresql.org/docs/current/sql-copy.html>`_
command copies data from a file-system file to a table (appending the data
to whatever is in the table already).

Commuter's :func:`~pgcom.commuter.Commuter.copy_from` method provides an adaptation
between Pandas DataFrame and COPY FROM, the DataFrame format however must be
compatible with database table (data types, columns order, etc).

.. code-block:: python

    >>> commuter.copy_from(table_name="test", data=df)

A slight adaptation of DataFrame to the table structure can be attained by setting
``format_data`` parameter to ``True``. This enables to use DataFrames with the incomplete
set of columns given in any order.

.. code-block:: python

    >>> df = pd.DataFrame([["abc", 100], ["abc'def", 200]], columns=["data", "num_2"])

    # DataFrame has column "num_2" not presented in table
    >>> commuter.copy_from("test", df)
    pgcom.exc.CopyError: column "num_2" of relation "test" does not exist

    >>> commuter.copy_from("test", df, format_data=True)
    >>> commuter.select("SELECT * FROM test")
       id   num     data
    0   1  None      abc
    1   2  None  abc'def

Upsert with copy from
---------------------

If DataFrame contains rows conflicting with table constraints and you need to implement
`UPSERT <https://www.postgresqltutorial.com/postgresql-upsert/>`_, you can specify
``where`` parameter of :func:`~pgcom.commuter.Commuter.copy_from` method.
Then it removes rows from the table before applying COPY FROM.

On the other hand, if you want to sanitize DataFrame and remove conflicts from it rather than from the table,
you can use :func:`~pgcom.commuter.Commuter.resolve_primary_conflicts` and :func:`~pgcom.commuter.Commuter.resolve_foreign_conflicts`.

.. code-block:: python

    >>> commuter.execute("CREATE TABLE test (id integer PRIMARY KEY, num integer, data varchar);")
    >>> df_1 = pd.DataFrame([[1, 100, "a"], [2, 200, "b"]], columns=["id", "num", "data"])
    >>> commuter.copy_from("test", df_1)

    # df_2 has primary key conflict
    >>> df_2 = pd.DataFrame([[2, 201, "bb"], [3, 300, "c"]], columns=["id", "num", "data"])
    >>> commuter.copy_from("test", df_2)
    pgcom.exc.CopyError: duplicate key value violates unique constraint "test_pkey"

    # remove all rows from test table where id >= 2
    >>> commuter.copy_from("test", df_2, where="id >= 2")
    >>> commuter.select("SELECT * FROM test")
       id  num data
    0   1  100    a
    1   2  201   bb
    2   3  300    c

    >>> df_3 = pd.DataFrame([[3, 301, "cc"], [4, 400, "d"]], columns=["id", "num", "data"])

    # remove conflicts from the DataFrame
    >>> commuter.resolve_primary_conflicts("test", df_3)
       id  num data
    0   4  400    d

.. note::

    Be careful when resolving conflicts on DataFrame. Since both methods query data from the table,
    the whole table will be queried if you don't specify ``where`` parameter.

Connection options
------------------

A connection pooling technique is used to maintain a "pool" of active database connections
in memory which are reused across the requests.

Testing the connection for liveness is available by using ``pre_ping`` argument. This feature
will normally emit "SELECT 1" statement on each request to the database. If an error is raised,
the pool will be immediately restarted.

.. code-block:: python

    >>> commuter = Commuter(pre_ping=True, **conn_args)

The exponential backoff strategy is used for reconnection. By default, it implements 3 reconnection attempts.
This can be changed by setting ``max_reconnects`` argument.

.. code-block:: python

    >>> commuter = Commuter(pre_ping=True, max_reconnects=5, **conn_args)

.. note::

    When creating a new instance of :class:`~pgcom.commuter.Commuter`, the connection pool
    is created by calling :func:`~pgcom.commuter.Connector.make_pool` and the connection
    is established. The typical usage of :class:`~pgcom.commuter.Commuter` is therefore once
    per particular database, held globally for the lifetime of a single application process.

.. warning::

    So far a simple connection pool is used and it can't be shared across different threads.

Extras
------

Here is the use cases of other :class:`~pgcom.commuter.Commuter` methods.

Select data from the table and return a scalar value.

.. code-block:: python

    >>> commuter.select_one("SELECT MAX(num) FROM test")
    300

Insert one row to the table passing values using key-value arguments.

.. code-block:: python

    >>> commuter.insert_row("test", id=5, num=500, data="abc'def")

When using a serial column to provide unique identifiers, it can be very handy to
insert one row and return the serial ID assigned to it.

.. code-block:: python

    >>> row_id = commuter.insert_row("test", return_id="id", num=500, data="abc'def")

Check if the table exists.

.. code-block:: python

    >>> commuter.is_table_exist("test")
    True

Return the number of active connections to the database.

.. code-block:: python

    >>> commuter.get_connections_count()
    9

Listener
--------

PostgreSQL provides tools for setting asynchronous interaction with database session using
`LISTEN <https://www.postgresql.org/docs/current/sql-listen.html>`_ and
`NOTIFY <https://www.postgresql.org/docs/current/sql-notify.html>`_ commands.

A client application registers as a listener on the notification channel with the LISTEN command
(and can stop listening with the UNLISTEN command). When the command NOTIFY is executed, the application
listening on the channel is notified. A payload can be passed to provide some extra data to the listener.
This is commonly used when sending notifications that table rows have been modified.

Here is the example of simple application receiving notification when rows are inserted to the table.
See `API reference <https://pgcom.readthedocs.io/en/latest/reference/listener.html>`_ for more details

.. code-block:: python

    from pgcom import Listener

    >>> listener = Listener(dbname="test", user="postgres", password="secret", host="localhost")

    # create a function called by trigger, it generates a notification
    # which is sending to test_channel
    >>> listener.create_notify_function(func_name='notify_trigger', channel='test_channel')

    # create a trigger executed AFTER INSERT STATEMENT
    >>> listener.create_trigger(table_name='test', func_name='notify_trigger')

    # register function callback activated on the notification
    >>> def on_notify(payload):
    ...     print("received notification")

    # listening loop, callback is activated on every INSERT to "test" table
    >>> listener.poll(channel='test_channel', on_notify=on_notify)
    received notification
    received notification

.. note::

    Note that the payload is only available from PostgreSQL 9.0: notifications received
    from a previous version server will have the payload attribute set to the empty string.