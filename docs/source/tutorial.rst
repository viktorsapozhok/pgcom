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



When table has a constraint and the DataFrame contains rows conflicted
with this constraint, the data cannot be added to the table
with the :func:`~pgcom.commuter.Commuter.copy_from`. It is still possible to
insert the data with the :func:`~pgcom.commuter.Commuter.execute` method,
using for example ``INSERT ON CONFLICT`` statement
(`see here for details <https://www.postgresqltutorial.com/postgresql-upsert/>`_).

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

Resolve primary conflicts
-------------------------

In the last example, we deleted rows from the table before using
:func:`~pgcom.commuter.Commuter.copy_from`. In contrast to it,
the :func:`~pgcom.commuter.Commuter.resolve_primary_conflicts` method can be used
to control the data integrity and, instead of removing rows from the table,
remove it from the DataFrame.

.. code-block:: python

    df = commuter.resolve_primary_conflicts(
        table_name='table_name',
        data=df,
        where='condition to reduce search complexity')

To implement it, the method selects data from the table and removes all
rows from the given DataFrame, which violate primary key constraint
in the selected data. To reduce the amount of querying data (when table is large),
you need to specify ``where`` argument. It specifies ``WHERE`` clause in
the ``SELECT`` query.

.. code-block:: bash

    >>> commuter.execute(f"""CREATE TABLE people (
    ...     id integer PRIMARY KEY, name text, age integer)""")
    >>>
    >>> df = pd.DataFrame({
    ...     'id': [1,2,3,4,5],
    ...     'name': ['Brezhnev', 'Andropov', 'Chernenko', 'Gorbachev', 'Yeltsin'],
    ...     'age': [75, 69, 73, 89, 76]})
    >>>
    >>> commuter.copy_from('people', df)
    >>> print(df)
       id       name  age
    0   1   Brezhnev   75
    1   2   Andropov   69
    2   3  Chernenko   73
    3   4  Gorbachev   89
    4   5    Yeltsin   76

Assume now, that we need to add new rows to the table.

.. code-block:: bash

    >>> new_data = pd.DataFrame({
    ...     'id': [6,3],
    ...     'name': ['Khrushchev', 'Putin'],
    ...     'age': [77, 67]})
    >>> print(new_data)
       id        name  age
    0   6  Khrushchev   77
    1   3       Putin   67

We apply :func:`~pgcom.commuter.Commuter.resolve_primary_conflicts` to sanitize
the new data before copying and specify ``where`` argument to compare the new
entries only across the people older than 60 (to reduce the complexity).

.. code-block:: bash

    >>> new_data = commuter.resolve_primary_conflicts(
    ...     table_name='people',
    ...     data=new_data,
    ...     where='age > 60')
    >>> print(new_data)
       id        name  age
    0   6  Khrushchev   77

Rows with conflicted keys have been deleted and
:func:`~pgcom.commuter.Commuter.copy_from` can be now used without a doubt.

Resolve foreign conflicts
-------------------------

To sanitize the DataFrame for the case of potential conflicts on the foreign key,
use :func:`~pgcom.commuter.Commuter.resolve_foreign_conflicts`. It selects data
from the ``parent_table`` and removes all rows from the given DataFrame,
which violate foreign key constraint in the selected data.

.. code-block:: python

    df = commuter.resolve_foreign_conflicts(
        table_name='table_name',
        parent_name='parent_table_name',
        data=df,
        where='condition to reduce the selected data')

Let's say, we have table named ``authors`` that stores meta-information about writers,
and table ``novels`` with a foreign key constraint that references to ``authors`` table.

.. code-block:: python

    commuter.execute(f"""CREATE TABLE authors (
        id SERIAL PRIMARY KEY,
        name VARCHAR (255),
        born INTEGER,
        died INTEGER);""")

    commuter.execute(f"""CREATE TABLE novels (
        novel_id SERIAL PRIMARY KEY,
        author_id INTEGER REFERENCES authors(id),
        author_name VARCHAR (255),
        novel VARCHAR (255));""")

Assume now, that we have added some data to ``authors``.

.. code-block:: bash

    >>> print(commuter.select('select * from authors'))
       id        name  born  died
    0   1     Tolstoy  1828  1910
    1   2  Dostoevsky  1821  1881
    2   3     Chekhov  1860  1904

We get an error, if we try to write from the DataFrame with unresolved foreign
key conflicts.

.. code-block:: bash

    >>> df = pd.DataFrame({
    ...     'author_id': [1, 1, 4],
    ...     'author_name': ['Tolstoy', 'Tolstoy', 'Nabokov'],
    ...     'novel': ['War and Peace', 'Anna Karenina', 'Lolita']})
    >>>
    >>> print(df)
       author_id author_name     novel_name
    0          1     Tolstoy  War and Peace
    1          1     Tolstoy  Anna Karenina
    2          4     Nabokov         Lolita
    >>>
    >>> commuter.copy_from(table_name='novels', data=df, format_data=True)
    pgcom.exc.CopyError: insert or update on table "novels" violates foreign key
    constraint "novels_author_id_fkey"
    DETAIL:  Key (author_id)=(4) is not present in table "authors".

Let's sanitize the DataFrame and try again.

    >>> df = commuter.resolve_foreign_conflicts(
    ...     table_name='novels', parent_name='authors', data=df)
    >>>
    >>> commuter.copy_from(table_name='novels', data=df, format_data=True)
    >>>
    >>> print(commuter.select('select * from novels'))
       novel_id  author_id author_name          novel
    0         1          1     Tolstoy  War and Peace
    1         2          1     Tolstoy  Anna Karenina

Success!




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
