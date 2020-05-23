Welcome to pgcom's documentation!
=================================

Communication manager for PostgreSQL database, provides a collection of wrappers over
Psycopg adapter to simplify the usage of basic SQL operators.

Key Features
------------

-  Executing of the database operations, reading query into a DataFrame, inserting records from DataFrame to the table.
-  Using `COPY FROM` for efficient writing to the tables.
-  Methods to resolve primary and foreign key conflicts before applying `COPY_FROM`.
-  Methods to resolve data format conflicts before applying `COPY_FROM`.
-  Tools for setting asynchronous communication with database using `LISTEN` and `NOTIFY` commands.

.. toctree::
   :maxdepth: 1

   installation
   overview
   reference/index
   changelog

MIT License (see `LICENSE <https://github.com/viktorsapozhok/pgcom/blob/master/LICENSE>`_).
