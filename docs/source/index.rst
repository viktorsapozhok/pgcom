Welcome to pgcom's documentation!
=================================

Communication manager for PostgreSQL database, provides a collection of convenience
wrappers over Psycopg adapter to simplify the usage of basic Psycopg methods in conjunction
with Pandas DataFrames.

Key Features
------------

-  Querying from database to Pandas DataFrame
-  Writing from DataFrame to a database tables.
-  Adaptation between DataFrames and COPY FROM.
-  Methods to resolve conflicts in DataFrame before applying COPY FROM.
-  Tools for setting asynchronous communication with database using LISTEN and NOTIFY commands.

.. toctree::
   :maxdepth: 1

   installation
   tutorial
   reference/index
   changelog

MIT License (see `LICENSE <https://github.com/viktorsapozhok/pgcom/blob/master/LICENSE>`_).
