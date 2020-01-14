Overview
========

Installation
------------

To install the package, simply use pip::

    pip install pgcom

Basic Usage
-----------

To initialize a new commuter, you need to set the basic connection parameters::

    conn_params = {
        'host': 'localhost',
        'port': '5432',
        'user': 'postgres',
        'password': 'password',
        'db_name': 'test_db'
    }

and create a new commuter instance::

    from pgcom import Commuter
    commuter = Commuter(**conn_params)

Any other connection parameter can be passed as a keyword.
The list of the supported parameters
`can be seen here <https://www.postgresql.org/docs/current/libpq-connect.html#LIBPQ-PARAMKEYWORDS>`_.
