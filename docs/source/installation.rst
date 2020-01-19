Installation
============

Pgcom supports Python 3.6 or newer.

To install the package, you can simply use pip:

.. code-block:: bash

    $ pip install pgcom

You can also install the development version of Pgcom from master branch of Git repository:

.. code-block:: bash

    $ pip install git+https://github.com/viktorsapozhok/pgcom.git

Some of pgcom's methods use SQLAlchemy engine to speed up the performance. Since
installing this library on Windows might be tricky, the basic pgcom's configuration
doesn't have SQLAlchemy among the dependencies. In this case, pgcom's methods use
Psycopg connection object.

To install pgcom with SQLAlchemy support, use following:

.. code-block:: bash

    $ pip install pgcom[sql]
