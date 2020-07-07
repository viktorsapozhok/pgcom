Installation
============

Pgcom supports Python 3.6 or newer.

To install the package, you can simply use pip:

.. code-block:: bash

    $ pip install pgcom

or clone the repository:

.. code-block:: bash

    $ git clone git@github.com:viktorsapozhok/pgcom.git
    $ cd pgcom
    $ pip install .

To install the package in development mode with the possibility to run tests
in local environment, use following:

.. code-block:: bash

    $ git clone git@github.com:viktorsapozhok/pgcom.git
    $ cd pgcom
    $ pip install --no-cache-dir --editable .[test]
