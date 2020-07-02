# pgcom

[![Build Status](https://travis-ci.org/viktorsapozhok/pgcom.svg?branch=master)](https://travis-ci.org/viktorsapozhok/pgcom)
[![pypi](https://img.shields.io/pypi/v/pgcom.svg)](https://pypi.python.org/pypi/pgcom)
[![Documentation Status](https://readthedocs.org/projects/pgcom/badge/?version=latest)](https://pgcom.readthedocs.io/en/latest/?badge=latest)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](https://opensource.org/licenses/MIT)

Communication manager for PostgreSQL database, provides a collection of convenience
wrappers over Psycopg adapter to simplify the usage of basic Psycopg methods in conjunction
with Pandas DataFrames.

## Installation

To install the package, simply use pip:

```
$ pip install pgcom
```

## Key features

* Querying from database to Pandas DataFrame 
* Writing from DataFrame to a database tables.
* Adaptation between DataFrames and COPY FROM.
* Methods to resolve conflicts in DataFrame before applying COPY FROM.
* Tools for setting asynchronous communication with database using LISTEN and NOTIFY commands.

Read the [documentation](https://pgcom.readthedocs.io/en/latest/) for more.

## License

MIT License (see [LICENSE](LICENSE)).
