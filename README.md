# pgcom

[![Python](https://img.shields.io/badge/python-3.6%20%7C%203.7-blue)](https://www.python.org)
[![Build Status](https://travis-ci.org/viktorsapozhok/pgcom.svg?branch=master)](https://travis-ci.org/viktorsapozhok/pgcom)
[![codecov](https://codecov.io/gh/viktorsapozhok/pgcom/branch/master/graph/badge.svg)](https://codecov.io/gh/viktorsapozhok/pgcom)
[![pypi](https://img.shields.io/pypi/v/pgcom.svg)](https://pypi.python.org/pypi/pgcom)
[![Documentation Status](https://readthedocs.org/projects/pgcom/badge/?version=latest)](https://pgcom.readthedocs.io/en/latest/?badge=latest)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](https://opensource.org/licenses/MIT)

Communication manager for PostgreSQL database, provides a collection of wrappers over
Psycopg adapter to simplify the usage of basic SQL operators.

## Installation

To install the package, simply use pip:

```
$ pip install pgcom
```

You can also install the development version of pgcom from master branch of Git repository:

```
$ pip install git+https://github.com/viktorsapozhok/pgcom.git
```

## Key features

* Execution of the database operations, reading query into a DataFrame, 
writing records from DataFrame to the table
* Using `COPY FROM` for efficient adding data to the table
* Methods to resolve primary and foreign key conflicts before adding data to the table
* Tools for setting asynchronous communication with database using 
PostgreSQL `LISTEN` and `NOTIFY` commands        

Read the [documentation](https://pgcom.readthedocs.io/en/latest/) for more.

## License

MIT License (see [LICENSE](LICENSE)).