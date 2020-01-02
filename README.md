# pgcom

[![Python](https://img.shields.io/badge/python-3.7-blue)](https://www.python.org)
[![Build Status](https://travis-ci.org/viktorsapozhok/pgcom.svg?branch=master)](https://travis-ci.org/viktorsapozhok/pgcom)
[![codecov](https://codecov.io/gh/viktorsapozhok/pgcom/branch/master/graph/badge.svg)](https://codecov.io/gh/viktorsapozhok/pgcom)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](https://opensource.org/licenses/MIT)

Communication manager for PostgreSQL database, provides a collection of wrappers over
`psycopg2` adapter to simplify the usage of basic SQL operators. 

## Installation

To install the package, simply use pip.

```
$ pip install pgcom
```

#### Basic usage

To initialize a new commuter, you need to set the basic connection parameters: 
`host`, `port`, `user`, `password`, and `db_name`. 
Any other connection parameter can be passed as a keyword. 
The list of the supported parameters [can be seen here](https://www.postgresql.org/docs/current/libpq-connect.html#LIBPQ-PARAMKEYWORDS).

```python
from pgcom import Commuter

conn_params = {
    'host': 'localhost',
    'port': '5432',
    'user': 'postgres',
    'password': 'password',
    'db_name': 'test_db'
}

commuter = Commuter(**conn_params)
```

Basic operations are provided with `select`, `insert` and `execute` methods.

```python
df = commuter.select('select * from people where age > %s and salary > %s' % (55, 1000))
commuter.insert(table_name='people', data=df)
commuter.execute(cmd='insert into people values (%s, %s)', vars=('Yeltsin', 72)) 
```   

#### Schema 

You can specify schema when creating a new `Commuter` instance.
 
```python
from pgcom import Commuter
commuter = Commuter(**conn_params, schema='model')
```

Alternatively, you can pass `schema` to the `Commuter` method.

```python
commuter.insert(table_name='people', data=df, schema='model')
```

Or directly in command string.

```python
df = commuter.select('select * from model.people')
```

#### Execute multiple SQL statement

```python
commuter.execute_script(path2script)
```

#### Insert row and return serial key 

Use `insert_return` method to insert a row and return the serial key of the newly inserted row.

```python
cmd = 'INSERT INTO people (name, age) VALUES (%s, %s)'
values = ('Yeltsin', '72')
pid = commuter.insert_return(cmd, values=values, return_id='person_id')
```

In the example above the table `people` should contain a serial key `person_id`. 

#### Insert row

Alternatively, you can use `insert_row` method to insert one new row.

```python
from datetime import datetime

commuter.insert_row(
    table_name='people', 
    name='Yeltsin', 
    age='72',
    birth_date=datetime(1931, 2, 1))
```

It also supports the returning of the serial key. 

```python
pid = commuter.insert_row(
    table_name='people', 
    return_id='person_id', 
    name='Yeltsin', 
    age='72')
```

#### Insert data using copy_from

In contrast to `insert` method, the `copy_from` method efficiently copies data 
from DataFrame employing PostgreSQL `copy_from` command. 

```python
commuter.copy_from(table_name='people', data=data)
```

As compared to `insert`, this method works much more effective on the large dataframes.
You can also set `format_data` parameter as `True` to allow automatically format your 
DataFrame before calling `copy_from` command. It adjusts columns order before copying
and converts types.

```python
commuter.copy_from(table_name='people', data=df, format_data=True)
```

#### Verify if table exists

```python
is_exist = commuter.is_table_exist(table_name='people', schema='my_schema')
```

#### Get names of the table columns

Return DataFrame with the column names and types.

```python
columns = commuter.get_columns(table_name='people', schema='my_schema')
```

#### Number of connections

Return the amount of active connections to the database.

```python
n_connections = commuter.get_connections_count()
```

#### Resolve primary conflicts

This method can be used when you want to apply `copy_from` and the DataFrame contains 
rows conflicting with the primary key (duplicates). To remove conflicted rows 
from the DataFrame you can use `resolve_primary_conflicts`.

```python
df = commuter.resolve_primary_conflicts(
    table_name='payments',
    data=df,
    p_key=['payment_date', 'payment_type'],
    filter_col='payment_date',
    schema='my_schema')
```

It selects data from the `table_name` where value in `filter_col` is greater or equal 
the minimal found value in `filter_col` of the given DataFrame. Rows having primary 
key which is already presented in selected data are deleted from the DataFrame.

You need to specify parameter `p_key` with the list of column names representing the primary key.

#### Resolve foreign conflicts

This method selects data from `parent_table_name` where value in `filter_parent` column
is greater or equal the minimal found value in `filter_child` column of the given DataFrame.
Rows having foreign key which is already presented in selected data are deleted from DataFrame.

```python
df = commuter.resolve_foreign_conflicts(
    parent_table_name='people',
    data=df,
    f_key='person_id',
    filter_parent='person_id',
    filter_child='person_id',
    schema='my_schema')
```

Parameter `f_key` should be specified with the list of column names represented the foreign key. 

## License

Package is released under [MIT License](https://github.com/viktorsapozhok/db-commuter/blob/master/LICENSE).
