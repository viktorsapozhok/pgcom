# pgcom

[![Python](https://img.shields.io/badge/python-3.6%20%7C%203.7-blue)](https://www.python.org)
[![Build Status](https://travis-ci.org/viktorsapozhok/pgcom.svg?branch=master)](https://travis-ci.org/viktorsapozhok/pgcom)
[![codecov](https://codecov.io/gh/viktorsapozhok/pgcom/branch/master/graph/badge.svg)](https://codecov.io/gh/viktorsapozhok/pgcom)
[![pypi](https://img.shields.io/pypi/v/pgcom.svg)](https://pypi.python.org/pypi/pgcom)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](https://opensource.org/licenses/MIT)

Communication manager for PostgreSQL database, provides a collection of wrappers over
`psycopg` adapter to simplify the usage of basic SQL operators.   

## Installation

To install the package, simply use pip.

```
$ pip install pgcom
```

## Basic usage

To initialize a new commuter, you need to set the basic connection parameters:
 
- **host** - database host address 
- **port** - connection port number
- **user** - user name used to authenticate
- **password** - password used to authenticate
- **db_name** - the database name
 
Any other connection parameter can be passed as a keyword. 
The list of the supported parameters 
[can be seen here](https://www.postgresql.org/docs/current/libpq-connect.html#LIBPQ-PARAMKEYWORDS).

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

Basic operations are provided with `execute` and `select`, `insert` methods.

Execute a database operation (query or command):

```python
commuter.execute(f"""
    CREATE TABLE IF NOT EXISTS people (
        name text, 
        age integer)
    """)
commuter.execute(
    cmd='INSERT INTO people VALUES (%s, %s)', 
    vars=('Yeltsin', 76))
commuter.execute('DROP TABLE people')
```

Use `select` for reading SQL query into a DataFrame. This method returns
a DataFrame corresponding to the result set of the query string.

```python
age = 55
df = commuter.select(f'SELECT * FROM people WHERE age > {age}')
```   

To write records stored in a DataFrame to database, you can use `insert` method.

```python
import pandas as pd

df = pd.DataFrame({
    'name': ['Gorbachev', 'Yeltsin'], 
    'age': [89, 76]
}) 

commuter.insert(table_name='people', data=df)
```   

## Schema 

To specify schema, you have two different options. You can either specify the 
`schema` attribute in the constructor, or just pass it directly to the method.
 
When you create a new `Commuter` instance with specified schema, all the methods
will use this schema if other is not specified by method parameter. 
By default the public schema is used.
 
```python
print(Commuter(**conn_params))
```
> (host=localhost, user=postgres, db_name=test_db, schema=public)

```python
print(Commuter(schema='model', **conn_params))
```
>(host=localhost, user=postgres, db_name=test_db, schema=model)

If you omit setting schema using class constructor and prefer rather pass it
to the methods, you can use any of the following options.

```python
commuter = Commuter(**conn_params)  # public schema is used by default

# specify schema in SQL string, if method doesn't have schema argument
df = commuter.select('SELECT * FROM model.people WHERE age > 55')

# if method contains `schema` and `table_name` argument
commuter.insert(table_name='model.people', data=df)
# or 
commuter.insert(table_name='people', data=df, schema='model')
```

### Select one element

Use the `select_one` method when your query results in a single element. 
This method returns a scalar value, not a DataFrame. Specify the `default`
argument if you need the default value to be returned in case the query result
is empty, otherwise `None` will be returned. 

```python
n_obs = commuter.select_one(
    cmd='SELECT COUNT(*) FROM people WHERE age > 55',
    default=0)
```

### Insert one row and return serial key

When using a `SERIAL` column to provide unique identifiers, you may need to 
return the ID assigned to a new row. To obtain this, `insert_return` or 
`insert_row` method can be used.

If you use `insert_row` then you need to pass values as kwargs.

```python
commuter.execute(f"""
    CREATE TABLE people (
        num SERIAL PRIMARY KEY, 
        name text, 
        age integer)
    """)

num = commuter.insert_row(
    table_name='people', 
    name='Yeltsin', 
    age=76, 
    return_id='num')

print(f'returned value: {num}')
```
> returned value: 1

Using `insert_return`, you need to specify SQL string.

```python
num = commuter.insert_return(
    cmd='INSERT INTO people (name, age) VALUES (%s, %s)', 
    vars=('Yeltsin', 76),
    return_id='num')

print(f'returned value: {num}')
```
> returned value: 2

### Insert data using COPY FROM command

PostgreSQL `COPY FROM` command copies data from a file-system file to a table 
(appending the data to whatever is in the table already).

Currently no adaptation is provided between Python and PostgreSQL types on COPY: 
the file can be any Python file-like object but its format must be in the format 
accepted by PostgreSQL COPY command (data format, escaped characters, etc).

The `copy_from` method adapts an interface to efficient PostgreSQL `COPY FROM` command
provided by Psycopg `cursor` objects to support writing data stored in a DataFrame.

To see a difference, let's try to insert data from the DataFrame with 1M rows 
and two columns using just a basic `insert` method. 

```python
from time import time
import pandas as pd

df = pd.DataFrame({
    'name': ['Yeltsin'] * int(1e6), 
    'age': [76] * int(1e6)
})
 
start = time()
commuter.insert(table_name='people', data=df)
print(f'processing time: {time() - start:.1f} sec')
```
> processing time: 47.6 sec

Now implementing the same operation with `copy_from`.

```python
start = time()
commuter.copy_from(table_name='people', data=df)
print(f'processing time: {time() - start:.1f} sec')
```
> processing time: 1.3 sec

##### format_data

Set the `format_data` argument as `True`, if you need to adjust data before applying
`copy_from`. It will control columns order according the table information
stored in database information schema and converts float types to integer if needed.

```python
df = pd.DataFrame({'age': [76.0], 'name': ['Yeltsin']})
commuter.copy_from('people', df)
```
> psycopg2.errors.InvalidTextRepresentation: invalid input syntax for type integer: "Yeltsin"

Without formatting we caught an error trying to insert a text data into the first table
column, which has an integer type. Now set `format_data` as `True` and repeat the operation. 

```python
commuter.copy_from('people', df, format_data=True)
n_obs = commuter.select_one('SELECT COUNT(*) FROM people')
print(f'number of added rows: {n_obs}')
```
> number of added rows: 1

##### where

When table has a constraint and the DataFrame contains rows conflicted 
with this constraint, the data cannot be added to the table with the `copy_from` and
`ValueError` will be raised. It is still possible to insert the data with the `execute` method, 
using for example `INSERT ON CONFLICT` statement 
([see here for details](https://www.postgresqltutorial.com/postgresql-upsert/)).

Let's create a table with the primary key and insert one row.

```python
commuter.execute(f"""
CREATE TABLE people (
    name text PRIMARY KEY, 
    age integer)
    """)

commuter.insert_row('people', name='Yeltsin', age=76)
```

Now, if we try to insert the same row we catch an error. 

```python
commuter.copy_from('people', df)
```
> ValueError: duplicate key value violates unique constraint "people_pkey"

> DETAIL:  Key (name)=(Yeltsin) already exists.

Using `where` argument, we can specify the `WHERE` clause of the `DELETE` statement,
which will be executed before calling `COPY FROM`. This means that all rows where
age is equal to 76 will be deleted from the table and then `COPY FROM` command
will be called.

```python
commuter.copy_from('people', df, where='age=76')
n_obs = commuter.select_one('SELECT COUNT(*) FROM people')
print(f'number of added rows: {n_obs}')
```
> number of added rows: 1

### Resolve primary conflicts

In the last example, we deleted rows from the table before using `copy_from`. 
In contrast to it, the `resolve_primary_conflicts` method can be used to control 
the data integrity and, instead of removing rows from the table, remove it from the DataFrame.

To implement it, the method selects data from the table and removes all
rows from the given DataFrame, which violate primary key constraint 
in the selected data. To reduce the amount of querying data (when table is large),
you need to specify the `where` argument. It specifies the `WHERE` clause in 
the `SELECT` query.

```python
commuter.execute(f"""
CREATE TABLE people (
    id integer PRIMARY KEY, 
    name text, 
    age integer)
    """)

df = pd.DataFrame({
    'id': [1,2,3,4,5], 
    'name': ['Brezhnev', 'Andropov', 'Chernenko', 'Gorbachev', 'Yeltsin'],
    'age': [75, 69, 73, 89, 76]})

commuter.copy_from('people', df)
print(df)
```
id | name | age
--- | --- | ---
1 | Brezhnev | 75
2 | Andropov | 69
3 | Chernenko | 73
4 | Gorbachev | 89
5 | Yeltsin | 76

Assume, that we have the new data we want to add to the table.

```python
df = pd.DataFrame({
    'id': [6,3], 
    'name': ['Khrushchev', 'Putin'],
    'age': [77, 67]})
print(df)
```
id | name | age
--- | --- | ---
6 | Khrushchev | 77
3 | Putin | 67

We apply `resolve_primary_conflicts` to sanitize the data before copying and specify
`where` to compare the new entries only across the people older than 60 
(to reduce the complexity).

```python
df = commuter.resolve_primary_conflicts(
    table_name='people',
    data=df,
    where='age > 60')
print(df)
```
id | name | age
--- | --- | ---
6 | Khrushchev | 77

Rows with conflicted keys have been deleted and `copy_from` can be now used without a doubt.

### Resolve foreign conflicts

To sanitize the DataFrame for the case of potential conflicts on the foreign key,
use `resolve_foreign_conflicts`. It selects data from the `parent_table` and 
removes all rows from the given DataFrame, which violate foreign key
constraint in the selected data.

```python
df = commuter.resolve_foreign_conflicts(
    table_name='table_name',
    parent_name='parent_table_name',
    data=df,
    where='condition to reduce the selected data')
```

## License

Package is released under [MIT License](https://github.com/viktorsapozhok/pgcom/blob/master/LICENSE).
