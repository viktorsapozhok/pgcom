from datetime import datetime
from functools import wraps

import numpy as np
import pandas as pd

from pgcom import Commuter, exc
from .conftest import conn_params

commuter = Commuter(**conn_params)


def test_connection():
    with commuter.connector.open_connection() as conn:
        assert conn is not None


def test_connection_keywords():
    _commuter = Commuter(**conn_params, sslmode='allow', schema='public')
    with _commuter.connector.open_connection() as conn:
        assert conn is not None
    del _commuter


def test_multiple_connection():
    n_conn = commuter.get_connections_count()
    assert n_conn > 0

    for i in range(100):
        with commuter.connector.open_connection() as conn:
            assert conn is not None
    assert commuter.get_connections_count() - n_conn < 10


def test_pool_connection():
    _commuter = Commuter(**conn_params, pool_size=20)
    with _commuter.connector.open_connection() as conn:
        assert conn is not None
    del _commuter


def test_multiple_pool_connection():
    _commuter = Commuter(**conn_params, pool_size=20)
    n_conn = _commuter.get_connections_count()
    assert n_conn > 0

    for i in range(100):
        with _commuter.connector.open_connection() as conn:
            assert conn is not None
    assert commuter.get_connections_count() - n_conn < 10
    del _commuter


def test_repr():
    assert repr(commuter)[0] == '('
    assert repr(commuter)[-1] == ')'


def test_execute():
    delete_table(table_name='test_table')
    assert not commuter.is_table_exist('test_table')

    commuter.execute('create table if not exists test_table(var_1 integer)')
    assert commuter.is_table_exist('test_table')

    try:
        commuter.execute('select 1 from fake_table')
        assert False
    except exc.QueryExecutionError:
        assert True

    delete_table(table_name='test_table')


def test_execute_script():
    delete_table(table_name='test_table')

    commuter.execute(create_test_table())

    assert commuter.is_table_exist('test_table')

    delete_table(table_name='test_table')


def test_select_insert():
    delete_table(table_name='test_table')
    commuter.execute(create_test_table())
    commuter.insert('test_table', create_test_data())
    df = commuter.select('select * from test_table')
    df['date'] = pd.to_datetime(df['var_1'])

    assert df['date'][0].date() == datetime.now().date()
    assert len(df) == 3

    delete_table(table_name='test_table')


def test_multiple_select():
    delete_table(table_name='test_table')
    commuter.execute(create_test_table())
    commuter.insert('test_table', create_test_data())

    n_conn = commuter.get_connections_count()

    for i in range(300):
        df = commuter.select('select * from test_table')
        assert len(df) == 3

    assert commuter.get_connections_count() - n_conn < 10

    delete_table(table_name='test_table')


def test_multiple_pool_select():
    _commuter = Commuter(**conn_params, pool_size=20)
    delete_table(table_name='test_table')
    _commuter.execute(create_test_table())
    _commuter.insert('test_table', create_test_data())

    n_conn = _commuter.get_connections_count()

    for i in range(300):
        df = _commuter.select('select * from test_table')
        assert len(df) == 3

    assert _commuter.get_connections_count() - n_conn < 10

    delete_table(table_name='test_table')
    del _commuter


def test_insert():
    try:
        commuter.insert('fake_table', create_test_data())
        assert False
    except exc.QueryExecutionError:
        assert True


def test_select_one():
    delete_table(table_name='test_table')

    commuter.execute(create_test_table())

    cmd = 'SELECT MAX(var_2) FROM test_table'
    value = commuter.select_one(cmd=cmd, default=0)
    assert value == 0

    commuter.copy_from('test_table', create_test_data())
    value = commuter.select_one('SELECT MAX(var_2) FROM test_table')
    assert value == 3

    cmd = 'SELECT MAX(var_2) FROM test_table WHERE var_2 > 10'
    value = commuter.select_one(cmd=cmd, default=-1)
    assert value == -1

    value = commuter.select_one('DROP TABLE test_table', default=1)
    assert value == 1

    delete_table(table_name='test_table')


def test_table_exist():
    delete_table(table_name='test_table')

    assert not commuter.is_table_exist('test_table')

    commuter.execute(create_test_table())

    assert commuter.is_table_exist('test_table')

    delete_table(table_name='test_table')


def test_copy_from():
    delete_table(table_name='test_table')

    commuter.execute(create_test_table())

    commuter.copy_from('test_table', create_test_data())
    df = commuter.select('select * from test_table')
    df['date'] = pd.to_datetime(df['var_1'])
    assert df['date'][0].date() == datetime.now().date()
    assert len(df) == 3

    try:
        commuter.copy_from('fake_table', create_test_data())
        assert False
    except exc.CopyError:
        assert True

    delete_table(table_name='test_table')


def test_copy_from_schema():
    delete_table(table_name='test_table', schema='model')

    commuter.execute(create_test_table(schema='model'))

    df = create_test_data()
    df['var_2'] = [1, 2, 3.01]
    df['new_var_1'] = 1
    df.insert(loc=0, column='new_var_2', value=[3, 2, 1])

    assert df.shape == (3, 7)

    commuter.copy_from(
        table_name='test_table',
        data=df,
        schema='model',
        format_data=True)

    data = commuter.select('select * from model.test_table')
    data['date'] = pd.to_datetime(data['var_1'])

    assert data['date'][0].date() == datetime.now().date()
    assert len(data) == 3

    commuter.copy_from(
        table_name='test_table',
        data=df,
        schema='model',
        format_data=True,
        where='var_2 in (1,2,3)')

    assert data['date'][0].date() == datetime.now().date()
    assert len(data) == 3

    delete_table(table_name='test_table', schema='model')


def test_copy_from_incomplete_data():
    delete_table(table_name='test_table', schema='model')
    commuter.execute(create_test_table_serial())
    df = pd.DataFrame({'var_2': [1, 2, 3], 'var_3': ['x', 'y', 'z']})
    commuter.copy_from(
        table_name='model.test_table',
        data=df,
        format_data=True)

    assert commuter.select_one(
        'select count(*) from model.test_table') == 3

    delete_table(table_name='test_table', schema='model')


def test_format_data():
    delete_table(table_name='test_table')
    commuter.execute(create_test_table())
    df = create_test_data()
    df['var_5'] = [np.nan, np.nan, 1]

    commuter.copy_from(
        table_name='test_table',
        data=df,
        format_data=True)

    assert commuter.select_one(
        'select count(*) from test_table') == 3

    delete_table(table_name='test_table')


def test_format_text_columns():
    delete_table(table_name='test_table')
    commuter.execute(create_test_table())
    df = create_test_data()
    df['var_3'] = ['abc', 'abc.abc', 'abc,abc']

    commuter.copy_from(
        table_name='test_table',
        data=df,
        format_data=True)
    df = commuter.select('select * from test_table')
    assert df['var_3'].to_list() == ['abc', 'abc.abc', 'abcabc']

    commuter.execute('delete from test_table where 1=1')

    df['var_3'] = [np.nan, np.nan, np.nan]
    commuter.copy_from(
        table_name='test_table',
        data=df,
        format_data=True)
    df = commuter.select('select * from test_table')
    assert df['var_3'].to_list() == [None, None, None]

    delete_table(table_name='test_table')


def test_execute_with_params():
    delete_table(table_name='people')
    who = "Yeltsin"
    age = 72

    cmd = """
    CREATE TABLE IF NOT EXISTS people(
        name text,
        age integer)
    """

    commuter.execute(cmd=cmd)
    commuter.execute(
        cmd="INSERT INTO people VALUES (%s, %s)",
        values=(who, age))

    df = commuter.select('SELECT * FROM people')

    assert df['age'][0] == 72
    assert len(df) == 1

    delete_table(table_name='people')


def test_schema():
    _commuter = Commuter(**conn_params, schema='model')

    delete_table(table_name='test_table', schema='model')

    _commuter.execute(create_test_table())
    _commuter.insert('test_table', create_test_data())

    df = _commuter.select('select * from test_table')
    df['date'] = pd.to_datetime(df['var_1'])

    assert df['date'][0].date() == datetime.now().date()
    assert len(df) == 3

    delete_table(table_name='test_table', schema='model')


def test_resolve_primary_conflicts():
    delete_table(table_name='test_table', schema='model')

    data = create_test_data()
    commuter.execute(create_test_table(schema='model'))
    commuter.copy_from(table_name='test_table', data=data, schema='model')

    df = commuter.resolve_primary_conflicts(
        table_name='model.test_table',
        data=data,
        where='var_2 in (1,2,3)')

    assert df.empty

    df = commuter.resolve_primary_conflicts(
        table_name='test_table',
        data=data,
        schema='model',
        where=f"var_1 > '{datetime(2020,1,1)}'")

    assert df.empty

    _data = data.copy()
    _data['var_2'] = [-1, 2, -3]

    df = commuter.resolve_primary_conflicts(
        table_name='test_table',
        data=_data,
        schema='model',
        where=f"var_1 > '{datetime(2020,1,1)}'")

    assert len(df) == 2

    delete_table(table_name='test_table', schema='model')


def test_resolve_foreign_conflicts():
    delete_table(table_name='test_table', schema='model')
    delete_table(table_name='child_table')

    parent_data = create_test_data()
    child_data = pd.DataFrame({
        'var_1': [1, 1, 3, 4, 5],
        'var_2': [1] * 5,
        'var_3': ['x'] * 5})

    commuter.execute(create_test_table(schema='model'))
    commuter.execute(create_child_table(
        child_name='child_table',
        parent_name='model.test_table'))
    commuter.copy_from('test_table', parent_data, schema='model')

    df = commuter.resolve_foreign_conflicts(
        table_name='child_table',
        parent_name='model.test_table',
        data=child_data,
        where='var_2=1')

    assert len(df) == 2

    delete_table(table_name='test_table', schema='model')
    delete_table(table_name='child_table')


def test_insert_row():
    delete_table(table_name='test_table', schema='model')

    commuter.execute(create_test_table(schema='model'))

    commuter.insert_row(
        table_name='test_table',
        schema='model',
        var_1=datetime(2019, 12, 9),
        var_2=7,
        var_3='test')

    df = commuter.select('select * from model.test_table')

    assert len(df) == 1
    assert df['var_1'][0] == datetime(2019, 12, 9)

    delete_table(table_name='test_table', schema='model')


def test_insert_string_with_quotes():
    commuter.execute(create_test_table())
    commuter.insert_row(
        table_name='test_table', var_2=1, var_3="test 'message'")
    delete_table(table_name='test_table')


def test_insert_row_return():
    delete_table(table_name='test_table', schema='model')

    commuter.execute(create_test_table_serial())

    row_id = commuter.insert_row(
        table_name='test_table',
        schema='model',
        return_id='id',
        var_1=datetime(2019, 12, 9),
        var_2=7,
        var_3='test')

    assert row_id == 1

    df = commuter.select('select * from model.test_table')

    assert len(df) == 1
    assert df['var_1'][0] == datetime(2019, 12, 9)

    cmd = """
    INSERT INTO model.test_table (var_1, var_2, var_3)
        VALUES (%s, %s, %s)
    """

    row_id = commuter.insert_return(
        cmd=cmd,
        values=(datetime(2019, 12, 9), 8, 'test'),
        return_id='id')
    assert row_id == 2

    try:
        _ = commuter.insert_return(
            cmd='insert into model.test_table VALUES (%s,%s)',
            values=(1, 1),
            return_id='id')
        assert False
    except exc.QueryExecutionError:
        assert True

    sid = commuter.insert_return('DROP TABLE model.test_table')
    assert sid == 0

    delete_table(table_name='test_table', schema='model')


def create_test_table(schema=None):
    if schema is not None:
        table_name = schema + '.test_table'
    else:
        table_name = 'test_table'

    return f"""
    CREATE TABLE IF NOT EXISTS {table_name} (
        var_1 timestamp,
        var_2 integer NOT NULL PRIMARY KEY,
        var_3 text,
        var_4 real,
        var_5 integer);
    """


def create_test_data():
    return pd.DataFrame({
        'var_1': pd.date_range(datetime.now(), periods=3),
        'var_2': [1, 2, 3],
        'var_3': ['x', 'xx', 'xxx'],
        'var_4': [1.1, 2.2, 3.3],
        'var_5': [1, 2, 3]})


def create_test_table_serial():
    return """
    CREATE TABLE IF NOT EXISTS model.test_table (
        id SERIAL PRIMARY KEY,
        var_1 timestamp,
        var_2 integer NOT NULL,
        var_3 text,
        var_4 real);
    """


def create_child_table(child_name, parent_name):
    return f"""
    CREATE TABLE IF NOT EXISTS {child_name} (
        var_1 integer,
        var_2 integer,
        var_3 integer,
        FOREIGN KEY (var_1) REFERENCES {parent_name}(var_2));
    """


def delete_table(table_name, schema=None):
    if commuter.is_table_exist(table_name, schema=schema):
        if schema is None:
            cmd = 'drop table ' + table_name + ' CASCADE'
        else:
            cmd = 'drop table ' + schema + '.' + table_name + ' CASCADE'
        commuter.execute(cmd)


def _delete_table(table_name, schema=None):
    def decorator(func):
        @wraps(func)
        def wrapped(*args, **kwargs):
            if commuter.is_table_exist(table_name, schema=schema):
                if schema is None:
                    cmd = 'drop table ' + table_name + ' CASCADE'
                else:
                    cmd = 'drop table ' + schema + '.' + table_name + ' CASCADE'
                commuter.execute(cmd)

            try:
                func(*args, **kwargs)
            except Exception:
                pass
        return wrapped
    return decorator
