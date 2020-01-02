# -*- coding: utf-8 -*-

"""Test commuter methods
"""
from datetime import datetime

import numpy as np
import pandas as pd

from pgcom import Commuter

conn_params = {
    'host': 'localhost',
    'port': '5432',
    'user': 'pguser',
    'password': 'password',
    'db_name': 'test_db'
}

commuter = Commuter(**conn_params)


def test_connection():
    with commuter.connector.make_connection() as conn:
        assert conn is not None


def test_connection_keywords():
    _commuter = Commuter(**conn_params, sslmode='require', schema='public')

    with _commuter.connector.make_connection() as conn:
        assert conn is not None


def test_engine():
    with commuter.connector.engine.connect() as conn:
        assert conn.connection.is_valid


def test_engine_keywords():
    _commuter = Commuter(**conn_params, sslmode='require', schema='public')

    with _commuter.connector.engine.connect() as conn:
        assert conn.connection.is_valid


def test_multiple_connection():
    n_conn = commuter.get_connections_count()

    assert n_conn > 0

    for i in range(100):
        with commuter.connector.make_connection() as conn:
            assert conn is not None

    assert commuter.get_connections_count() - n_conn < 10


def test_multiple_engine():
    n_conn = commuter.get_connections_count()

    assert n_conn > 0

    for i in range(10000):
        with commuter.connector.engine.connect() as conn:
            assert conn.connection.is_valid

    assert commuter.get_connections_count() - n_conn < 10


def test_execute():
    delete_table(table_name='test_table')

    assert not commuter.is_table_exist('test_table')

    commuter.execute('create table if not exists test_table(var_1 integer)')

    assert commuter.is_table_exist('test_table')

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


def test_select_one():
    delete_table(table_name='test_table')

    commuter.execute(create_test_table())

    value = commuter.select_one(
        cmd='SELECT MAX(var_2) FROM test_table',
        default=0)

    assert value == 0

    commuter.copy_from('test_table', create_test_data())
    value = commuter.select_one('SELECT MAX(var_2) FROM test_table')

    assert value == 3

    cmd = 'SELECT MAX(var_2) FROM test_table WHERE var_2 > 10'
    value = commuter.select_one(cmd=cmd, default=-1)

    assert value == -1

    delete_table(table_name='test_table')


def test_copy_from():
    delete_table(table_name='test_table')

    commuter.execute(create_test_table())
    commuter.copy_from('test_table', create_test_data())

    df = commuter.select('select * from test_table')
    df['date'] = pd.to_datetime(df['var_1'])

    assert df['date'][0].date() == datetime.now().date()
    assert len(df) == 3

    delete_table(table_name='test_table')


def test_copy_from_schema():
    delete_table(table_name='test_table', schema='model')

    commuter.execute(create_test_table_schema())

    df = create_test_data()
    df['new_var_1'] = 1
    df.insert(loc=0, column='new_var_2', value=[3, 2, 1])

    assert df.shape == (3, 6)

    commuter.copy_from(
        table_name='test_table',
        data=df,
        schema='model',
        format_data=True)

    data = commuter.select('select * from model.test_table')
    data['date'] = pd.to_datetime(data['var_1'])

    assert data['date'][0].date() == datetime.now().date()
    assert len(data) == 3

    delete_table(table_name='test_table', schema='model')


def test_execute_with_params():
    delete_table(table_name='people')
    who = "Yeltsin"
    age = 72

    cmd = f"""
    CREATE TABLE IF NOT EXISTS people(
        name text,
        age integer)
    """

    commuter.execute(cmd=cmd)
    commuter.execute(
        cmd="INSERT INTO people VALUES (%s, %s)",
        vars=(who, age))

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
    delete_table(table_name='test_table')

    data = create_test_data()
    commuter.execute(create_test_table())
    commuter.copy_from(table_name='test_table', data=data)

    df = commuter.resolve_primary_conflicts(
        table_name='test_table',
        data=data,
        p_key=['var_1', 'var_2'],
        filter_col='var_1')

    assert df.empty

    _data = data.copy()
    _data['var_2'] = [-1, 2, -3]

    df = commuter.resolve_primary_conflicts(
        table_name='test_table',
        data=_data,
        p_key=['var_1', 'var_2'],
        filter_col='var_1')

    assert len(df) == 2

    delete_table(table_name='test_table')


def test_resolve_foreign_conflicts():
    delete_table(table_name='test_table')

    data = create_test_data()
    commuter.execute(create_test_table())
    commuter.copy_from('test_table', data)

    _data = data.copy()
    _data['var_2'] = [1, 2, 4]

    df = commuter.resolve_foreign_conflicts(
        parent_table_name='test_table',
        data=_data,
        f_key=['var_2'],
        filter_parent='var_1',
        filter_child='var_1')

    assert len(df) == 2

    delete_table(table_name='test_table')


def test_column_names():
    delete_table(table_name='test_table')

    commuter.execute(create_test_table())
    columns = commuter.get_columns('test_table')

    assert columns['column_name'].to_list() == \
        ['var_1', 'var_2', 'var_3', 'var_4']

    delete_table(table_name='test_table')


def test_schema_parser():
    assert commuter.get_schema(table_name='schema.table') == \
        ('schema', 'table')
    assert commuter.get_schema() == ('public', None)
    assert commuter.get_schema(schema='schema') == ('schema', None)
    assert commuter.get_schema(table_name='my_table') == \
        ('public', 'my_table')


def test_insert_row():
    delete_table(table_name='test_table', schema='model')

    commuter.execute(create_test_table_schema())

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

    cmd = f"""
    INSERT INTO
        model.test_table (var_1, var_2, var_3)
    VALUES (%s, %s, %s)
    """

    row_id = commuter.insert_return(
        cmd=cmd,
        values=(datetime(2019, 12, 9), 8, 'test'),
        return_id='id')

    assert row_id == 2

    delete_table(table_name='test_table', schema='model')


def create_test_table():
    cmd = f"""
    CREATE TABLE IF NOT EXISTS test_table (
        var_1 timestamp,
        var_2 integer NOT NULL,
        var_3 text,
        var_4 real);
    """

    return cmd


def create_test_data():
    df = pd.DataFrame({
        'var_1': pd.date_range(datetime.now(), periods=3),
        'var_2': [1, 2, 3],
        'var_3': ['x', 'xx', 'xxx'],
        'var_4': np.random.rand(3)})

    return df


def create_test_table_schema():
    cmd = f"""
    CREATE TABLE IF NOT EXISTS model.test_table (
        var_1 timestamp,
        var_2 integer NOT NULL,
        var_3 text,
        var_4 real);
    """

    return cmd


def create_test_table_serial():
    cmd = f"""
    CREATE TABLE IF NOT EXISTS model.test_table (
        id SERIAL PRIMARY KEY,
        var_1 timestamp,
        var_2 integer NOT NULL,
        var_3 text,
        var_4 real);
    """

    return cmd


def delete_table(table_name, schema=None):
    if commuter.is_table_exist(table_name, schema=schema):
        if schema is None:
            commuter.execute('drop table ' + table_name)
        else:
            commuter.execute('drop table ' + schema + '.' + table_name)
