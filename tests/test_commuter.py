from datetime import datetime
from unittest.mock import mock_open, patch

import numpy as np
import pandas as pd
import pytest

from pgcom import Commuter, exc
from .conftest import commuter, conn_params, delete_table, with_table


def create_test_table(table_name, schema='public'):
    return f"""
    CREATE TABLE IF NOT EXISTS {schema}.{table_name} (
        var_1 timestamp,
        var_2 integer NOT NULL PRIMARY KEY,
        var_3 text,
        var_4 real,
        var_5 integer);
    """


def create_test_table_serial(table_name, schema='public'):
    return f"""
    CREATE TABLE IF NOT EXISTS {schema}.{table_name} (
        id SERIAL PRIMARY KEY,
        var_1 timestamp,
        var_2 integer NOT NULL,
        var_3 text,
        var_4 real);
    """


def create_child_table(child_name, parent_name, schema='public'):
    return f"""
    CREATE TABLE IF NOT EXISTS {schema}.{child_name} (
        var_1 integer,
        var_2 integer,
        var_3 integer,
        FOREIGN KEY (var_1) REFERENCES {parent_name}(var_2));
    """


def create_test_data():
    return pd.DataFrame({
        'var_1': pd.date_range(datetime.now(), periods=3),
        'var_2': [1, 2, 3],
        'var_3': ['x', 'xx', 'xxx'],
        'var_4': [1.1, 2.2, 3.3],
        'var_5': [1, 2, 3]})


def test_repr():
    assert repr(commuter)[0] == '('
    assert repr(commuter)[-1] == ')'


@with_table('test_table', create_test_table)
def test_execute():
    assert commuter.is_table_exist('test_table')
    with pytest.raises(exc.QueryExecutionError) as e:
        commuter.execute('SELECT 1 FROM fake_table')
    assert e.type == exc.QueryExecutionError


@with_table('test_table', create_test_table)
def test_execute_script():
    assert commuter.is_table_exist('test_table')
    with patch('builtins.open', mock_open(read_data='DROP TABLE test_table')):
        commuter.execute_script('path/to/open')
    assert not commuter.is_table_exist('test_table')


@with_table('test_table', create_test_table)
def test_select_insert():
    commuter.insert('test_table', create_test_data())
    df = commuter.select('SELECT * FROM test_table')
    df['date'] = pd.to_datetime(df['var_1'])
    assert df['date'][0].date() == datetime.now().date()
    assert len(df) == 3


@with_table('test_table', create_test_table)
def test_multiple_select():
    commuter.insert('test_table', create_test_data())
    n_conn = commuter.get_connections_count()
    for i in range(300):
        df = commuter.select('SELECT * FROM test_table')
        assert len(df) == 3
    assert commuter.get_connections_count() - n_conn < 10


def test_insert():
    with pytest.raises(exc.QueryExecutionError) as e:
        commuter.insert('fake_table', create_test_data())
    assert e.type == exc.QueryExecutionError


@with_table('test_table', create_test_table)
def test_select_one():
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


@with_table('test_table', create_test_table)
def test_table_exist():
    assert commuter.is_table_exist('test_table')

    delete_table(table_name='test_table')
    assert not commuter.is_table_exist('test_table')


@with_table('test_table', create_test_table)
def test_copy_from():
    commuter.copy_from('test_table', create_test_data())
    df = commuter.select('SELECT * FROM test_table')
    df['date'] = pd.to_datetime(df['var_1'])
    assert df['date'][0].date() == datetime.now().date()
    assert len(df) == 3

    with pytest.raises(exc.CopyError) as e:
        commuter.copy_from('fake_table', create_test_data())
    assert e.type == exc.CopyError


@with_table('test_table', create_test_table, schema='model')
def test_copy_from_schema():
    df = create_test_data()
    df['var_2'] = [1, 2, 3.01]
    df['new_var_1'] = 1
    df.insert(loc=0, column='new_var_2', value=[3, 2, 1])
    assert df.shape == (3, 7)

    commuter.copy_from('test_table', df, schema='model', format_data=True)
    data = commuter.select('SELECT * FROM model.test_table')
    data['date'] = pd.to_datetime(data['var_1'])
    assert data['date'][0].date() == datetime.now().date()
    assert len(data) == 3

    commuter.copy_from(
        'test_table', df,
        schema='model', format_data=True, where='var_2 in (1,2,3)')
    assert data['date'][0].date() == datetime.now().date()
    assert len(data) == 3


@with_table('test_table', create_test_table_serial, schema='model')
def test_copy_from_incomplete_data():
    df = pd.DataFrame({'var_2': [1, 2, 3], 'var_3': ['x', 'y', 'z']})
    commuter.copy_from('model.test_table', df, format_data=True)
    assert commuter.select_one('SELECT COUNT(*) FROM model.test_table') == 3


@with_table('test_table', create_test_table)
def test_format_data():
    df = create_test_data()
    df['var_5'] = [np.nan, np.nan, 1]
    commuter.copy_from('test_table', df, format_data=True)
    assert commuter.select_one('SELECT COUNT(*) FROM test_table') == 3


@with_table('test_table', create_test_table)
def test_format_text_columns():
    df = create_test_data()
    df['var_3'] = ['abc', 'abc.abc', 'abc,abc']
    commuter.copy_from('test_table', df, format_data=True)
    df = commuter.select('SELECT * FROM test_table')
    assert df['var_3'].to_list() == ['abc', 'abc.abc', 'abcabc']

    commuter.execute('DELETE FROM test_table WHERE 1=1')
    df['var_3'] = [np.nan, np.nan, np.nan]
    commuter.copy_from('test_table', df, format_data=True)
    df = commuter.select('SELECT * FROM test_table')
    assert df['var_3'].to_list() == [None, None, None]


def test_execute_with_params():
    delete_table(table_name='people')
    who = "Yeltsin"
    age = 72
    cmd = "CREATE TABLE IF NOT EXISTS people(name text, age integer)"
    commuter.execute(cmd=cmd)
    commuter.execute(
        cmd="INSERT INTO people VALUES (%s, %s)",
        values=(who, age))
    df = commuter.select('SELECT * FROM people')
    assert df['age'][0] == 72
    assert len(df) == 1
    delete_table(table_name='people')


@with_table('test_table', create_test_table, schema='model')
def test_schema():
    _commuter = Commuter(**conn_params, schema='model')
    _commuter.insert('test_table', create_test_data())
    df = _commuter.select('SELECT * FROM test_table')
    df['date'] = pd.to_datetime(df['var_1'])
    assert df['date'][0].date() == datetime.now().date()
    assert len(df) == 3
    del _commuter


@with_table('test_table', create_test_table, schema='model')
def test_resolve_primary_conflicts():
    data = create_test_data()
    commuter.copy_from('test_table', data, schema='model')
    df = commuter.resolve_primary_conflicts(
        'model.test_table', data, where='var_2 in (1,2,3)')
    assert df.empty

    df = commuter.resolve_primary_conflicts(
        'test_table', data,
        schema='model', where=f"var_1 > '{datetime(2020,1,1)}'")
    assert df.empty

    _data = data.copy()
    _data['var_2'] = [-1, 2, -3]

    df = commuter.resolve_primary_conflicts(
        'test_table', _data,
        schema='model', where=f"var_1 > '{datetime(2020,1,1)}'")
    assert len(df) == 2


@with_table('test_table', create_test_table, schema='model')
@with_table('child_table', create_child_table, 'model.test_table')
def test_resolve_foreign_conflicts():
    parent_data = create_test_data()
    child_data = pd.DataFrame({
        'var_1': [1, 1, 3, 4, 5], 'var_2': [1] * 5, 'var_3': ['x'] * 5})

    commuter.copy_from('test_table', parent_data, schema='model')

    df = commuter.resolve_foreign_conflicts(
        table_name='child_table',
        parent_name='model.test_table',
        data=child_data,
        where='var_2=1')
    assert len(df) == 2


@with_table('test_table', create_test_table, schema='model')
def test_insert_row():
    commuter.insert_row(
        table_name='test_table',
        schema='model',
        var_1=datetime(2019, 12, 9),
        var_2=7,
        var_3='test')
    df = commuter.select('SELECT * FROM model.test_table')
    assert len(df) == 1
    assert df['var_1'][0] == datetime(2019, 12, 9)


@with_table('test_table', create_test_table)
def test_insert_string_with_quotes():
    commuter.insert_row('test_table', var_2=1, var_3="test 'message'")
    msg = commuter.select_one('SELECT var_3 FROM test_table')
    assert msg == "test 'message'"


@with_table('test_table', create_test_table_serial, schema='model')
def test_insert_row_return():
    row_id = commuter.insert_row(
        table_name='test_table',
        schema='model',
        return_id='id',
        var_1=datetime(2019, 12, 9),
        var_2=7,
        var_3='test')
    assert row_id == 1

    df = commuter.select('SELECT * FROM model.test_table')
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

    with pytest.raises(exc.QueryExecutionError) as e:
        _ = commuter.insert_return(
            cmd='INSERT INTO model.test_table VALUES (%s,%s)',
            values=(1, 1),
            return_id='id')
    assert e.type == exc.QueryExecutionError

    sid = commuter.insert_return('DROP TABLE model.test_table')
    assert sid == 0
