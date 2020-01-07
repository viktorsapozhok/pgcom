# -*- coding: utf-8 -*-
import json

from pgcom import Commuter, Listener

conn_params = {
    'host': 'localhost',
    'port': '5432',
    'user': 'pguser',
    'password': 'password',
    'db_name': 'test_db'
}

commuter = Commuter(**conn_params)
listener = Listener(**conn_params)


def test_poll():
    delete_table('people', schema='model')
    delete_table('test', schema='model')
    listener.execute('create table model.people (id integer, name text)')
    listener.execute('create table model.test (id integer, name text)')

    listener.create_notify_function(
        func_name='notify_trigger',
        channel='test_channel',
        schema='model')

    listener.create_trigger(
        func_name='notify_trigger',
        table_name='people',
        schema='model')

    listener.poll(
        channel='test_channel',
        on_notify=on_notify,
        on_timeout=on_timeout,
        on_close=on_close,
        timeout=1)

    df = commuter.select('select * from model.test')

    assert df['id'].to_list() == [2, 3]

    delete_table('people', schema='model')
    delete_table('test', schema='model')


def on_notify(payload):
    if len(payload) > 0:
        payload = json.loads(payload)
        id, name = int(payload['id']), payload['name']
    else:
        id, name = 2, 'Yeltsin'

    commuter.insert_row(
        table_name='test',
        id=id,
        name=name,
        schema='model')

    raise KeyboardInterrupt


def on_timeout():
    commuter.insert_row(
        table_name='people',
        id=1,
        name='Yeltsin',
        schema='model')


def on_close():
    commuter.insert_row(
        table_name='test',
        id=3,
        name='Yeltsin',
        schema='model')


def delete_table(table_name, schema=None):
    if listener.is_table_exist(table_name, schema=schema):
        if schema is None:
            listener.execute('drop table ' + table_name)
        else:
            listener.execute('drop table ' + schema + '.' + table_name)
