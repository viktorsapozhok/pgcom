# -*- coding: utf-8 -*-
# from multiprocessing import Process
# import time

from pgcom import Listener

conn_params = {
    'host': 'localhost',
    'port': '5432',
    'user': 'pguser',
    'password': 'password',
    'db_name': 'test_db'
}

listener = Listener(**conn_params)


def test_poll():
    delete_table('people', schema='model')
    delete_table('test', schema='model')
    listener.execute('create table model.people (id integer, name text)')
    listener.execute('create table model.test (id integer, name text)')

    listener.create_notification(
        notification_name='notify_trigger',
        channel='test_channel',
        schema='model')

    listener.create_insert_trigger(
        notification_name='notify_trigger',
        table_name='people',
        schema='model')

#    p = Process(target=poll)
#    p.start()

    listener.insert_row(
        table_name='people',
        id=7,
        name='Yeltsin',
        schema='model')

#    time.sleep(3)
#    p.join(timeout=3)

#    _id = listener.select_one('select id from model.test')
#    _name = listener.select_one('select name from model.test')

#    assert _id == 7
#    assert _name == 'Yeltsin'

    delete_table('people', schema='model')
    delete_table('test', schema='model')


def poll():
    def on_notify(payload):
        _listener.insert_row(
            table_name='test',
            id=int(payload['id']),
            name=payload['name'],
            schema='model')

    _listener = Listener(**conn_params)
    _listener.poll(channel='test_channel', on_notify=on_notify)


def delete_table(table_name, schema=None):
    if listener.is_table_exist(table_name, schema=schema):
        if schema is None:
            listener.execute('drop table ' + table_name)
        else:
            listener.execute('drop table ' + schema + '.' + table_name)
