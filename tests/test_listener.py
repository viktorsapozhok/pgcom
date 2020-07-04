import json

from pgcom import Commuter, Listener
from .conftest import ConnParams

commuter = Commuter(**ConnParams().get())
listener = Listener(**ConnParams().get())


def test_poll():
    delete_table('people', schema='model')
    delete_table('test', schema='model')
    listener.execute('CREATE TABLE model.people (id integer, name text)')
    listener.execute('CREATE TABLE model.test (id integer, name text)')

    listener.create_notify_function(
        func_name='notify_trigger',
        channel='test_channel',
        schema='model')

    listener.create_trigger(
        table_name='model.people',
        func_name='notify_trigger')

    listener.poll(
        channel='test_channel',
        on_notify=on_notify,
        on_timeout=on_timeout,
        on_close=on_close,
        timeout=1)

    df = commuter.select('SELECT * FROM model.test')

    assert df['id'].to_list() == [2, 3]

    delete_table('people', schema='model')
    delete_table('test', schema='model')


def on_notify(payload):
    if len(payload) > 0:
        payload = json.loads(payload)
        _id, _name = int(payload['id']), payload['name']
    else:
        _id, _name = 2, 'Yeltsin'

    commuter.insert_row(
        table_name='test',
        id=_id,
        name=_name,
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
    if commuter.is_table_exist(table_name, schema=schema):
        if schema is None:
            commuter.execute('DROP TABLE ' + table_name)
        else:
            commuter.execute('DROP TABLE ' + schema + '.' + table_name)
