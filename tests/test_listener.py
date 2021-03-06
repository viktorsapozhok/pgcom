import json

from pgcom import Listener
from .conftest import conn_params, commuter, with_table

listener = Listener(**conn_params)


def create_table(table_name, schema='public'):
    return f"CREATE TABLE {schema}.{table_name} (id integer, name text)"


@with_table('people', create_table, schema='model')
@with_table('test', create_table, schema='model')
def test_poll():
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

    df = commuter.select("SELECT * FROM model.test")

    assert df['id'].to_list() == [2, 3]


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
