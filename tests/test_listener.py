import json

from pgcom import Listener
from .conftest import conn_params, commuter, with_table

listener = Listener(**conn_params)


def create_table(table_name):
    return f"CREATE TABLE {table_name} (id integer, name text)"


@with_table("model.people", create_table)
@with_table("model.test", create_table)
def test_poll():
    listener.create_notify_function(
        func_name="model.notify_trigger", channel="test_channel"
    )

    listener.create_trigger(table_name="model.people", func_name="notify_trigger")

    listener.poll(
        channel="test_channel",
        on_notify=on_notify,
        on_timeout=on_timeout,
        on_close=on_close,
        timeout=1,
    )

    df = commuter.select("SELECT * FROM model.test")

    assert df["id"].to_list() == [2, 3]


def on_notify(payload):
    if len(payload) > 0:
        payload = json.loads(payload)
        _id, _name = int(payload["id"]), payload["name"]
    else:
        _id, _name = 2, "Yeltsin"

    commuter.insert_row(table_name="model.test", id=_id, name=_name)

    raise KeyboardInterrupt


def on_timeout():
    commuter.insert_row(table_name="model.people", id=1, name="Yeltsin")


def on_close():
    commuter.insert_row(table_name="model.test", id=3, name="Yeltsin")
